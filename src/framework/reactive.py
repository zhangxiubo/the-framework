"""
Reactive builder: dependency-driven, streaming composition over the event runtime.

This module implements ReactiveBuilder, a processor that:
- Subscribes to one or more prerequisite topics ("requires")
- Publishes results to a provided topic ("provides")
- Treats resolve(provides) as a pure kickoff to upstream producers (no subscription or caching)
- Always listens for prerequisite built() events; for every complete set of input values
  (Cartesian product), it invokes build(context, target, *args) and streams each produced
  element to the provides topic.

Protocol (mirrors builder.BuilderEvent):
- Incoming control:
  - ReactiveEvent(name="resolve", target=provides, sender=client)  # kick-off only; no subscription
- Fan-out to prerequisites (one-time bootstrap on first resolve):
  - ReactiveEvent(name="resolve", target=require_i, sender=self)
- Incoming data from prerequisites (addressed by topic; no per-processor 'to' required):
  - ReactiveEvent(name="built", target=require_i, artifact=value)
- Outgoing streamed results (broadcast to the provides topic):
  - ReactiveEvent(name="built", target=provides, sender=self, artifact=item)

Behavior with multiple inputs (per run, in-memory):
- Maintains a per-topic cache of distinct values (by content digest).
- On each new value for topic T, if all topics have at least one value:
  - Compute the Cartesian product where T contributes only the new value and other topics
    contribute all cached values; invoke build for each unseen combination.
- Maintains a per-combination "seen" set (digest of ordered tuple of value digests per target)
  to avoid recomputing previously processed combinations during the run.

Persistent caching (optional):
- When constructed with cache=True, persist and deterministically replay emitted built() events
  keyed by the computation unit: [class source, provides pattern, actual target, ordered input digests].
- On cache hit for a (target, input tuple), replay stored built() events without invoking build().
- Resolve events are never cached; they only trigger upstream resolution.

Notes:
- Requires are deduplicated while preserving the first occurrence (like AbstractBuilder).
- Generators are iterated; general iterables are enumerated (strings/bytes are treated as singletons).
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import re
import types
import inspect
from collections import OrderedDict
from typing import Any, Collection, Dict, Iterable, List, Sequence, Set

import dill
from pydantic import BaseModel, ConfigDict

from .core import AbstractProcessor, Context

logger = logging.getLogger(__name__)


class ReactiveEvent(BaseModel):
    """Immutable event used by the reactive system; shape mirrors BuilderEvent.

    Fields:
        name: Event type, e.g., "resolve", "built".
    Extra:
        - extra="allow" to carry additional dynamic fields:
            - resolve: target, sender
            - built: target, artifact, sender
        Note: a 'to' field may appear in other subsystems; ReactiveBuilder uses broadcast semantics.
    """

    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


class ReactiveBuilder(AbstractProcessor):
    """Streaming, dependency-aware builder.

    A ReactiveBuilder:
    - Provides a single target topic (provides)
    - Requires zero or more prerequisite topics (requires)
    - On first resolve(provides), fans out resolve(require_i) once (bootstrap) and then always listens
    - As prerequisite values arrive (built(...)), it computes Cartesian
      products across cached values and invokes build(context, provides, *args)
    - Emits each produced element as built(provides, artifact=item) broadcast to the provides topic

    Runtime (in-memory) caching:
    - Per-topic value cache (digest -> value) de-duplicates identical values by content
    - Combination cache prevents re-invocation of build for already-seen value tuples
      during the lifetime of the processor (no persistence across runs)

    Persistent caching (optional):
    - When cache=True, persist and replay built() outputs for a specific (target, input tuple)
      using the processor class source + provides pattern + target + ordered input digests as key.
      Archive storage is provided by AbstractProcessor.archive(workspace).

    Concurrency:
    - Single-threaded per instance (enforced by the Pipeline/Inbox)
    """

    def __init__(self, provides: str, requires: Collection[str], cache: bool = False):
        super().__init__()
        # Public configuration
        self.provides: str = provides
        # Deduplicate requires while preserving order (first occurrence wins)
        self.requires: List[str] = list(dict.fromkeys(requires))

        # Always-listening mode with one-time bootstrap fanout
        self._bootstrapped: bool = False

        # Per-topic value caches: topic -> OrderedDict[digest -> value]
        self._values: Dict[str, "OrderedDict[str, Any]"] = {
            topic: OrderedDict() for topic in self.requires
        }
        # Active targets observed via resolve(target) that match provides pattern
        self._active_targets: Set[str] = set()
        # Seen combination keys per-target: target -> Set[tuple of digests in self.requires order]
        self._combos_seen_by_target: Dict[str, Set[tuple[str, ...]]] = {}

        # Compile provides into a regex; literal strings remain compatible via fullmatch
        self._provides_re = re.compile(self.provides)

        # Persistent caching toggle for combo-level caching
        self._cache_enabled: bool = bool(cache)

    def process(self, context: Context, event: ReactiveEvent):
        """Handle resolve kickoffs and prerequisite built events."""
        match event:
            case ReactiveEvent(name="resolve", target=target) if self._target_matches(target):
                # Track the resolved target
                self._active_targets.add(target)

                # One-time bootstrap: fan out prerequisite resolves
                if not self._bootstrapped:
                    self._bootstrapped = True
                    if self.requires:
                        for req in self.requires:
                            context.submit(
                                ReactiveEvent(name="resolve", target=req, sender=self)
                            )

                # Backfill all combinations for this target from current caches
                self._produce_all_combinations_for_target(context, target)

            case ReactiveEvent(name="built", target=topic, artifact=value) if (topic in self._values):
                # Record the value (de-dup happens in cache); generate combos (seen-set prevents reprocessing)
                self._add_value(topic, value)
                self._produce_new_combinations(context, topic, value)

    # --------------------------
    # Internal helpers
    # --------------------------

    def _digest(self, obj: Any) -> str:
        """Content digest for de-duplication; uses dill for broad Python object coverage."""
        try:
            data = dill.dumps(obj)
        except Exception:
            # Fallback: best-effort repr
            data = repr(obj).encode("utf-8", errors="ignore")
        return hashlib.sha256(data).hexdigest()

    def _add_value(self, topic: str, value: Any) -> bool:
        """Insert value into topic cache if new by digest. Returns True if inserted."""
        digest = self._digest(value)
        bucket = self._values[topic]
        if digest in bucket:
            return False
        bucket[digest] = value
        return True

    def _produce_new_combinations(self, context: Context, new_topic: str, new_value: Any):
        """Create and process combinations including the new value for new_topic across all active targets."""
        # Build lists of value sequences per topic in requires order.
        # For the topic that just received a new value, only use that value;
        # For others, use all cached values.
        per_topic_values: List[List[Any]] = []
        for topic in self.requires:
            if topic == new_topic:
                per_topic_values.append([new_value])
            else:
                per_topic_values.append(list(self._values[topic].values()))

        # Iterate Cartesian product and process unseen combinations per active target
        for values_tuple in itertools.product(*per_topic_values):
            combo_key = tuple(self._digest(v) for v in values_tuple)
            for target in self._active_targets:
                seen = self._combos_seen_by_target.setdefault(target, set())
                if combo_key in seen:
                    continue
                seen.add(combo_key)
                self._invoke_and_emit(context, target, values_tuple)

    def _produce_all_combinations_for_target(self, context: Context, target: str):
        """For a given active target, (back)produce all combinations from current caches exactly once."""
        per_topic_values: List[List[Any]] = [list(self._values[topic].values()) for topic in self.requires]
        # When there are no requires, itertools.product() with no inputs yields a single empty tuple
        for values_tuple in itertools.product(*per_topic_values):
            combo_key = tuple(self._digest(v) for v in values_tuple)
            seen = self._combos_seen_by_target.setdefault(target, set())
            if combo_key in seen:
                continue
            seen.add(combo_key)
            self._invoke_and_emit(context, target, values_tuple)

    def _target_matches(self, target: str) -> bool:
        """Return True when the target name matches the provides regex pattern (fullmatch)."""
        return self._provides_re.fullmatch(target) is not None

    def _combo_cache_key(self, target: str, args: Sequence[Any]) -> str:
        """Compute persistent cache key for (class source, provides pattern, actual target, ordered input digests)."""
        input_digests = tuple(self._digest(v) for v in args)
        key_parts = [
            inspect.getsource(self.__class__),
            self.provides,
            target,
            input_digests,
        ]
        return hashlib.sha256(dill.dumps(key_parts)).hexdigest()

    def _invoke_and_emit(self, context: Context, target: str, args: Sequence[Any]):
        """Invoke build with provided args (in requires order) or replay from persistent cache; emit items."""
        # Persistent combo-level caching
        if self._cache_enabled:
            digest = self._combo_cache_key(target, args)
            archive = self.archive(context.pipeline.workspace)
            if digest in archive:
                for cached_event in archive[digest]:
                    context.submit(cached_event)
                return

        # Cache miss or caching disabled: call build and stream items, then archive if enabled
        try:
            artifact_or_iter = self.build(context, target, *args)
        except Exception:
            logger.error("Reactive build failed for target %s", target, exc_info=True)
            raise

        # Normalize to iterable of items
        if isinstance(artifact_or_iter, types.GeneratorType):
            iterator = artifact_or_iter
        elif isinstance(artifact_or_iter, Iterable) and not isinstance(
            artifact_or_iter, (str, bytes, bytearray, dict)
        ):
            iterator = iter(artifact_or_iter)
        else:
            iterator = iter((artifact_or_iter,))

        created_events: List[ReactiveEvent] = []
        for item in iterator:
            ev = ReactiveEvent(
                name="built",
                target=target,
                sender=self,
                artifact=item,
            )
            created_events.append(ev)
            context.submit(ev)

        if self._cache_enabled:
            digest = self._combo_cache_key(target, args)
            archive = self.archive(context.pipeline.workspace)
            archive[digest] = tuple(created_events)

    # --------------------------
    # API for subclasses
    # --------------------------

    def build(self, context: Context, target: str, *args, **kwargs):
        """Construct and return an iterable/generator of artifacts for (target, args).

        Args:
            context: Execution context
            target: Target (actual matched name)
            *args: Values in the exact order of self.requires
            **kwargs: Reserved for custom subclass parameters

        Returns:
            Iterable or generator of items to emit as built(target, artifact=item).
        """
        raise NotImplementedError("ReactiveBuilder.build must be implemented by subclasses")


def reactive(provides: str, requires: List[str], cache: bool = False):
    """Class decorator to bind provides/requires for ReactiveBuilder subclasses.

    Args:
        provides: The provided topic name or regex pattern for the reactive builder.
        requires: A list of prerequisite topic names (duplicates are deduplicated preserving order).
        cache: When True, enable persistent combo-level caching of emitted built() events keyed by
               [class source, provides pattern, target, ordered input digests], using the archive
               from AbstractProcessor.archive(). Resolve events are not cached.

    Example:
        @reactive(provides="A", requires=["X", "Y"])
        class JoinXY(ReactiveBuilder):
            def build(self, context, target, x, y):
                yield (x, y)
    """

    def decorator(cls):
        assert issubclass(cls, ReactiveBuilder)
        cls.__init__ = functools.partialmethod(
            cls.__init__,
            provides=provides,
            requires=list(requires),
            cache=cache,
        )
        return cls

    return decorator