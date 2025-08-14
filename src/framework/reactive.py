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

Refactor overview (escalated abstractions):
- State management (values, targets, combination-seen) is encapsulated in _ReactiveState.
- Persistent caching is encapsulated behind a small policy interface (_NullCache | _ArchiveComboCache).
- Combination generation is unified through _produce_for_targets(), reducing branching.
- Build, invocation, and emission logic are encapsulated in a `BuildPolicy`.
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import re
import inspect
import abc
from collections import OrderedDict
from typing import Any, Collection, Dict, Iterable, List, Sequence, Set, Optional

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


# --------------------------
# Internal abstractions
# --------------------------

class _ReactiveState:
    """Encapsulates reactive runtime state: values, active targets, and seen combinations."""

    def __init__(self, topics: List[str]):
        self.topics: List[str] = list(topics)
        # Per-topic value caches: topic -> OrderedDict[digest -> value]
        self.values: Dict[str, "OrderedDict[str, Any]"] = {
            topic: OrderedDict() for topic in self.topics
        }
        # Active resolved targets that matched the provides pattern
        self.active_targets: Set[str] = set()
        # Combination seen-set per-target (tuple of per-value digests in requires order)
        self.combos_seen_by_target: Dict[str, Set[tuple[str, ...]]] = {}

    def add_target(self, target: str) -> None:
        self.active_targets.add(target)

    def _digest(self, obj: Any) -> str:
        """Content digest for de-duplication; uses dill for broad Python object coverage."""
        try:
            data = dill.dumps(obj)
        except Exception:
            # Fallback: best-effort repr
            data = repr(obj).encode("utf-8", errors="ignore")
        return hashlib.sha256(data).hexdigest()

    def add_value(self, topic: str, value: Any) -> bool:
        """Insert value into topic cache if new by digest. Returns True if inserted."""
        digest = self._digest(value)
        bucket = self.values[topic]
        if digest in bucket:
            return False
        bucket[digest] = value
        return True

    def collect_values(
        self, new_topic: Optional[str] = None, new_value: Any = None
    ) -> List[List[Any]]:
        """Assemble per-topic value lists, optionally injecting a new value for one topic."""
        per_topic: List[List[Any]] = []
        for topic in self.topics:
            if topic == new_topic:
                per_topic.append([new_value])
            else:
                per_topic.append(list(self.values[topic].values()))
        return per_topic

    def combo_key(self, values_tuple: Sequence[Any]) -> tuple[str, ...]:
        """Digest tuple for the ordered values in a combination."""
        return tuple(self._digest(v) for v in values_tuple)

    def mark_combo_if_new(self, target: str, key: tuple[str, ...]) -> bool:
        """Mark a combination key for a target; return True if not seen before."""
        seen = self.combos_seen_by_target.setdefault(target, set())
        if key in seen:
            return False
        seen.add(key)
        return True


class _NullCache:
    """Disabled persistent caching policy."""

    def replay_or_none(self, context: Context, owner: "ReactiveBuilder", target: str, args: Sequence[Any]):
        return None

    def store(self, context: Context, owner: "ReactiveBuilder", target: str, args: Sequence[Any], events: Sequence[ReactiveEvent]) -> None:
        return None


class _ArchiveComboCache:
    """Persistent combo-level caching policy using AbstractProcessor.archive(workspace)."""

    def replay_or_none(self, context: Context, owner: "ReactiveBuilder", target: str, args: Sequence[Any]):
        digest = owner._combo_cache_key(target, args)
        archive = owner.archive(context.pipeline.workspace)
        if digest in archive:
            return archive[digest]
        return None

    def store(self, context: Context, owner: "ReactiveBuilder", target: str, args: Sequence[Any], events: Sequence[ReactiveEvent]) -> None:
        digest = owner._combo_cache_key(target, args)
        archive = owner.archive(context.pipeline.workspace)
        archive[digest] = tuple(events)


class BuildPolicy(abc.ABC):
    """Policy for invoking a build and emitting results."""

    @abc.abstractmethod
    def execute(
        self, owner: "ReactiveBuilder", context: Context, target: str, args: Sequence[Any]
    ) -> List[ReactiveEvent]:
        """Invoke the build, emit events, and return the created events for caching."""
        pass


class DefaultBuildPolicy(BuildPolicy):
    """Default "build and stream" policy: iterate results and emit 'built' for each item."""

    def execute(
        self, owner: "ReactiveBuilder", context: Context, target: str, args: Sequence[Any]
    ) -> List[ReactiveEvent]:
        try:
            artifact_or_iter = owner.build(context, target, *args)
        except Exception:
            logger.error("Reactive build failed for target %s", target, exc_info=True)
            raise

        # Normalize to iterable of items
        if isinstance(artifact_or_iter, Iterable) and not isinstance(
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
                sender=owner,
                artifact=item,
            )
            created_events.append(ev)
            context.submit(ev)
        return created_events


# --------------------------
# Processor
# --------------------------

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

    def __init__(self, provides: str, requires: Collection[str], cache: bool = False, build_policy: BuildPolicy = None):
        super().__init__()
        # Public configuration
        self.provides: str = provides
        # Deduplicate requires while preserving order (first occurrence wins)
        self.requires: List[str] = list(dict.fromkeys(requires))

        # Always-listening mode with one-time bootstrap fanout
        self._bootstrapped: bool = False

        # Encapsulated runtime state
        self._state = _ReactiveState(self.requires)

        # Compile provides into a regex; literal strings remain compatible via fullmatch
        self._provides_re = re.compile(self.provides)

        # Configurable policies
        self._build_policy = build_policy or DefaultBuildPolicy()
        self._cache = _ArchiveComboCache() if cache else _NullCache()

    def process(self, context: Context, event: ReactiveEvent):
        """Handle resolve kickoffs and prerequisite built events."""
        match event:
            case ReactiveEvent(name="resolve", target=target) if self._target_matches(target):
                self._handle_resolve(context, target)

            case ReactiveEvent(name="built", target=topic, artifact=value) if (topic in self._state.values):
                self._handle_built(context, topic, value)

    # --------------------------
    # Internal orchestration
    # --------------------------

    def _handle_resolve(self, context: Context, target: str) -> None:
        """Track target, bootstrap once, and backfill all current combinations for this target."""
        self._state.add_target(target)
        self._bootstrap(context)
        self._produce_for_targets(context, [target])

    def _handle_built(self, context: Context, topic: str, value: Any) -> None:
        """Record new value and generate combinations that include it across all active targets."""
        if self._state.add_value(topic, value):
            self._produce_for_targets(context, self._state.active_targets, new_topic=topic, new_value=value)

    def _bootstrap(self, context: Context) -> None:
        """Fan out prerequisite resolve events once."""
        if self._bootstrapped:
            return
        self._bootstrapped = True
        for req in self.requires:
            context.submit(ReactiveEvent(name="resolve", target=req, sender=self))

    def _produce_for_targets(
        self,
        context: Context,
        targets: Iterable[str],
        new_topic: Optional[str] = None,
        new_value: Any = None,
    ) -> None:
        """Generate unseen Cartesian combinations for targets using current caches."""
        per_topic_values = self._state.collect_values(new_topic, new_value)
        self._produce_combinations(context, targets, per_topic_values)

    def _produce_combinations(
        self, context: Context, targets: Iterable[str], per_topic_values: List[List[Any]]
    ):
        """Process unseen Cartesian combinations for the given targets."""
        for values_tuple in itertools.product(*per_topic_values):
            combo_key = self._state.combo_key(values_tuple)
            for target in targets:
                if not self._state.mark_combo_if_new(target, combo_key):
                    continue
                self._invoke_and_emit(context, target, values_tuple)

    def _target_matches(self, target: str) -> bool:
        """Return True when the target name matches the provides regex pattern (fullmatch)."""
        return self._provides_re.fullmatch(target) is not None

    def _combo_cache_key(self, target: str, args: Sequence[Any]) -> str:
        """Compute persistent cache key for (class source, provides pattern, actual target, ordered input digests)."""
        # Reuse state's digest so equality matches in-memory combo tracking
        input_digests = tuple(self._state._digest(v) for v in args)
        key_parts = [
            inspect.getsource(self.__class__),
            self.provides,
            target,
            input_digests,
        ]
        return hashlib.sha256(dill.dumps(key_parts)).hexdigest()

    def _invoke_and_emit(self, context: Context, target: str, args: Sequence[Any]):
        """Invoke build with provided args (in requires order) or replay from persistent cache; emit items."""
        # Try persistent combo-level replay
        cached_events = self._cache.replay_or_none(context, self, target, args)
        if cached_events is not None:
            for ev in cached_events:
                context.submit(ev)
            return

        # Delegate build and emission to the policy
        created_events = self._build_policy.execute(self, context, target, args)

        # Persist if enabled
        self._cache.store(context, self, target, args, created_events)

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


def reactive(provides: str, requires: List[str], cache: bool = False, **kwargs):
    """Class decorator to bind provides/requires for ReactiveBuilder subclasses.

    Args:
        provides: The provided topic name or regex pattern for the reactive builder.
        requires: A list of prerequisite topic names (duplicates are deduplicated preserving order).
        cache: When True, enable persistent combo-level caching of emitted built() events keyed by
               [class source, provides pattern, target, ordered input digests], using the archive
               from AbstractProcessor.archive(). Resolve events are not cached.
        **kwargs: Forwarded to the `ReactiveBuilder` constructor (e.g., `build_policy`).
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
            **kwargs,
        )
        return cls

    return decorator
