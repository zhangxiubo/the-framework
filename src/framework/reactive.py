"""
Reactive builder: dependency-driven, streaming composition over the event runtime.

This module implements ReactiveBuilder, a processor that:
- Subscribes to one or more prerequisite topics ("requires")
- Publishes results to a provided topic ("provides")
- Triggers upstream resolution on first resolve() request
- Enters a listen mode; for every complete set of input values (Cartesian product),
  it invokes build(context, target, *args) and streams each produced element to subscribers.

Protocol (mirrors builder.BuilderEvent):
- Incoming control:
  - ReactiveEvent(name="resolve", target=provides, sender=client)  # subscribe client and start
- Fan-out to prerequisites:
  - ReactiveEvent(name="resolve", target=require_i, sender=self)
- Incoming data from prerequisites:
  - ReactiveEvent(name="built", target=require_i, artifact=value, to=self)
- Outgoing streamed results to subscribers:
  - ReactiveEvent(name="built", target=provides, sender=self, to=subscriber, artifact=item)

Behavior with multiple inputs:
- Maintains a per-topic cache of distinct values (by content digest).
- On each new value for topic T, if all topics have at least one value:
  - Compute the Cartesian product where T contributes only the new value and other topics
    contribute all cached values; invoke build for each unseen combination.
- Maintains a per-combination cache (by digest of the ordered tuple of value digests) to avoid
  recomputing previously processed combinations during the run.

Notes:
- Requires are deduplicated while preserving the first occurrence (like AbstractBuilder).
- Subscribers are deduplicated by identity (object instance).
- Generators are iterated; general iterables are enumerated (strings/bytes are treated as singletons).
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import types
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
            - built: target, artifact, sender, to
    """

    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


class ReactiveBuilder(AbstractProcessor):
    """Streaming, dependency-aware builder.

    A ReactiveBuilder:
    - Provides a single target topic (provides)
    - Requires zero or more prerequisite topics (requires)
    - On first resolve(provides), fans out resolve(require_i) and enters listen mode
    - As prerequisite values arrive (built(..., to=self)), it computes Cartesian
      products across cached values and invokes build(context, provides, *args)
    - Emits each produced element as built(provides, artifact=item, to=subscriber) to all subscribers

    Caching:
    - Per-topic value cache (digest -> value) de-duplicates identical values by content
    - Combination cache prevents re-invocation of build for already-seen value tuples
      during the lifetime of the processor (no persistence across runs)

    Concurrency:
    - Single-threaded per instance (enforced by the Pipeline/Inbox)
    """

    def __init__(self, provides: str, requires: Collection[str], cache: bool = False):
        super().__init__()
        # Public configuration
        self.provides: str = provides
        # Deduplicate requires while preserving order (first occurrence wins)
        self.requires: List[str] = list(dict.fromkeys(requires))

        # State-machine style (mirrors AbstractBuilder): start in (new, listening)
        # - new: perform one-time fan-out of prerequisite resolves
        # - listening: accept subscribers and stream outputs as data arrives
        self.current_states: Set = {self.new, self.listening}

        # Subscribers (deduped by identity)
        self._subscribers: List[Any] = []

        # Per-topic value caches: topic -> OrderedDict[digest -> value]
        self._values: Dict[str, "OrderedDict[str, Any]"] = {
            topic: OrderedDict() for topic in self.requires
        }
        # Seen combination keys (tuple of digests in self.requires order)
        self._combos_seen: Set[tuple[str, ...]] = set()

        # Placeholder for future persistent caching toggle (currently unused)
        self._cache_enabled: bool = bool(cache)

    def process(self, context: Context, event: ReactiveEvent):
        """Dispatch the incoming event to active state handlers with deterministic ordering.

        Rationale:
            For zero-prerequisite builders, we must ensure subscribers are registered before the one-time
            build emit. That requires invoking 'listening' before 'new' on resolve events.
        """
        match event:
            case ReactiveEvent(name="resolve"):
                # Invoke 'listening' first to register subscribers, then 'new' to kick off once.
                for state in (self.listening, self.new):
                    if state in self.current_states:
                        state(context, event)
            case ReactiveEvent(name="built"):
                # Only 'listening' handles built events
                if self.listening in self.current_states:
                    self.listening(context, event)

    def new(self, context: Context, event: ReactiveEvent):
        """State: initial; perform one-time kick-off on first resolve(provides).

        Behavior mirrors AbstractBuilder's 'new' state:
        - If prerequisites exist: fan out resolve(require_i) once
        - If no prerequisites: invoke build exactly once
        - Then drop 'new' from current_states
        """
        match event:
            case ReactiveEvent(name="resolve", target=target) if (
                target == self.provides
            ):
                if self.requires:
                    # Fan out prerequisite resolutions only once
                    for req in self.requires:
                        context.submit(
                            ReactiveEvent(name="resolve", target=req, sender=self)
                        )
                else:
                    # Zero-prerequisite reactive: invoke build exactly once
                    self._invoke_and_emit(context, ())
                # Transition: drop 'new' state
                self.current_states -= {self.new}

    def listening(self, context: Context, event: ReactiveEvent):
        """State: accept subscribers and stream outputs as prerequisite data arrives."""
        match event:
            # Add/track subscribers when sender is provided
            case ReactiveEvent(name="resolve", target=target, sender=reply_to) if (
                target == self.provides
            ):
                self._add_subscriber(reply_to)
            # Accept resolve without a sender (no subscription)
            case ReactiveEvent(name="resolve", target=target) if (
                target == self.provides
            ):
                pass

            # Incoming prerequisite values
            case ReactiveEvent(
                name="built", target=topic, artifact=value, to=to_obj
            ) if (to_obj is self and topic in self._values):
                # Record the new value; if it is not a duplicate, produce all new combos
                is_new = self._add_value(topic, value)
                if is_new:
                    self._produce_new_combinations(context, topic, value)

    # --------------------------
    # Internal helpers
    # --------------------------

    def _add_subscriber(self, obj: Any):
        """Add a subscriber if not already present (by identity)."""
        for existing in self._subscribers:
            if existing is obj:
                return
        self._subscribers.append(obj)

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

    def _all_topics_have_values(self) -> bool:
        return all(len(bucket) > 0 for bucket in self._values.values()) or not self.requires

    def _produce_new_combinations(self, context: Context, new_topic: str, new_value: Any):
        """Create and process combinations including the new value for new_topic."""
        if not self._all_topics_have_values():
            return

        # Build lists of value sequences per topic in requires order.
        # For the topic that just received a new value, only use that value;
        # For others, use all cached values.
        per_topic_values: List[List[Any]] = []
        for topic in self.requires:
            if topic == new_topic:
                per_topic_values.append([new_value])
            else:
                per_topic_values.append(list(self._values[topic].values()))

        # Iterate Cartesian product and process unseen combinations
        for values_tuple in itertools.product(*per_topic_values):
            combo_key = tuple(self._digest(v) for v in values_tuple)
            if combo_key in self._combos_seen:
                continue
            self._combos_seen.add(combo_key)
            self._invoke_and_emit(context, values_tuple)

    def _invoke_and_emit(self, context: Context, args: Sequence[Any]):
        """Invoke build with provided args (in requires order), emit each produced item."""
        try:
            artifact_or_iter = self.build(context, self.provides, *args)
        except Exception:
            logger.error("Reactive build failed for target %s", self.provides, exc_info=True)
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

        # Stream each item to all current subscribers
        for item in iterator:
            for subscriber in self._subscribers:
                context.submit(
                    ReactiveEvent(
                        name="built",
                        target=self.provides,
                        sender=self,
                        to=subscriber,
                        artifact=item,
                    )
                )

    # --------------------------
    # API for subclasses
    # --------------------------

    def build(self, context: Context, target: str, *args, **kwargs):
        """Construct and return an iterable/generator of artifacts for (target, args).

        Args:
            context: Execution context
            target: Target (self.provides)
            *args: Values in the exact order of self.requires
            **kwargs: Reserved for custom subclass parameters

        Returns:
            Iterable or generator of items to emit as built(target, artifact=item).
        """
        raise NotImplementedError("ReactiveBuilder.build must be implemented by subclasses")


def reactive(provides: str, requires: List[str], cache: bool = False):
    """Class decorator to bind provides/requires for ReactiveBuilder subclasses.

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