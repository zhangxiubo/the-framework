"""Reactive builders layered on top of the core event pipeline."""

from __future__ import annotations

import abc
import functools
import heapq
import itertools
import logging
import random
import re
from collections import OrderedDict, defaultdict, deque
from collections.abc import Callable, Hashable, Iterable
from typing import Any, Collection, List, Optional, Tuple, Type

import deepdiff

from framework.core import InMemCache
from .core import AbstractProcessor, Context, Event, timeit

logger = logging.getLogger(__name__)


class ReactiveEvent(Event):
    """Event subtype used by the reactive layer for `resolve`/`built` flows."""

    pass


class ReactiveBuilder(AbstractProcessor):
    """Base class for dependency-aware processors that react to ``ReactiveEvent``s.

    ``requires`` defines upstream targets. Prefix a requirement with ``*`` to expand
    tuple artifacts into individual arguments before passing them to ``build``.
    When ``persist`` is ``True`` the builder stores its outputs in the pipeline archive
    and future ``resolve`` requests are served by the replay-only ``reply`` handler.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        persist: bool = False,
        name: Optional[str] = None,
        priority: int = 0,
        **kwargs,
        # todo: optionally allow the setting of a name argument and use it to distinguish the cache store for different instances of the same ReactiveBuilder subclass
    ):
        super().__init__(
            priority=priority,
            name=name,
        )
        self.provides: str = provides
        self.require_specs = []
        normalized = OrderedDict()
        for r in requires:
            expand = r.startswith("*")
            name = re.sub(r"^\*", "", r)
            self.require_specs.append((name, expand))
            normalized.setdefault(name, None)
        self.requires: List[str] = list(normalized.keys())
        self.require_index = {name: idx for idx, name in enumerate(self.requires)}
        self.handlers = {self.new, self.listen}
        self.input_store = defaultdict(deque)  # requrie -> artifacts (ingridents)
        self.persist = persist

    def _process(self, context: Context, event: ReactiveEvent):
        """Dispatch to the currently-installed handler set."""
        for handler in list(self.handlers):
            handler(context, event)

    def process(self, context: Context, event: ReactiveEvent):
        """Handle lifecycle events plus the reactive ``resolve``/``built`` traffic."""
        match event:
            case ReactiveEvent() as e:
                self._process(context, e)
            case Event(name="__INIT__") as e:
                self.on_init(context)
            case Event(name="__POISON__") as e:
                self.on_terminate(context)
            case Event(name="__PHASE__", phase=phase) as e:
                for artifact in self.phase(context, phase):
                    context.submit(
                        ReactiveEvent(
                            name="built", target=self.provides, artifact=artifact
                        )
                    )

    def publish(self, context: Context):
        """Attempt to build artifacts once every requirement has at least one value."""
        keys = [tuple()]
        if len(self.requires) > 0:
            keys = list(zip(*[self.input_store[require] for require in self.requires]))

        for key in keys:
            # each key in keys is a tuple of known input arguments
            skey = deepdiff.DeepHash(key)[key]
            archive = self.get_cache(context)
            if skey not in archive:
                with timeit(self.build.__qualname__, logger=logger):
                    args = []
                    for req_name, expand in self.require_specs:
                        value = key[self.require_index[req_name]]
                        if expand:
                            args.extend(tuple(value))
                        else:
                            args.append(value)
                    artifacts = self.build(context, *args)
                    assert isinstance(artifacts, Iterable)
                    collected = list()
                    for artifact in artifacts:
                        context.submit(
                            ReactiveEvent(
                                name="built", target=self.provides, artifact=artifact
                            )
                        )
                        collected.append(artifact)
                    archive[skey] = collected
            for require in self.requires:
                self.input_store[require].popleft()

    def new(self, context: Context, event: ReactiveEvent):
        """Initial ``resolve`` handler installing the steady-state `reply`/`react` pair."""
        match event:
            case ReactiveEvent(name="resolve", target=target) if (
                self.provides == target
            ):
                self.handlers -= {self.new, self.listen}
                self.reply(context, event)
                self.publish(context)
                self.handlers |= {self.reply, self.react}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))

    def reply(self, context: Context, event: ReactiveEvent):
        """Replay cached artifacts when a ``resolve`` arrives for ``provides``."""
        match event:
            case ReactiveEvent(name="resolve", target=target) if (
                self.provides == target
            ):
                # someone is asking for "target"
                # we should tell them about all the things we know about target
                # by publishing all the values we have built for target
                # the values we publish shall be picked up by all who are interested
                # the initial blanket "resolve-broadcast" hopefully should be brief
                for artifacts in self.get_cache(context).values():
                    # retrieve the things we have built for target
                    # build_cache will be a dictionary indexed by target and whose value is another ordered dictionary that maps key -> artifact
                    for artifact in artifacts:
                        context.submit(
                            ReactiveEvent(
                                name="built",
                                target=target,
                                artifact=artifact,
                            )
                        )

    def listen(self, context: Context, event: ReactiveEvent):
        """Collect artifacts during the bootstrap phase before publication begins."""
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)

    def react(self, context: Context, event: ReactiveEvent):
        """Steady-state handler: consume new artifacts and trigger builds."""
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)
                self.publish(context)

    @abc.abstractmethod
    def build(self, context: Context, *args, **kwargs):
        """Yield artifacts to publish for ``self.provides``."""
        pass

    def phase(self, context: Context, phase: int):
        """Optional hook giving builders a chance to emit work during phase ticks."""
        return tuple()

    def on_init(self, context: Context):
        """Called when the pipeline emits ``__INIT__``."""
        pass

    def on_terminate(self, context: Context):
        """
        Overriding processors should call super().on_terminate(context) to ensure cache stores are properly closed.
        """
        self.get_cache(context).flush_wal(sync=True)

    def get_cache(self, context: Context):
        """Return a persistence-aware archive keyed by processor signature."""
        if self.persist:
            return context.pipeline.archive(self.signature())
        else:
            return self.get_noop_cache()

    @functools.cache
    def get_noop_cache(self):
        """Memoized in-memory cache for non-persistent builders."""
        return InMemCache()


def reactive(
    provides: str,
    requires: List[str],
    persist: bool = False,
):
    """Class decorator that partially applies ``ReactiveBuilder.__init__``."""

    def decorator(cls):
        assert issubclass(cls, ReactiveBuilder)
        cls.__init__ = functools.partialmethod(
            cls.__init__,
            provides=provides,
            requires=list(requires),
            persist=persist,
        )
        return cls

    return decorator


class Collector(ReactiveBuilder):
    """Batch inputs during a phase and forward them once per phase if changed."""

    def __init__(self, forwards_to, topics, limit=None):
        super().__init__(forwards_to, topics, persist=False)
        self.current_batch = list()
        self.past_batches = list()
        self.limit = float("inf") if limit is None else limit

    def build(self, context, *args, **kwargs):
        """Accumulate incoming artifacts up to the configured limit."""
        if len(self.current_batch) < self.limit:
            self.current_batch.append(args)
        yield from []

    def phase(self, context, phase):
        """Emit the batch when advancing phases and remember historical values."""
        self.past_batches.append(self.current_batch)
        if self.current_batch:
            yield self.current_batch
        self.current_batch = list()

    def values(self):
        """Iterate over all values ever seen."""
        return itertools.chain.from_iterable(self.past_batches + [self.current_batch])


class Grouper(ReactiveBuilder):
    """Group artifacts by a derived key per phase."""

    def __init__(self, forwards_to, topics, keyfunc: Callable[..., Any]):
        super().__init__(forwards_to, topics, persist=False)
        self.keyfunc = keyfunc
        self.store = defaultdict(list)
        self.last = dict()

    def build(self, context, *args):
        """Collect arguments under the derived key until the phase boundary."""
        key = self.keyfunc(*args)
        self.store[key].append(args)
        yield from []

    def phase(self, context, phase):
        """On each phase, emit all groups and reset the working store."""
        self.last = self.store.copy()
        self.store.clear()
        if self.last:
            for key, group in self.last.items():
                yield key, group


class StreamGrouper(ReactiveBuilder):
    """Streaming grouper that emits whenever the key changes."""

    _UNSET = object()  # Sentinel that can't collide with any user key

    def __init__(
        self,
        provides,
        requires,
        keyfunc: Callable[..., Any],
        **kwargs,
    ):
        super().__init__(provides, requires, persist=False, **kwargs)
        self.lastkey = self._UNSET
        self.lastset = []
        self.keyfunc = keyfunc

    def build(self, context, *args):
        """Emit the previous key's group when a new key is observed."""
        thiskey = self.keyfunc(*args)
        if thiskey == self.lastkey:
            self.lastset.append(args)
        else:
            if self.lastkey is not self._UNSET:
                yield self.lastkey, self.lastset
            self.lastkey = thiskey
            self.lastset = [args]

    def phase(self, context, phase):
        """Flush any trailing group so nothing is lost between phases."""
        if self.lastkey is not self._UNSET:
            yield self.lastkey, self.lastset
        self.lastkey = self._UNSET
        self.lastset = []


class StreamSampler(ReactiveBuilder):
    """Emit each artifact with a fixed probability."""

    def __init__(self, provides, requires, probability: float, seed=42, **kwargs):
        super().__init__(provides, requires, **kwargs)
        self.probability = probability
        self.seed = seed
        self.random = random.Random(seed)

    def build(
        self,
        context,
        *args,
    ):
        """Yield ``args`` whenever the RNG draw is beneath ``probability``."""
        if self.random.random() < self.probability:
            yield args


class ReservoirSampler(ReactiveBuilder):
    """Bounded-size random sample maintained via reservoir sampling."""

    def __init__(self, provides, requires, size: int, **kwargs):
        super().__init__(provides, requires, **kwargs)
        self.size = size
        self.heap = []

    def build(
        self,
        context: Context,
        *args,
    ):
        """Keep a max-heap of random priorities and trim it to ``size``."""
        seed = deepdiff.DeepHash(args)[args]
        rng = random.Random(seed)
        heapq.heappush(self.heap, (rng.random(), args))
        if len(self.heap) > self.size:
            heapq.heappop(self.heap)
        yield from []

    def phase(self, context, phase):
        """Drain the heap and emit the sampled items."""
        while self.heap:
            _, item = heapq.heappop(self.heap)
            yield item


class RendezvousPublisher(ReactiveBuilder):
    """Joins items from multiple streams by matching keys.

    Items from each input stream are collected and indexed by their extracted key.
    When all streams have contributed an item with the same key, the joined tuple
    is emitted, ordered by stream index.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        keys: Collection[Callable[[Any], Hashable]],
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        # index maps key -> dict of {stream_index: value}
        # Using dict instead of list prevents duplicates from same stream
        self.index: dict[Hashable, dict[int, Any]] = defaultdict(dict)
        self.keys = keys

    def build(self, context: Context, item: Tuple[int, Any]):
        i, (v, *_) = item
        key_func = self.keys[i]
        k = key_func(v)

        # Check for duplicate key from same stream
        if i in self.index[k]:
            logger.warning(
                f"Rendezvous: duplicate key '{k}' from stream {i}, "
                f"overwriting previous value"
            )

        self.index[k][i] = v

        if len(self.index[k]) == len(self.keys):
            # All streams have contributed - emit the joined result
            result = tuple(self.index[k][idx] for idx in range(len(self.keys)))
            del self.index[k]
            yield result

    def clear_orphaned_keys(self) -> int:
        """Remove keys that will never complete. Returns count of removed keys.

        Call this periodically to prevent memory leaks from unmatched keys.
        """
        count = len(self.index)
        self.index.clear()
        return count


class RendezvousReceiver(ReactiveBuilder):
    """Tags incoming artifacts with a stream index for the RendezvousPublisher.

    Each receiver is assigned an index corresponding to its position in the
    list of required streams. It wraps each incoming artifact as (index, args)
    for downstream processing by the publisher.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        index: int,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.index = index

    def build(self, context: Context, *args, **kwargs):
        yield self.index, args


def make_rendezvous(
    provides: str,
    requires: Collection[str],
    keys: Collection[Callable[[Any], Hashable]],
) -> Tuple[RendezvousPublisher, ...]:
    """Create a rendezvous pattern that joins multiple input streams by key.

    Args:
        provides: The target name for the joined output.
        requires: Collection of input stream names to join.
        keys: Collection of key extraction functions, one per input stream.
              Each function extracts a hashable key from items in its stream.

    Returns:
        A tuple of (publisher, receiver1, receiver2, ...) where:
        - publisher: The RendezvousPublisher that emits joined tuples
        - receivers: One RendezvousReceiver per input stream

    Example:
        >>> publisher, recv_a, recv_b = make_rendezvous(
        ...     provides="joined",
        ...     requires=["stream_a", "stream_b"],
        ...     keys=[lambda x: x["id"], lambda x: x["id"]],
        ... )
    """
    import uuid

    keys_list = list(keys)  # Materialize to support generators
    if len(requires) != len(keys_list):
        raise ValueError(
            f"Number of key functions ({len(keys_list)}) must match "
            f"number of required streams ({len(requires)})"
        )

    topic = str(uuid.uuid4())
    publisher = RendezvousPublisher(provides, [topic], keys_list)
    receivers = tuple(
        RendezvousReceiver(topic, [require], i) for i, require in enumerate(requires)
    )
    return (publisher, *receivers)


class LoadBalancerSequencer(ReactiveBuilder):
    """Tags each input with a monotonically increasing sequence number."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self._next_seq = 0

    def build(self, context: Context, *args):
        seq = self._next_seq
        self._next_seq += 1
        yield seq, args


class LoadBalancerRouter(ReactiveBuilder):
    """Routes items to a specific worker based on sequence number modulo.

    Each router has a worker_id and only forwards items where
    seq % num_workers == worker_id. This distributes work evenly
    across workers while maintaining deterministic routing.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        worker_id: int,
        num_workers: int,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.worker_id = worker_id
        self.num_workers = num_workers

    def build(self, context: Context, item: Tuple[int, Tuple[Any, ...]]):
        seq, args = item
        if seq % self.num_workers == self.worker_id:
            yield item


class LoadBalancerWorkerWrapper(ReactiveBuilder):
    """Wraps a worker builder and preserves FIFO ordering metadata.

    Input artifact is `(seq, args)` and output artifact is `(seq, results)` where
    `results` is a list of artifacts produced by the wrapped worker.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        worker: ReactiveBuilder,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.worker = worker

    def build(self, context: Context, item: Tuple[int, Tuple[Any, ...]]):
        seq, args = item
        results = list(self.worker.build(context, *args))
        yield seq, results


class LoadBalancerWorkerEmitter(ReactiveBuilder):
    """Runs a wrapped worker and emits artifacts immediately.

    This is the low-overhead alternative to the FIFO collector path. It disables
    any cross-worker reassembly/reordering: artifacts are emitted as soon as the
    worker produces them.

    Input artifact is `(seq, args)`; the sequence number is only used for routing
    and is not included in output artifacts.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        worker: ReactiveBuilder,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.worker = worker

    def build(self, context: Context, item: Tuple[int, Tuple[Any, ...]]):
        _seq, args = item
        yield from self.worker.build(context, *args)


class LoadBalancerCollector(ReactiveBuilder):
    """Reorders worker results back into FIFO sequence order."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.buffer: dict[int, List[Any]] = {}
        self._next_expected = 0

    def build(self, context: Context, item: Tuple[int, List[Any]]):
        seq, results = item
        self.buffer[seq] = list(results)

        while self._next_expected in self.buffer:
            ready = self.buffer.pop(self._next_expected)
            self._next_expected += 1
            for artifact in ready:
                yield artifact

    def get_pending_count(self) -> int:
        return len(self.buffer)

    def get_next_expected_seq(self) -> int:
        return self._next_expected


def make_load_balancer(
    provides: str,
    requires: Collection[str],
    worker_class: Type[ReactiveBuilder],
    num_workers: int,
    worker_init_args: Tuple[Any, ...] = (),
    worker_init_kwargs: Optional[dict[str, Any]] = None,
    preserve_fifo: bool = True,
) -> List[ReactiveBuilder]:
    """Build a load balancer pipeline.

    When ``preserve_fifo`` is True (default), results are reassembled into the
    original input order using a collector stage. When False, artifacts are
    emitted as soon as workers produce them (no reassembly/reordering).

    Returns components in this order:
      - preserve_fifo=True:  sequencer, (router_i, wrapper_i)*, collector
      - preserve_fifo=False: sequencer, (router_i, emitter_i)*
    """

    if num_workers < 1:
        raise ValueError("num_workers must be >= 1")

    worker_init_kwargs = {} if worker_init_kwargs is None else dict(worker_init_kwargs)

    requires_list = list(requires)
    requires_tag = "+".join(requires_list)
    base = f"load_balancer::{provides}::{worker_class.__name__}::{requires_tag}"

    sequenced = f"{base}::sequenced"
    results = f"{base}::results"

    components: List[ReactiveBuilder] = [
        LoadBalancerSequencer(provides=sequenced, requires=requires_list),
    ]

    for worker_id in range(num_workers):
        routed = f"{base}::routed::{worker_id:02d}"
        components.append(
            LoadBalancerRouter(
                provides=routed,
                requires=[sequenced],
                worker_id=worker_id,
                num_workers=num_workers,
            )
        )

        worker_name = f"{base}::_worker_{worker_id}"
        worker = worker_class(
            *worker_init_args,
            name=worker_name,
            **worker_init_kwargs,
        )
        if preserve_fifo:
            components.append(
                LoadBalancerWorkerWrapper(
                    provides=results,
                    requires=[routed],
                    worker=worker,
                )
            )
        else:
            components.append(
                LoadBalancerWorkerEmitter(
                    provides=provides,
                    requires=[routed],
                    worker=worker,
                )
            )

    if preserve_fifo:
        components.append(LoadBalancerCollector(provides=provides, requires=[results]))
    return components


def parallelize(
    provides: str,
    requires: Collection[str],
    worker_builder: Type[ReactiveBuilder],
    num_workers: int,
    preserve_fifo: bool = True,
    worker_init_args: Tuple[Any, ...] = (),
    **kwargs,
) -> Tuple[ReactiveBuilder, ...]:
    """Backward-compatible alias for make_load_balancer.

    Set ``preserve_fifo=False`` to disable output reassembly/reordering.
    Use ``worker_init_args`` for workers requiring positional constructor args.
    """
    components = make_load_balancer(
        provides=provides,
        requires=requires,
        worker_class=worker_builder,
        num_workers=num_workers,
        worker_init_args=worker_init_args,
        worker_init_kwargs=kwargs,
        preserve_fifo=preserve_fifo,
    )
    return tuple(components)
