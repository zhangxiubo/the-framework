"""Reactive builders layered on top of the core event pipeline."""

from __future__ import annotations

import abc
import re
import functools
import itertools
import logging
import random
import heapq
from collections import OrderedDict, defaultdict, deque
from collections.abc import Callable, Iterable
from typing import Any, Collection, List, Optional

import deepdiff
from framework.core import InMemCache

from .core import AbstractProcessor, Context, Event, timeit, NoOpCache

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
            expand = r.startswith('*')
            name = re.sub(r'^\*', '', r)
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

    def __init__(self, provides: str, requires: Collection[str], keys=Collection[Callable[[Any], Hashable]], **kwargs) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.index = defaultdict(list)
        self.keys = keys

    def build(self, context: Context, item: Tuple[int, Any]):
        i, (v, *_) = item
        key = self.keys[i]
        k = key(v)
        self.index[k].append((i, v))
        if len(  self.index[k]  ) == len(self.keys):
            # we are done with this key
            if len({i for i, _ in self.index[k]}) != len(self.keys):  # we expect all keys to be unique
                print('duplicated payload detected')
                assert False
            yield tuple(e for _, e in sorted(self.index[k]) )
            del self.index[k]

class RendezvousReceiver(ReactiveBuilder):

    def __init__(self, provides: str, requires: Collection[str], index, **kwargs) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.index = index

    def build(self, context: Context, *args, **kwargs):
        yield self.index, args


def make_rendezvous(provides: str, requires: Collection[str], keys=Collection[Callable[[Any], Hashable]]):
    import uuid
    topic = str(uuid.uuid4())
    return RendezvousPublisher(provides, [topic], keys), *tuple(RendezvousReceiver(topic, [require], i) for i, require in enumerate(requires))
