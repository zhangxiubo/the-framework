from __future__ import annotations

import functools
import itertools
import logging
from pathlib import Path
import re
import abc
from collections import OrderedDict, defaultdict
from typing import Any, Collection, List
from collections.abc import Callable, Iterable
from smart_open import open

import deepdiff
from pydantic import BaseModel

from .core import AbstractProcessor, Context, Event

logger = logging.getLogger(__name__)


class ReactiveEvent(Event):
    pass


class CustomSet:

    def __init__(self):
        self.data = dict()

    def hash(self, item):
        digest = deepdiff.DeepHash(item)[item]
        return digest
    
    def add(self, item):
        digest = self.hash(item)
        self.data[digest] = item
    
    def __contains__(self, item):
        return self.hash(item) in self.data

    def __iter__(self):
        return iter(self.data.values())
    
    def __len__(self):
        return len(self.data)
    
    def __repr(self):
        return f"DeepHashSet({list(self.data.values())})"

# --------------------------
# Processor
# --------------------------


class ReactiveBuilder(AbstractProcessor):
    """Event-reactive combinator.

    Lifecycle:
        - Handlers set = {new, reply, listen}. process() dispatches each incoming ReactiveEvent
          to all handlers still registered in the set.
        - new(): On first resolve(target matching provides), deregisters itself and fan-outs
          resolve to each prerequisite in 'requires', then attempts an immediate publish().
        - reply(): On resolve(target), adds target to known_targets, replays all cached artifacts
          for that target via context.submit(... built ...), then attempts publish().
        - listen(): On built(target in requires), appends the artifact to input_store[target] and
          attempts publish().

    Publishing:
        - For each Cartesian product of accumulated require artifacts, compute a combo key using
          DeepHash(tuple(artifacts)). For each known target, call build(context, target, *combo)
          only if the key is not present in the per-target cache. Each yielded artifact is emitted
          as ReactiveEvent(name="built", target=target, artifact=item) and cached under that key.

    Caching:
        - When cache=False: use a per-target in-memory dict; no persistence across runs.
        - When cache=True: use AbstractProcessor.archive(workspace/target) to obtain a persistent
          klepto sql_archive keyed by the processor class' source hash. Cache survives across runs.

    Phase and termination:
        - On Event '__PHASE__', calls phase(context, target, phase) for each known target.
        - on_terminate receives '__POISON__' and can clean up resources in subclasses.

    Notes:
        - 'provides' is treated as a fullmatch regex against resolve.target.
        - 'requires' is deduplicated preserving order.
    """
    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        cache: bool = False,
        priority=0,
        **kwargs,
    ):
        super().__init__(priority=priority, **kwargs)
        self.provides: str = provides
        self.matcher = re.compile(self.provides)
        self.requires: List[str] = list(OrderedDict.fromkeys(requires))
        self.handlers = {self.new, self.reply, self.listen}
        self.known_targets = set()
        self.input_store = defaultdict(CustomSet)  # requrie -> artifacts (ingridents)
        self.persist = cache

    @functools.cache
    def _get_cache(self, target: str, workspace: Path):
        match self.persist:
            case True:
                assert workspace is not None
                return self.archive(workspace.joinpath(target))
            case False:
                assert workspace is None
                return dict()

    def get_cache(self, target, context):
        match self.persist:
            case True:
                return self._get_cache(target, context.pipeline.workspace)
            case False:
                return self._get_cache(target, None)

    def _process(self, context: Context, event: ReactiveEvent):
        for handler in list(self.handlers):
            handler(context, event)

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent() as e:
                self._process(context, e)
            case Event(name="__POISON__") as e:
                self.on_terminate(context, e)
            case Event(name="__PHASE__", phase=phase) as e:
                for target in self.known_targets:
                    self.phase(context, target, phase)

    def publish(self, context):
        """Attempt to build and emit artifacts for all known targets.

        For each product of self.input_store[require] across requires, compute a DeepHash key.
        For each known target, if the key is absent from the per-target cache, call build(...)
        to obtain an iterable of artifacts, emit each as a built event, and store the collected
        artifacts in the cache keyed by the combo key. No-op if no known targets yet.
        """
        # immediately tries to trigger builds
        for key in itertools.product(
            *[self.input_store[require] for require in self.requires]
        ):
            skey = deepdiff.DeepHash(key)[key]
            for target in self.known_targets:
                if skey not in self.get_cache(target, context):
                    artifacts = self.build(context, target, *key)
                    assert isinstance(artifacts, Iterable)
                    collected = list()
                    for artifact in artifacts:
                        context.submit(
                            ReactiveEvent(
                                name="built", target=target, artifact=artifact
                            )
                        )
                        collected.append(artifact)
                    self.get_cache(target, context)[skey] = collected

    def new(self, context: Context, event: ReactiveEvent):
        """Handle initial resolve(target) for provides.

        Behavior:
            - If event is resolve for a matching target: remove this new handler from handlers,
              submit resolve for each require to kick downstream producers, and attempt publish().
        """
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.fullmatch(
                target
            ):
                self.handlers -= {self.new}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))
                # for known_provide in self.known_targets:
                self.publish(
                    context,
                )
        pass

    def reply(self, context: Context, event: ReactiveEvent):
        """Handle resolve(target) by replying with cached artifacts and attempting new publishes.

        - Adds the target to known_targets.
        - Replays all cached artifacts for the target via context.submit(built,...).
        - Calls publish() to produce any newly available combos.
        """
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.fullmatch(
                target
            ):
                self.known_targets.add(target)
                # someone is asking for "target"
                # we should tell them about all the things we know about target
                # by publishing all the values we have built for target
                # the values we publish shall be picked up by all who are interested
                # the initial blanket "resolve-broadcast" hopefully should be brief
                for artifacts in self.get_cache(target, context).values():
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
                self.publish(
                    context,
                )

    def listen(self, context: Context, event: ReactiveEvent):
        """Handle built(require, artifact) by recording the input and attempting publish()."""
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].add(artifact)
                self.publish(
                    context,
                )

    @abc.abstractmethod
    def build(self, context: Context, target: str, *args, **kwargs):
        pass

    def phase(self, context: Context, target: str, phase: int):
        pass

    def on_terminate(self, context: Context, event: Event):
        pass


def reactive(
    provides: str,
    requires: List[str],
    cache: bool = False,
):
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
        )
        return cls

    return decorator


class Collector(ReactiveBuilder):
    """Accumulate built() arguments from multiple topics and emit snapshots on phases.

    - Requires = topics; Provides = forwards_to.
    - build() records each incoming args tuple and yields nothing.
    - On each __PHASE__, if the accumulated list changed since last emission, emit a
      built(forwards_to, artifact=copy_of_values) and update last. Initial last=None
      ensures at least one emission once values arrive.
    """
    def __init__(self, forwards_to, topics, limit=None):
        super().__init__(forwards_to, topics, cache=False)
        self.values = list()
        self.last = None  # setting last to None gurantees that the collector shall publish at least once during the initial phasing
        self.limit = float('inf') if limit is None else limit 

    def build(self, context, target, *args, **kwargs):
        """Record the received args tuple; do not emit immediately. Emission happens in phase()."""
        if len(self.values) < self.limit:
            self.values.append(args)
        yield from []

    def phase(self, context, target, phase):
        """On each phase, emit a snapshot if values changed since last emission."""
        if self.values != self.last:
            context.submit(
                ReactiveEvent(name="built", target=target, artifact=self.values.copy())
            )
            self.last = list(self.values)


class StreamGrouper(ReactiveBuilder):
    """Group consecutive stream items by key, emitting the previous group on key change.

    - keyfunc: computes the grouping key from the args.
    - Maintains (lastkey, lastset). When a new key arrives:
        - If same as lastkey: append args to lastset.
        - If different: yield (lastkey, lastset) then reset to new key with empty set.
    - Initial emission yields (None, []) before the first key.
    - No final flush is performed at end of stream.
    """
    def __init__(
        self,
        provides,
        requires,
        keyfunc: Callable[..., Any],
    ):
        super().__init__(provides, requires, cache=False)
        self.lastkey = None
        self.lastset = list()
        self.keyfunc = keyfunc

    def build(
        self,
        context,
        target,
        *args,
    ):
        thiskey = self.keyfunc(*args)
        if thiskey == self.lastkey:
            self.lastset.append(args)
        else:
            yield self.lastkey, self.lastset
            self.lastkey = thiskey
            self.lastset = list()


class JsonLSink(ReactiveBuilder):
    """JSONL sink that writes BaseModel artifacts for a topic to a file, one per line.

    - Requires=[name] and Provides=name.
    - build() asserts artifact is a BaseModel, writes model_dump_json() + newline, flushes.
    - on_terminate() closes the file; class runs at low priority so it executes after producers.
    """
    def __init__(self, name: str, filepath: str, compression='infer_from_extension'):
        super().__init__(name, [name], cache=False, priority=-100)
        self.filepath = filepath
        self.file = open(self.filepath, "wt", compression=compression)

    def build(self, context, target, item):
        assert isinstance(item, BaseModel)
        self.file.writelines((item.model_dump_json(), "\n"))
        self.file.flush()
        yield from []

    def on_terminate(self, context, event):
        """Close the underlying file on pipeline termination (__POISON__)."""
        self.file.close()


class JsonLSource(ReactiveBuilder):
    def __init__(self, name: str, filepath: str, itemtype, compression='infer_from_extension'):
        super().__init__(name, [], cache=False, priority=100)
        self.filepath = filepath
        assert issubclass(itemtype, BaseModel)
        self.itemtype = itemtype
        self.file = open(self.filepath, "rt", compression=compression)

    def build(self, context, target, ):
        for line in self.file:
            yield self.itemtype.model_validate_json(line)
        

    def on_terminate(self, context, event):
        self.file.close()
