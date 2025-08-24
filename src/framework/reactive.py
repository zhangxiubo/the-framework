from __future__ import annotations

import functools
import itertools
import logging
from pathlib import Path
import re
import abc
from collections import OrderedDict, defaultdict, ChainMap, deque
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
        persist: bool = False,
        priority=0,
        **kwargs,
    ):
        super().__init__(priority=priority, **kwargs)
        self.provides: str = provides
        self.requires: List[str] = list(OrderedDict.fromkeys(requires))
        self.handlers = {self.new,  self.listen}
        self.input_store = defaultdict(deque)  # requrie -> artifacts (ingridents)
        self.persist = persist

    @functools.cache
    def _get_cache(self, workspace: Path):
        match self.persist:
            case True:
                assert workspace is not None
                return self.archive(workspace)
            case False:
                assert workspace is None
                return dict()

    def get_cache(self, context):
        match self.persist:
            case True:
                return self._get_cache(context.pipeline.workspace)
            case False:
                return self._get_cache(None)

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
                self.phase(context, phase)

    def publish(self, context: Context, input_target: str, new_artifact: Any):
        """Attempt to build and emit artifacts for all known targets.

        For each product of self.input_store[require] across requires, compute a DeepHash key.
        For each known target, if the key is absent from the per-target cache, call build(...)
        to obtain an iterable of artifacts, emit each as a built event, and store the collected
        artifacts in the cache keyed by the combo key. No-op if no known targets yet.
        """
        # immediately tries to trigger builds
        # input_store = ChainMap({input_target: [new_artifact]}, self.input_store)
        # print(list(zip(*[self.input_store[require][:1] for require in self.requires])))

        for key in list(zip(
            *[self.input_store[require] for require in self.requires]
        )):
            skey = deepdiff.DeepHash(key)[key]
            if skey not in self.get_cache(context):
                artifacts = self.build(context, *key)
                assert isinstance(artifacts, Iterable)
                collected = list()
                for artifact in artifacts:
                    context.submit(
                        ReactiveEvent(
                            name="built", target=self.provides, artifact=artifact
                        )
                    )
                    collected.append(artifact)
                self.get_cache(context)[skey] = collected
            for require in self.requires:
                self.input_store[require].popleft()
            

            
            

    def new(self, context: Context, event: ReactiveEvent):
        """Handle initial resolve(target) for provides.

        Behavior:
            - If event is resolve for a matching target: remove this new handler from handlers,
              submit resolve for each require to kick downstream producers, and attempt publish().
        """
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.provides == target:
                self.handlers -= {self.new}
                self.reply(context, event)           
                self.publish(
                    context,
                    None,
                    None
                )
                self.handlers |= {self.reply}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))

        pass

    def reply(self, context: Context, event: ReactiveEvent):
        """Handle resolve(target) by replying with cached artifacts and attempting new publishes.

        - Adds the target to known_targets.
        - Replays all cached artifacts for the target via context.submit(built,...).
        - Calls publish() to produce any newly available combos.
        """
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.provides == target:
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
        """Handle built(require, artifact) by recording the input and attempting publish()."""
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)
                self.publish(context, target, artifact)

    @abc.abstractmethod
    def build(self, context: Context, *args, **kwargs):
        pass

    def phase(self, context: Context, phase: int):
        pass

    def on_terminate(self, context: Context, event: Event):
        pass


def reactive(
    provides: str,
    requires: List[str],
    persist: bool = False,
):

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

    def __init__(self, forwards_to, topics, limit=None):
        super().__init__(forwards_to, topics, persist=False)
        self.values = list()
        self.last = None  # setting last to None gurantees that the collector shall publish at least once during the initial phasing
        self.limit = float("inf") if limit is None else limit

    def build(self, context, *args, **kwargs):
        if len(self.values) < self.limit:
            self.values.append(args)
        yield from []

    def phase(self, context, phase):
        if self.values != self.last:
            context.submit(
                ReactiveEvent(name="built", target=self.provides, artifact=self.values)
            )
            self.last = list(self.values)


class StreamGrouper(ReactiveBuilder):

    def __init__(
        self,
        provides,
        requires,
        keyfunc: Callable[..., Any],
    ):
        super().__init__(provides, requires, persist=False)
        self.lastkey = None
        self.lastset = list()
        self.keyfunc = keyfunc

    def build(
        self,
        context,
        *args,
    ):
        thiskey = self.keyfunc(*args)
        if thiskey == self.lastkey:
            self.lastset.append(args)
        else:
            yield self.lastkey, self.lastset
            self.lastkey = thiskey
            self.lastset = list()
            self.lastset.append(args)

    def phase(self, context, phase):
        if self.lastkey is not None:
            yield self.lastkey, self.lastset
        self.lastkey = None
        self.lastset = list()


class JsonLSink(ReactiveBuilder):
    """JSONL sink that writes BaseModel artifacts for a topic to a file, one per line.

    - Requires=[name] and Provides=name.
    - build() asserts artifact is a BaseModel, writes model_dump_json() + newline, flushes.
    - on_terminate() closes the file; class runs at low priority so it executes after producers.
    """

    def __init__(self, name: str, filepath: str, compression="infer_from_extension"):
        super().__init__(name, [name], persist=False, priority=-100)
        self.filepath = filepath
        self.file = open(self.filepath, "wt", compression=compression)

    def build(self, context, item):
        assert isinstance(item, BaseModel)
        self.file.writelines((item.model_dump_json(), "\n"))
        self.file.flush()
        yield from []

    def on_terminate(self, context, event):
        """Close the underlying file on pipeline termination (__POISON__)."""
        self.file.close()


class JsonLSource(ReactiveBuilder):
    def __init__(
        self,
        name: str,
        filepath: str,
        itemtype,
        limit=None,
        compression="infer_from_extension",
    ):
        super().__init__(name, [], persist=False, priority=100)
        self.filepath = filepath
        assert issubclass(itemtype, BaseModel)
        self.itemtype = itemtype
        self.limit = float("inf") if limit is None else limit
        self.file = open(self.filepath, "rt", compression=compression)

    def build(
        self,
        context,
    ):
        for i, line in enumerate(self.file):
            if i < self.limit:
                yield self.itemtype.model_validate_json(line)

    def on_terminate(self, context, event):
        self.file.close()
