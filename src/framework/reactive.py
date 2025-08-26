from __future__ import annotations

import functools
import logging
from pathlib import Path
import abc
from collections import OrderedDict, defaultdict, deque
from typing import Any, Collection, List
from collections.abc import Callable, Iterable

import deepdiff

from .core import AbstractProcessor, Context, Event

logger = logging.getLogger(__name__)


class ReactiveEvent(Event):
    pass


class ReactiveBuilder(AbstractProcessor):
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
        self.handlers = {self.new, self.listen}
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
                self.on_terminate()
            case Event(name="__PHASE__", phase=phase) as e:
                for artifact in self.phase(context, phase):
                    context.submit(
                        ReactiveEvent(
                            name="built", target=self.provides, artifact=artifact
                        )
                    )

    def publish(self, context: Context, input_target: str, new_artifact: Any):
        iter = [tuple()]
        if len(self.requires) > 0:
            iter = list(zip(*[self.input_store[require] for require in self.requires]))

        for key in iter:
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
        match event:
            case ReactiveEvent(name="resolve", target=target) if (
                self.provides == target
            ):
                self.handlers -= {self.new}
                self.reply(context, event)
                self.publish(context, None, None)
                self.handlers |= {self.reply}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))

        pass

    def reply(self, context: Context, event: ReactiveEvent):
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

    def on_terminate(self):
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
                ReactiveEvent(
                    name="built", target=self.provides, artifact=list(self.values)
                )
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
            context.submit(
                ReactiveEvent(
                    name="built",
                    target=self.provides,
                    artifact=(self.lastkey, self.lastset),
                )
            )
        self.lastkey = None
        self.lastset = list()

