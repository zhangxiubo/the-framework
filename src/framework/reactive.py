from __future__ import annotations

import functools
import itertools
import logging
from pathlib import Path
import re
import abc
from collections import OrderedDict, defaultdict
from typing import Any, Collection, Dict, List
from collections.abc import Callable, Iterable

import deepdiff
from pydantic import BaseModel, ConfigDict

from .core import AbstractProcessor, Context, Event

logger = logging.getLogger(__name__)


class ReactiveEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


# --------------------------
# Processor
# --------------------------


class ReactiveBuilder(AbstractProcessor):
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
        self.input_store = defaultdict(list)  # requrie -> artifacts (ingridents)
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
        # immediately tries to trigger builds
        for key in itertools.product(
            *[self.input_store[require] for require in self.requires]
        ):
            skey = deepdiff.DeepHash(key)[key]
            for target in self.known_targets:
                if skey not in self.get_cache(target, context):
                    artifacts = self.build(context, target, *key)
                    assert isinstance(artifacts, Iterable)
                    collected = list(artifacts)
                    for artifact in collected:
                        context.submit(
                            ReactiveEvent(
                                name="built", target=target, artifact=artifact
                            )
                        )
                    self.get_cache(target, context)[skey] = collected

    def new(self, context: Context, event: ReactiveEvent):
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
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)
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
    def __init__(self, forwards_to, topics):
        super().__init__(forwards_to, topics, cache=False)
        self.values = list()
        self.last = None  # setting last to None gurantees that the collector shall publish at least once during the initial phasing

    def build(self, context, target, *args, **kwargs):
        self.values.append(args)
        yield from []

    def phase(self, context, target, phase):
        if self.values != self.last:
            context.submit(
                ReactiveEvent(name="built", target=target, artifact=self.values.copy())
            )
            self.last = list(self.values)


class StreamGrouper(ReactiveBuilder):
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
    def __init__(self, name: str, filepath: str):
        super().__init__(name, [name], cache=False, priority=-100)
        self.filepath = filepath
        self.file = open(self.filepath, "wt")

    def build(self, context, target, item):
        assert isinstance(item, BaseModel)
        self.file.writelines((item.model_dump_json(), "\n"))
        self.file.flush()
        yield from []

    def on_terminate(self, context, event):
        self.file.close()
