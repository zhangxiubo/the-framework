from __future__ import annotations

import functools
import itertools
import logging
import re
import abc
from collections import OrderedDict, defaultdict
from typing import Collection, List
from collections.abc import Iterable

import deepdiff
from pydantic import BaseModel, ConfigDict

from .core import AbstractProcessor, Context

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
    ):
        super().__init__()
        self.provides: str = provides
        self.matcher = re.compile(self.provides)
        self.requires: List[str] = list(OrderedDict.fromkeys(requires))
        self.handlers = {self.new, self.reply, self.listen}
        self.known_targets = set()
        self.input_store = defaultdict(list)  # requrie -> artifacts (ingridents)
        self.persist = cache

    @functools.cache
    def _get_cache(self, target, workspace):
        match self.persist:
            case True:
                assert workspace is not None
                return self.archive(workspace)
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
        for known_provide in self.known_targets:
            self.publish(context, known_provide)

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent():
                self._process(context, event)

    def publish(self, context, target):
        # immediately tries to trigger builds
        for key in itertools.product(
            *[self.input_store[require] for require in self.requires]
        ):
            skey = deepdiff.DeepHash(key)[key]
            if skey not in self.get_cache(target, context):
                artifacts = self.build(context, target, *key)
                assert isinstance(artifacts, Iterable)
                collected = list(artifacts)
                for artifact in collected:
                    context.submit(
                        ReactiveEvent(name="built", target=target, artifact=artifact)
                    )
                if skey not in self.get_cache(target, context):
                    self.get_cache(target, context)[skey] = list()
                self.get_cache(target, context)[skey] += collected

    def new(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.fullmatch(
                target
            ):
                self.handlers -= {self.new}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))
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
                    for artifact in set(artifacts):
                        context.submit(
                            ReactiveEvent(
                                name="built",
                                target=target,
                                artifact=artifact,
                            )
                        )
                    pass

    def listen(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)

    @abc.abstractmethod
    def build(self, context: Context, target: str, *args, **kwargs):
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
