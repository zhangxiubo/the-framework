from __future__ import annotations

from contextlib import contextmanager
import functools
import itertools
import logging
import re
import abc
from collections import OrderedDict, defaultdict
from typing import Any, Collection, Dict, Iterable, List, Sequence, Set, Optional

import dill
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
        build_policy: BuildPolicy = None,
    ):
        super().__init__()
        self.provides: str = provides
        self.matcher = re.compile(self.provides)
        self.requires: List[str] = list(OrderedDict.fromkeys(requires))
        self.handlers = {self.new, self.reply, self.listen}
        self.known_targets = set()
        self.build_cache = defaultdict(lambda: defaultdict(lambda: list()))
        self.input_store = defaultdict(list)  # requrie -> artifacts (ingridents)

    def _process(self, context: Context, event: ReactiveEvent):
            for handler in list(self.handlers):
                handler(context, event)
            for known_provide in self.known_targets:
                self.publish(context, known_provide)

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve"):
                self._process(context, event)
            case ReactiveEvent(name="built"):
                self._process(context, event)



    def publish(self, context, target):
        # immediately tries to trigger builds
        for key in itertools.product( *[self.input_store[require] for require in self.requires]):
            if key not in self.build_cache[target]:
                for artifact in self.build(context, target, *key):
                    self.build_cache[target][key].append(artifact)
                    context.submit(ReactiveEvent(name="built", target=target, artifact=artifact))


    def new(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.match(
                target
            ):
                print('resolving', target)
                self.handlers -= {self.new}
                for require in self.requires:
                    # kicks-off downstream tasks
                    context.submit(ReactiveEvent(name="resolve", target=require))
        pass

    def reply(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.match(
                target
            ):
                self.known_targets.add(target)
                # someone is asking for "target"
                # we should tell them about all the things we know about target
                # by publishing all the values we have built for target
                # the values we publish shall be picked up by all who are interested
                # the initial blanket "resolve-broadcast" hopefully should be brief
                for artifact in self.build_cache[target].values():
                    # retrieve the things we have built for target
                    # build_cache will be a dictionary indexed by target and whose value is another ordered dictionary that maps key -> artifact
                    context.submit(ReactiveEvent(name="built", target=target, ))
                    pass

    def listen(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if target in self.requires:
                self.input_store[target].append(artifact)

                    


    @abc.abstractmethod
    def build(self, context: Context, target: str, *args, **kwargs):
        pass


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
