from __future__ import annotations

import functools
import itertools
import logging
import re
import abc
from types import MethodType
from collections import OrderedDict, defaultdict
from typing import Collection, List, Literal

from pydantic import BaseModel, ConfigDict
import dill
import hashlib
from .core import AbstractProcessor, Context, caching

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
        # The build cache is a flat key-value store.
        # Key: "target|skey" where skey is the hash of the input artifact tuple.
        # Value: list of artifacts produced by build().
        self.build_cache = defaultdict(list)
        self.input_store = defaultdict(list)  # require -> artifacts (ingridents)
        self.use_persistent_cache = cache

    def _process(self, context: Context, event: ReactiveEvent):
        if self.use_persistent_cache and isinstance(self.build_cache, defaultdict):
            # First-time processing for this instance with caching enabled.
            # We switch from the in-memory defaultdict to a persistent klepto archive.
            persistent_cache = self.archive(context.pipeline.workspace)
            persistent_cache.update(self.build_cache)  # Copy any initial state.
            self.build_cache = persistent_cache

        for handler in list(self.handlers):
            handler(context, event)
        for known_provide in self.known_targets:
            self.publish(context, known_provide)

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent():
                self._process(context, event)

    def publish(self, context, target):
        # Immediately tries to trigger builds for all combinations of inputs.
        for key in itertools.product(
            *[self.input_store[require] for require in self.requires]
        ):
            skey = hashlib.sha256(dill.dumps(key)).hexdigest()
            cache_key = f"{target}|{skey}"
            if cache_key not in self.build_cache:
                # Cache miss: run the build function and store the resulting artifacts.
                # Even if the build produces nothing, we mark it as cached to avoid re-running.
                self.build_cache[cache_key] = list(self.build(context, target, *key))

            # Always publish the artifacts that are now in the cache.
            for artifact in self.build_cache[cache_key]:
                context.submit(
                    ReactiveEvent(name="built", target=target, artifact=artifact)
                )

    def new(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.match(
                target
            ):
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
                # A new listener has appeared. We should inform them of all the artifacts
                # we have already built for the target they are interested in.
                for cache_key, artifact_list in self.build_cache.items():
                    # Key is a string like "target|skey", so we check if it starts with the target.
                    if cache_key.startswith(f"{target}|"):
                        for artifact in artifact_list:
                            context.submit(
                                ReactiveEvent(
                                    name="built", target=target, artifact=artifact
                                )
                            )

    def listen(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target in self.requires
            ):
                self.input_store[target].append(artifact)

    @abc.abstractmethod
    def build(self, context: Context, target: str, *args, **kwargs):
        pass


def reactive(provides: str, requires: List[str], cache: bool = False, **kwargs):
    """Class decorator to bind provides/requires for ReactiveBuilder subclasses.

    Args:
        provides: The provided topic name or regex pattern for the reactive builder.
        requires: A list of prerequisite topic names (duplicates are deduplicated preserving order).
        cache: When True, the internal cache of built artifacts is made persistent across runs,
               using the workspace provided to the Pipeline. This avoids re-running the `build`
               method for the same combination of inputs. When False (default), the cache is
               in-memory only for the lifetime of the processor instance.
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
