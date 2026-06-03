"""Core reactive builder primitive + lifecycle events."""

from __future__ import annotations

import abc
import functools
import logging
import re
from collections import OrderedDict, defaultdict, deque
from collections.abc import Callable, Iterable
from typing import Collection, List, Optional

from ..core import AbstractProcessor, Context, Event, InMemCache, timeit

logger = logging.getLogger(__name__)


class ReactiveEvent(Event):
    """Event subtype used by the reactive layer for ``resolve``/``built`` flows."""

    pass


class ReactiveBuilder(AbstractProcessor):
    """Base class for dependency-aware processors that react to ``ReactiveEvent``s.

    ``requires`` defines upstream targets. Prefix a requirement with ``*`` to expand
    tuple artifacts into individual arguments before passing them to ``build``.

    Two orthogonal behaviors govern the archive:

    * ``persist=True`` — use the pipeline's persistent archive (RocksDB when a
      workspace is configured). Future ``resolve`` requests are served by the
      replay-only ``reply`` handler.
    * ``dedupe=True`` (default) — skip ``build`` for an input digest already in
      the archive. Independently of ``persist``; set ``dedupe=False`` to run
      ``build`` every time regardless of archive state.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        persist: bool = False,
        dedupe: Optional[bool] = None,
        name: Optional[str] = None,
        priority: int = 0,
        **kwargs,
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
            req_name = re.sub(r"^\*", "", r)
            self.require_specs.append((req_name, expand))
            normalized.setdefault(req_name, None)
        self.requires: List[str] = list(normalized.keys())
        self.require_index = {req_name: idx for idx, req_name in enumerate(self.requires)}
        self.handlers = {self.new, self.listen}
        self.input_store = defaultdict(deque)  # require -> artifacts (ingredients)
        self.persist = persist
        self.dedupe = True if dedupe is None else bool(dedupe)

    def process_reactive_event(self, context: Context, event: ReactiveEvent):
        """Dispatch to the currently-installed handler set."""
        for handler in list(self.handlers):
            handler(context, event)

    def process(self, context: Context, event: ReactiveEvent):
        """Handle lifecycle events plus the reactive ``resolve``/``built`` traffic."""
        match event:
            case ReactiveEvent() as e:
                self.process_reactive_event(context, e)
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

    #: Publish strategy override. Parallelize sets this on worker instances
    #: to a callable ``(self, context) -> None`` that implements the
    #: sequence-tagged worker path. None means "use the default join path".
    _publish_strategy: Optional[Callable[["ReactiveBuilder", Context], None]] = None

    def publish(self, context: Context):
        """Build artifacts once every requirement has at least one value."""
        if self._publish_strategy is not None:
            self._publish_strategy(self, context)
            return

        keys = [tuple()]
        if len(self.requires) > 0:
            keys = list(zip(*[self.input_store[require] for require in self.requires]))

        for key in keys:
            skey = context.pipeline.digest(key)
            archive = self.get_cache(context)
            if self.dedupe and skey in archive:
                for require in self.requires:
                    self.input_store[require].popleft()
                continue
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
                        ReactiveEvent(name="built", target=self.provides, artifact=artifact)
                    )
                    collected.append(artifact)
                if self.dedupe or self.persist:
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
                for artifacts in self.get_cache(context).values():
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
        """Override should call ``super().on_terminate(context)`` so cache stores flush cleanly."""
        self.get_cache(context).flush_wal(sync=True)

    def on_parallelize_rewire(self):
        """Hook invoked after ``parallelize`` instantiates a worker.

        Override to refresh state that depends on ``self.requires``/``self.provides``
        when the worker is run inside a parallel graph.
        """
        pass

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
    dedupe: Optional[bool] = None,
):
    """Class decorator that partially applies ``ReactiveBuilder.__init__``."""

    def decorator(cls):
        assert issubclass(cls, ReactiveBuilder)
        partial_kwargs = dict(
            provides=provides,
            requires=list(requires),
            persist=persist,
        )
        if dedupe is not None:
            partial_kwargs["dedupe"] = dedupe
        cls.__init__ = functools.partialmethod(cls.__init__, **partial_kwargs)
        return cls

    return decorator
