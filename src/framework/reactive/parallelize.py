"""Load-balancing / parallelization primitives for reactive builders."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any, Collection, List, Optional, Tuple, Type

from ..core import Context, timeit
from .builder import ReactiveBuilder, ReactiveEvent

logger = logging.getLogger(__name__)


class LoadBalancerSequencer(ReactiveBuilder):
    """Tags each input with a monotonically increasing sequence number."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        persist: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=persist, **kwargs)
        self.next_seq = 0

    def on_init(self, context: Context):
        if self.persist:
            self.next_seq = _next_persisted_sequence(self.get_cache(context))

    def build(self, context: Context, *args):
        seq = self.next_seq
        self.next_seq += 1
        yield seq, args


class LoadBalancerRouter(ReactiveBuilder):
    """Routes items to a specific worker based on ``seq % num_workers == worker_id``."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        worker_id: int,
        num_workers: int,
        persist: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=persist, **kwargs)
        self.worker_id = worker_id
        self.num_workers = num_workers

    def build(self, context: Context, item: Tuple[int, Tuple[Any, ...]]):
        seq, args = item
        if seq % self.num_workers == self.worker_id:
            yield item


class ParallelResultEmitter(ReactiveBuilder):
    """Forward worker batches as soon as they're produced.

    There is no FIFO reassembly: artifacts arrive at downstream stages in
    whichever order the workers complete. Sequence numbers are still
    used internally for routing, dedupe, and persistence — they are not
    exposed as a downstream ordering contract.
    """

    def publish(self, context: Context):
        _publish_by_sequence(self, context, mode="emitter")

    def build(self, context: Context, item: Tuple[int, List[Any]]):
        _seq, results = item
        yield from results


def parallelize(
    provides: str,
    requires: Collection[str],
    worker_builder: Type[ReactiveBuilder],
    num_workers: int,
    worker_init_args: Tuple[Any, ...] = (),
    **kwargs,
) -> Tuple[ReactiveBuilder, ...]:
    """Create a reactive parallelization graph for a builder class.

    Fans out build tasks across ``num_workers`` real ``ReactiveBuilder``
    instances. Worker outputs are forwarded immediately via
    :class:`ParallelResultEmitter` — there is no cross-worker ordering
    guarantee. Downstream stages MUST be order-independent.

    Worker contract (enforced at instantiation):
      * ``worker_builder.__init__`` must accept ``**kwargs`` and forward
        ``provides``, ``requires``, ``persist``, ``name``, ``priority`` to
        :class:`ReactiveBuilder`; or inherit ``ReactiveBuilder.__init__`` unchanged.
      * Workers whose ``__init__`` derives state from wiring fields (e.g.
        ``len(self.requires)``) will see the *final* wiring directly — no
        post-construction rewiring — so derived state is correct immediately.

    Reserved kwargs: ``persist``, ``name``, ``priority``. Anything else is
    forwarded to the worker constructor.
    """
    if num_workers < 1:
        raise ValueError("num_workers must be >= 1")

    starred = [r for r in requires if r.startswith("*")]
    if starred:
        raise ValueError(
            "parallelize(...) does not support *-expanded requires: "
            f"{starred}. Remove the leading '*' and adapt the worker's "
            "build() signature to accept the raw tuple, or file an issue "
            "if you need expansion propagated through the sequencing stage."
        )

    persist = bool(kwargs.pop("persist", False))
    name = kwargs.pop("name", None)
    priority = kwargs.pop("priority", 0)

    requires_list = list(requires)
    requires_tag = "+".join(requires_list)
    topology_tag = f"workers={num_workers}"
    base = (
        f"{name}::{topology_tag}"
        if name is not None
        else f"parallelize::{provides}::{worker_builder.__name__}::{requires_tag}::{topology_tag}"
    )

    sequenced = f"{base}::sequenced"
    results = f"{base}::results"

    components: List[ReactiveBuilder] = [
        LoadBalancerSequencer(
            provides=sequenced,
            requires=requires_list,
            persist=persist,
            name=f"{base}::sequencer",
            priority=priority,
        ),
    ]

    for worker_id in range(num_workers):
        routed = f"{base}::routed::{worker_id:02d}"
        components.append(
            LoadBalancerRouter(
                provides=routed,
                requires=[sequenced],
                worker_id=worker_id,
                num_workers=num_workers,
                persist=persist,
                name=f"{base}::router_{worker_id}",
                priority=priority,
            )
        )
        components.append(
            _instantiate_parallel_worker(
                worker_class=worker_builder,
                worker_name=f"{base}::_worker_{worker_id}",
                task_topic=routed,
                results_topic=results,
                persist=persist,
                priority=priority,
                worker_init_args=worker_init_args,
                worker_init_kwargs=kwargs,
            )
        )

    components.append(
        ParallelResultEmitter(
            provides=provides,
            requires=[results],
            persist=persist,
            name=base,
            priority=priority,
        )
    )

    return tuple(components)


def _publish_by_sequence(
    stage: ReactiveBuilder,
    context: Context,
    *,
    mode: str,
):
    """Publish input-store items keyed by their ``seq``.

    Shared implementation for two sequence-tagged publish paths:

    * ``mode="worker"`` — routed task is ``(seq, args)``; ``build`` receives
      ``*args`` and emits a single ``(seq, collected)`` payload. Skip gate:
      ``self.dedupe``. Archive value: ``[payload]``.
    * ``mode="emitter"`` — incoming ``(seq, payload)`` is passed through
      ``build`` which yields the worker's individual artifacts; each is
      submitted immediately to ``stage.provides``. Every processed seq
      is archived (even when the worker emitted nothing) so dedupe stays
      consistent across replays.
    """
    items = stage.input_store[stage.requires[0]]
    archive = stage.get_cache(context)

    while items:
        seq = items[0][0]
        already_cached = seq in archive
        should_skip = (mode == "worker" and stage.dedupe and already_cached) or (
            mode != "worker" and already_cached
        )
        if not should_skip:
            with timeit(stage.build.__qualname__, logger=logger):
                if mode == "worker":
                    artifacts = stage.build(context, *items[0][1])
                else:
                    artifacts = stage.build(context, items[0])
                assert isinstance(artifacts, Iterable)
                collected = list(artifacts)
            if mode == "worker":
                payload = (seq, collected)
                context.submit(
                    ReactiveEvent(name="built", target=stage.provides, artifact=payload)
                )
                if stage.dedupe or stage.persist:
                    archive[seq] = [payload]
            else:
                for artifact in collected:
                    context.submit(
                        ReactiveEvent(name="built", target=stage.provides, artifact=artifact)
                    )
                archive[seq] = collected
        items.popleft()


def _instantiate_parallel_worker(
    worker_class: Type[ReactiveBuilder],
    worker_name: str,
    task_topic: str,
    results_topic: str,
    persist: bool,
    priority: int,
    worker_init_args: Tuple[Any, ...],
    worker_init_kwargs: dict[str, Any],
) -> ReactiveBuilder:
    """Instantiate a worker under the ``parallelize`` contract."""
    wiring_kwargs = dict(
        worker_init_kwargs,
        provides=results_topic,
        requires=[task_topic],
        persist=persist,
        name=worker_name,
        priority=priority,
    )
    try:
        worker = worker_class(*worker_init_args, **wiring_kwargs)
    except TypeError as exc:
        raise TypeError(
            f"{worker_class.__name__} does not accept parallelize wiring kwargs. "
            "Worker __init__ must accept **kwargs and forward "
            "provides/requires/persist/name/priority to ReactiveBuilder.__init__, "
            "or inherit ReactiveBuilder.__init__ unchanged. "
            f"Underlying error: {exc}"
        ) from exc

    worker._publish_strategy = _parallel_worker_publish
    worker.on_parallelize_rewire()
    return worker


def _parallel_worker_publish(worker: ReactiveBuilder, context: Context) -> None:
    """Sequence-tagged publish strategy installed on parallelized workers."""
    _publish_by_sequence(worker, context, mode="worker")


def _next_persisted_sequence(archive) -> int:
    """Next sequence cursor from an archive keyed by integer ``seq``.

    Returns 0 when no int keys are present, rather than ``len(keys)``, so
    archives containing only non-int keys don't silently skip sequences.
    """
    return max((key for key in archive.keys() if isinstance(key, int)), default=-1) + 1
