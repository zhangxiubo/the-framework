"""Load-balancing / parallelization primitives for reactive builders."""

from __future__ import annotations

import abc
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


class SequenceGapPolicy(abc.ABC):
    """Strategy for handling missing sequences in FIFO collectors."""

    @abc.abstractmethod
    def resolve_next_expected(
        self,
        buffer: dict[int, List[Any]],
        next_expected: int,
    ) -> int:
        """Return the cursor to use before draining ready buffered sequences."""
        pass


class WaitForSequenceGaps(SequenceGapPolicy):
    """Default gap policy: wait indefinitely for missing sequence numbers."""

    def resolve_next_expected(
        self,
        buffer: dict[int, List[Any]],
        next_expected: int,
    ) -> int:
        return next_expected


class SkipAheadOnBacklog(SequenceGapPolicy):
    """Skip missing sequence gaps when buffered backlog reaches ``max_buffered``."""

    def __init__(self, max_buffered: int):
        if max_buffered < 1:
            raise ValueError("max_buffered must be >= 1")
        self.max_buffered = max_buffered

    def resolve_next_expected(
        self,
        buffer: dict[int, List[Any]],
        next_expected: int,
    ) -> int:
        if next_expected in buffer or len(buffer) < self.max_buffered:
            return next_expected
        return min(buffer, default=next_expected)


class LoadBalancerCollector(ReactiveBuilder):
    """Reorders worker results back into FIFO sequence order."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        gap_policy: Optional[SequenceGapPolicy] = None,
        persist: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=persist, **kwargs)
        self.buffer: dict[int, List[Any]] = {}
        self.next_expected = 0
        self.gap_policy = WaitForSequenceGaps() if gap_policy is None else gap_policy
        self.skipped_sequences = 0

    def on_init(self, context: Context):
        if self.persist:
            self.next_expected = _next_persisted_sequence(self.get_cache(context))

    def publish(self, context: Context):
        _publish_by_sequence(self, context, mode="collector")

    def build(self, context: Context, item: Tuple[int, List[Any]]):
        seq, results = item
        self.buffer[seq] = list(results)
        resolved = self.gap_policy.resolve_next_expected(self.buffer, self.next_expected)
        if resolved < self.next_expected:
            raise ValueError(
                "gap policy returned a next_expected smaller than current cursor"
            )
        if resolved > self.next_expected:
            self.skipped_sequences += resolved - self.next_expected
            logger.warning(
                "LoadBalancerCollector skipped %d sequence(s) due to gap policy %s",
                resolved - self.next_expected,
                self.gap_policy.__class__.__name__,
            )
        self.next_expected = resolved

        while self.next_expected in self.buffer:
            ready = self.buffer.pop(self.next_expected)
            self.next_expected += 1
            for artifact in ready:
                yield artifact

    def get_pending_count(self) -> int:
        return len(self.buffer)

    def get_next_expected_seq(self) -> int:
        return self.next_expected


class ParallelResultEmitter(ReactiveBuilder):
    """Forward worker batches immediately without restoring FIFO order."""

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
    preserve_fifo: bool = True,
    worker_init_args: Tuple[Any, ...] = (),
    **kwargs,
) -> Tuple[ReactiveBuilder, ...]:
    """Create a reactive parallelization graph for a builder class.

    Fans out build tasks across ``num_workers`` real ``ReactiveBuilder`` instances.
    The sequencing stage preserves single-instance cache-key semantics;
    ``preserve_fifo=True`` reassembles worker outputs back into input order.

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
    topology_tag = f"workers={num_workers}::fifo={int(preserve_fifo)}"
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

    if preserve_fifo:
        components.append(
            LoadBalancerCollector(
                provides=provides,
                requires=[results],
                persist=persist,
                name=base,
                priority=priority,
            )
        )
    else:
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

    Shared implementation for three sequence-tagged publish paths:

    * ``mode="worker"`` — routed task is ``(seq, args)``; ``build`` receives
      ``*args`` and emits a single ``(seq, collected)`` payload. Skip gate:
      ``self.dedupe``. Archive value: ``[payload]``.
    * ``mode="collector"`` — incoming ``(seq, payload)`` is passed through
      ``build`` which yields individual artifacts that are submitted one by
      one. Buffered-only sequences are persisted regardless of emptiness.
    * ``mode="emitter"`` — like ``collector`` but only archive sequences that
      produced at least one artifact.
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
                # emitter archives every processed seq; collector only
                # persists sequences that actually produced downstream
                # artifacts (buffer-only seqs stay unpersisted).
                if collected or mode == "emitter":
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
