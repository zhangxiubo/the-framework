"""Core runtime primitives for the framework event pipeline.

This module intentionally keeps all core runtime types in one place so control
flow is easy to audit:

- Event model (`Event`)
- Execution context and completion handling (`Context`)
- Per-processor mailbox (`Inbox`)
- Global scheduler/orchestrator (`Pipeline`)
- Processor base contract (`AbstractProcessor`)

Most behavior here is concurrency-sensitive; comments are intentionally verbose
for maintainability.
"""

from __future__ import annotations

import abc
import functools
import hashlib
import itertools
import logging
import threading
import time
from collections import UserDict, defaultdict
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue, SimpleQueue
from typing import Any, Callable, Optional

import dill
from rocksdict import Rdict, WriteOptions
from rocksdict.rocksdict import AccessType

# Re-export selected helpers so callers can import from framework.core directly.
from .utils import (
    InterestInferenceError,
    caching,
    class_identifier,
    default_digest,
    event_digest,
    find_match_stmt,
    get_source,
    in_jupyter_notebook,
    infer_interests,
    normalized_source_digest,
    parse_pattern,
    retry,
    timeit,
)

# Module-level logger used by all runtime components.
logger = logging.getLogger(__name__)

# Public symbols exported by `from framework.core import *`.
# This list also serves as a compact API inventory for maintainers.
__all__ = [
    "AbstractProcessor",
    "Context",
    "Event",
    "Inbox",
    "InMemCache",
    "InterestInferenceError",
    "Pipeline",
    "ProcessEntry",
    "caching",
    "class_identifier",
    "event_digest",
    "find_match_stmt",
    "get_source",
    "in_jupyter_notebook",
    "infer_interests",
    "parse_pattern",
    "retry",
    "timeit",
]


class Event:
    """Lightweight message envelope moving through the pipeline.

    `name` is used for routing/pattern matching; additional keyword arguments are
    attached dynamically as attributes.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        # Semantic event label consumed by processor `match` patterns.
        self.name = name
        # Attach arbitrary payload keys directly to instance attributes.
        # #concern: Dynamic attribute injection means no schema enforcement and no
        # static field guarantees at runtime.
        self.__dict__.update(kwargs)


class InMemCache(UserDict):
    """In-memory mapping adapter used when no persistent workspace is configured.

    Implements a small subset of RocksDB-like methods so callers can treat both
    backends uniformly.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lock = threading.RLock()

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self.data

    def __getitem__(self, key: Any) -> Any:
        with self._lock:
            return self.data[key]

    def __setitem__(self, key: Any, item: Any) -> None:
        with self._lock:
            self.data[key] = item

    def __iter__(self):
        with self._lock:
            return iter(tuple(self.data))

    def __len__(self) -> int:
        with self._lock:
            return len(self.data)

    def get(self, key: Any, default: Any = None) -> Any:
        with self._lock:
            return self.data.get(key, default)

    def keys(self):
        with self._lock:
            return tuple(self.data.keys())

    def values(self):
        with self._lock:
            return tuple(self.data.values())

    def items(self):
        with self._lock:
            return tuple(self.data.items())

    def close(self) -> None:
        # Release in-memory entries explicitly for symmetry with persistent stores.
        with self._lock:
            self.clear()

    def flush_wal(self, *args: Any, **kwargs: Any) -> None:
        # No-op compatibility method for RocksDB API parity.
        return None

    def flush(self, *args: Any, **kwargs: Any) -> None:
        # No-op compatibility method for RocksDB API parity.
        return None

    def clear(self) -> None:
        with self._lock:
            self.data.clear()


class Context:
    """Per-task execution context passed into processor `process` calls.

    Each dispatched event gets its own `Context` instance. The context records
    emitted events for caching/replay decorators.
    """

    def __init__(
        self,
        pipeline: "Pipeline",
        *,
        enforce_lifecycle: bool = False,
    ) -> None:
        # Back-reference to owning pipeline for submissions/accounting.
        self.pipeline = pipeline
        # Ordered list of events emitted from this processing invocation.
        self.events: list[Event] = []
        # When enabled, submissions are restricted to the owning task/thread window.
        self.enforce_lifecycle = enforce_lifecycle
        # True only while processor task is actively running.
        self.active = not enforce_lifecycle
        # Thread identity allowed to call submit when lifecycle enforcement is on.
        self.owner_thread_id: Optional[int] = None
        # Lock protecting task-lifecycle fields above.
        self.lifecycle_lock = threading.Lock()

    def activate(self) -> None:
        """Mark context as active for the current worker thread."""
        if not self.enforce_lifecycle:
            return
        with self.lifecycle_lock:
            self.owner_thread_id = threading.get_ident()
            self.active = True

    def deactivate(self) -> None:
        """Mark context as inactive to reject late/unsafe submissions."""
        if not self.enforce_lifecycle:
            return
        with self.lifecycle_lock:
            self.active = False

    def ensure_submission_is_safe(self) -> None:
        """Reject late or cross-thread submissions for lifecycle-enforced contexts."""
        if not self.enforce_lifecycle:
            return
        with self.lifecycle_lock:
            active = self.active
            owner_thread_id = self.owner_thread_id
        if not active:
            raise RuntimeError("late/unsafe context.submit: processor task already completed")
        if owner_thread_id != threading.get_ident():
            raise RuntimeError("late/unsafe context.submit: cross-thread submissions are forbidden")

    def submit(self, event: Event) -> int:
        """Submit a downstream event and track it for replay/caching.

        Worker-originated submissions are internal for phase accounting
        (they cascade from pipeline-driven work); only direct external
        ``Pipeline.submit`` calls from user threads count as external.
        """
        try:
            self.ensure_submission_is_safe()
            recipients = self.pipeline.submit(event, _internal=True)
            self.events.append(event)
            return recipients
        except Exception:
            logger.exception("failed to submit event %s", event)
            raise

    def done_callback(
        self,
        future: Future,
        processor: "AbstractProcessor",
        event: Event,
        inbox: "Inbox",
        signal: Optional[threading.Event] = None,
    ) -> None:
        """Completion callback attached to each submitted processor future.

        Responsibilities:
        - Log execution failures.
        - Mark mailbox task completion.
        - Decrement global job accounting.
        - Wake dispatcher wait loop via `signal`.
        """
        try:
            exc = future.exception()
            # Suppress post-shutdown "cannot schedule new futures" noise; the
            # pipeline knows authoritatively whether its executor is gone.
            shutdown_error = (
                isinstance(exc, RuntimeError) and self.pipeline.is_executor_shut_down()
            )
            if exc is not None and not shutdown_error:
                logger.error(
                    "processor failed: %s on event %s, name: %s",
                    processor,
                    event.__class__.__name__,
                    event.name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
                # Promote processor failures to fatal pipeline state so
                # `raise_if_failed()` re-raises and `Pipeline.run()` exits
                # non-zero. Without this, processor tracebacks reach the log
                # but the pipeline still returns cleanly.
                self.pipeline.set_fatal_error(exc)
        except CancelledError:
            # `future.exception()` may raise if task was cancelled.
            logger.info(
                "processor cancelled: %s on event %s, name: %s",
                processor,
                event.__class__.__name__,
                event.name,
            )
        finally:
            # Mark context inactive before opening the next mailbox slot.
            self.deactivate()
            # Always release mailbox state so processor can receive next event.
            inbox.mark_task_done()
            # Always update global completion counter; must stay balanced.
            self.pipeline.decrement()
            # Nudge dispatcher out of wait-loop if requested.
            if signal is not None:
                signal.set()


# Tie-breaker for ProcessEntry so equal-priority entries dispatch FIFO.
# (heapq is not stable for equal keys.)
_PROCESS_ENTRY_SEQ = itertools.count()


@dataclass(order=True, frozen=True)
class ProcessEntry:
    """Ready-queue entry. Lower ``priority`` dispatches first; ``seq`` breaks ties FIFO."""

    priority: float
    seq: int
    processor: Optional["AbstractProcessor"] = field(compare=False)


def _make_process_entry(priority: int, processor: "AbstractProcessor") -> ProcessEntry:
    return ProcessEntry(priority, next(_PROCESS_ENTRY_SEQ), processor)


# Sentinel entry used to unblock the dispatcher; priority=-inf so fatal-path
# STOPs preempt any queued work.
_STOP_SENTINEL = ProcessEntry(priority=float("-inf"), seq=-1, processor=None)


class Inbox:
    """Single-processor mailbox enforcing one in-flight task per processor.

    This mailbox handles per-processor sequencing while allowing overall
    pipeline-level concurrency across different processors.
    """

    def __init__(
        self,
        processor: "AbstractProcessor",
        ready_queue: PriorityQueue[ProcessEntry],
    ) -> None:
        self.processor = processor
        self.ready_queue = ready_queue
        self.events: SimpleQueue[Event] = SimpleQueue()
        # `pending` is manually synced with `events` queue depth because
        # SimpleQueue.qsize is unreliable for this purpose.
        self.pending = 0
        self.active = False
        self.lock = threading.RLock()

    def signal_ready(self) -> None:
        """Publish a runnable processor entry to the global ready queue."""
        self.ready_queue.put(_make_process_entry(self.processor.priority, self.processor))

    def put_event(self, event: Event) -> None:
        """Push one event into mailbox and schedule processor if currently idle."""
        should_signal = False
        with self.lock:
            # Append work item into per-processor FIFO.
            self.events.put(event)
            # Track queue depth explicitly.
            self.pending += 1
            # Transition idle -> active once, then enqueue processor once.
            if not self.active:
                self.active = True
                should_signal = True
        if should_signal:
            self.signal_ready()

    def take_event(self) -> Event:
        """Take next event without blocking.

        Caller assumes an event should exist when processor was marked ready.
        """
        try:
            # Non-blocking pop because dispatcher already validated readiness.
            event = self.events.get(block=False)
        except Empty:
            # Safety alarm for invariant violation.
            msg = f"taking from empty inbox from {self.processor}; this should not have happened"
            logger.critical(msg)
            raise RuntimeError(msg)
        with self.lock:
            # Mirror queue pop in explicit pending counter.
            self.pending -= 1
        return event

    def mark_task_done(self) -> None:
        """Finalize current task and re-schedule processor if pending work remains."""
        should_signal = False
        with self.lock:
            # If events remain, keep processor active and requeue immediately.
            if self.pending > 0:
                should_signal = True
            else:
                # No backlog left; processor becomes idle.
                self.active = False
        if should_signal:
            self.signal_ready()

    # `reset_active` is a dispatch-failure-path alias for `mark_task_done`;
    # the state transition is identical whether a task completed normally or
    # failed to start because the event couldn't be taken.
    reset_active = mark_task_done


def reject_archive_namespace_collisions(
    processors: "list[AbstractProcessor]",
) -> None:
    """Fail fast when two processors claim the same persistent-archive
    namespace (see :meth:`AbstractProcessor.archive_namespace`).

    A shared namespace aliases one archive between two distinct processors:
    their cache/dedupe state collides, the last writer wins, and a resume
    can replay one processor's artifacts under another's identity. The
    intended contract is that same-class persistent builders take distinct
    names; this guard turns a silent corruption into a construction-time error.
    """
    claimed: defaultdict[str, list["AbstractProcessor"]] = defaultdict(list)
    for processor in processors:
        namespace = processor.archive_namespace()
        if namespace is not None:
            claimed[namespace].append(processor)
    collisions = {
        namespace: sharers
        for namespace, sharers in claimed.items()
        if len(sharers) > 1
    }
    if not collisions:
        return

    def describe(processor: "AbstractProcessor") -> str:
        return f"{type(processor).__name__}(name={processor.name!r})"

    detail = "; ".join(
        f"{namespace!r} claimed by {[describe(p) for p in sharers]}"
        for namespace, sharers in sorted(collisions.items())
    )
    raise ValueError(
        "duplicate persistent-archive namespace(s): " + detail + ". "
        "Same-class persistent builders must take distinct names (pass "
        "name=...) so their archives and dedupe caches do not alias and "
        "corrupt resume."
    )


class Pipeline:
    """Global orchestrator that routes events and drives processor execution."""

    def __init__(
        self,
        processors: list["AbstractProcessor"],
        strict_interest_inference: bool = False,
        workspace: Optional[str | Path] = None,
        max_workers: Optional[int] = None,
        config: Optional[dict[str, Any]] = None,
        archive_options: Optional[dict[str, Any]] = None,
        key_fn: Optional[Callable[[Any], str]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # Dedupe/cache key policy. Defaults to a structural content hash
        # (``default_digest``). Callers with a cheaper, equally-stable key for
        # their own artifact types inject one here; the framework never needs to
        # know what those types are.
        self.key_fn: Callable[[Any], str] = key_fn or default_digest
        # Supported keys: ``wal_sync`` (bool, default True). Sync WAL writes
        # are durable but slower; set False for throughput at the cost of a
        # post-crash loss window.
        self.archive_options: dict[str, Any] = (
            {} if archive_options is None else dict(archive_options)
        )
        self.archive_options.setdefault("wal_sync", True)

        self.jobs = 0
        self.done = 0
        # `internal_jobs` excludes external `Pipeline.submit(...)` calls so
        # `run_phases` can distinguish work caused by a phase from concurrent
        # user-thread submissions.
        self.internal_jobs = 0
        self.cond = threading.Condition()

        self.rdyq: PriorityQueue[ProcessEntry] = PriorityQueue()
        self.inbox_lock = threading.Lock()
        self.archive_lock = threading.Lock()
        self.inboxes: dict[AbstractProcessor, Inbox] = {}
        self.archives_by_key: dict[tuple[Optional[str], bool], Any] = {}
        self.fatal_error: Optional[BaseException] = None
        self.accepting_submissions = True
        self._executor_shut_down = False
        # Once set, `decrement()` becomes a no-op: fatal/abort paths have
        # force-balanced `done`/`jobs` and further increments would drift.
        self._accounting_poisoned = False

        self.strict_interest_inference = strict_interest_inference
        if max_workers is None:
            self.max_workers = max(1, len(processors))
        else:
            if max_workers < 1:
                raise ValueError("max_workers must be >= 1")
            self.max_workers = max_workers
        self.config = {} if config is None else config.copy()

        reject_archive_namespace_collisions(processors)

        # (event_class_name | None, event_name | None) -> processor set
        self.processors: defaultdict[tuple[Optional[str], Optional[str]], set[Any]] = defaultdict(set)
        for processor in processors:
            self.register_processor(processor)

        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

        self.submit(Event(name="__INIT__"), _internal=True)

    def get_inbox(self, processor: "AbstractProcessor") -> Inbox:
        """Return singleton inbox for a processor (lazy initialized)."""
        with self.inbox_lock:
            inbox = self.inboxes.get(processor)
            if inbox is None:
                inbox = Inbox(processor, self.rdyq)
                self.inboxes[processor] = inbox
            return inbox

    def register_processor(self, processor: "AbstractProcessor") -> None:
        """Register processor under each inferred interest key.

        In strict mode, re-infer with ``strict=True`` so ambiguous/unrecognized
        match patterns raise at registration time. We can't rely on
        ``processor.interests`` here because the processor was constructed
        before it knew which pipeline it would join.
        """
        if self.strict_interest_inference:
            interests = frozenset(infer_interests(processor.process, strict=True))
        else:
            interests = processor.interests
        for interest in interests:
            logger.debug("registering interest for %s: %s", processor, interest)
            self.processors[interest].add(processor)

    def run(self) -> None:
        """Run full phase loop and graceful shutdown sequence."""
        # Shared worker pool used for all dispatched processor tasks.
        executor = ThreadPoolExecutor(self.max_workers)
        # Dedicated dispatcher thread consumes pulses and schedules work.
        dispatcher = threading.Thread(
            target=self.execute_events,
            args=(executor,),
            daemon=True,
        )
        dispatcher.start()
        try:
            self.run_phases()
        except KeyboardInterrupt:
            logger.info("pipeline interrupted; attempting graceful shutdown")
            try:
                self.run_poison_phase()
            except KeyboardInterrupt:
                # A second interrupt during shutdown forces an immediate abort path.
                self.abort_waiters()
                logger.info("pipeline interrupted again during shutdown; forcing termination")
        finally:
            # Send stop pulse to dispatcher loop.
            self.freeze_external_submissions()
            self.rdyq.put(_STOP_SENTINEL)
            dispatcher.join(timeout=5)
            if dispatcher.is_alive():
                self.set_fatal_error(RuntimeError("dispatcher thread did not terminate within timeout"))
            executor.shutdown(wait=True, cancel_futures=True)
            self._executor_shut_down = True
            self.close_archives()
            self.raise_if_failed()
            logger.info("pipeline run completed")

    def run_phases(self) -> None:
        """Drive cooperative phase progression until a phase reaches fixed-point.

        Fixed-point rule:
        - Drain all currently queued work.
        - Emit one `__PHASE__` event.
        - Drain all work caused by that phase event.
        - Stop when the phase event created no additional work beyond itself.
        """
        # Current phase index emitted as `__PHASE__` payload.
        phase = 0

        while True:
            self.raise_if_failed()
            logger.info("starting phase: %02d", phase)
            started = time.perf_counter()

            # Wait until all currently known jobs complete.
            self.wait()
            logger.info(
                "ending phase: %02d; elapsed time: %.4f seconds",
                phase,
                time.perf_counter() - started,
            )

            baseline_internal = self.internal_jobs_snapshot()

            # Advance and broadcast next phase marker, then drain to quiescence.
            phase += 1
            phase_recipients = self.submit(
                Event(name="__PHASE__", phase=phase),
                _internal=True,
            )
            self.wait()

            # If no processor listens for __PHASE__, no phase-driven work can continue.
            if phase_recipients == 0:
                break

            internal_after_phase = self.internal_jobs_snapshot()
            phase_only_internal = baseline_internal + phase_recipients
            if internal_after_phase < phase_only_internal:
                self.set_fatal_error(RuntimeError(
                    "job accounting drift detected: internal_after_phase < baseline + phase_recipients"
                ))
                self.raise_if_failed()
            if internal_after_phase == phase_only_internal:
                break

        # Final drain before poison-pill shutdown event.
        self.wait()
        self.run_poison_phase()

    def run_poison_phase(self) -> None:
        """Run the termination phase by emitting `__POISON__` and waiting for it."""
        logger.info("processing done; proceeding to shutdown phase")
        # Freeze all non-runtime submissions before termination.
        self.freeze_external_submissions()
        # Ask processors to finalize/flush via conventional poison event.
        self.submit(Event(name="__POISON__"), _internal=True)

        self.wait()
        logger.info("dispatcher terminated")

    def is_executor_shut_down(self) -> bool:
        """Whether the worker pool has been torn down by ``run()``."""
        return self._executor_shut_down

    def jobs_snapshot(self) -> int:
        """Read cumulative scheduled-job counter under lock."""
        with self.cond:
            return self.jobs

    def internal_jobs_snapshot(self) -> int:
        """Read cumulative internal-only scheduled-job counter under lock."""
        with self.cond:
            return self.internal_jobs

    def set_fatal_error(self, exc: BaseException) -> None:
        """Record terminal runtime failure and wake all blocked loops/waiters."""
        with self.cond:
            if self.fatal_error is None:
                self.fatal_error = exc
            self.accepting_submissions = False
            self._accounting_poisoned = True
            # Force waiters to unblock immediately.
            self.done = self.jobs
            self.cond.notify_all()
        # Unblock dispatcher so it can exit.
        self.rdyq.put(_STOP_SENTINEL)

    def raise_if_failed(self) -> None:
        """Raise a deterministic runtime error if the pipeline entered fatal state."""
        with self.cond:
            exc = self.fatal_error
        if exc is not None:
            raise RuntimeError("pipeline terminated due to an internal dispatch error") from exc

    def abort_waiters(self) -> None:
        """Force all waiters to unblock by setting done == jobs."""
        with self.cond:
            self.accepting_submissions = False
            self._accounting_poisoned = True
            self.done = self.jobs
            self.cond.notify_all()

    def freeze_external_submissions(self) -> None:
        """Disallow further non-internal submissions."""
        with self.cond:
            self.accepting_submissions = False

    def close_archives(self) -> None:
        """Close all cached archive handles."""
        for (suffix, readonly), archive in self.archives_by_key.items():
            logger.info("closing archive for archive: %s", suffix)
            try:
                archive.close()
            except Exception:
                logger.exception(
                    "failed to close archive: %s (readonly=%s)",
                    suffix,
                    readonly,
                )

    def recipients_for_event(self, event: Event) -> set["AbstractProcessor"]:
        """Resolve target processors based on inferred interests and event shape."""
        recipients: set[AbstractProcessor] = set()

        # Optional generic fallback for processors registered at (None, None).
        if not self.strict_interest_inference:
            recipients |= self.processors.get((None, None), set())

        # Include exact and wildcard-name matches across event MRO classes.
        for event_class in self.event_class_names(event):
            recipients |= self.processors.get((event_class, None), set())
            recipients |= self.processors.get((event_class, event.name), set())

        return recipients

    def submit(self, event: Event, *, _internal: bool = False) -> int:
        """Route event to recipient inboxes and pulse dispatcher.

        Returns number of matched recipients.
        """
        with self.cond:
            if self._accounting_poisoned:
                raise RuntimeError("pipeline is not accepting submissions")
            if not _internal and not self.accepting_submissions:
                raise RuntimeError("pipeline is not accepting external submissions")
            recipients = self.recipients_for_event(event)
            if not recipients:
                return 0
            self.jobs += len(recipients)
            if _internal:
                self.internal_jobs += len(recipients)
            for processor in recipients:
                self.get_inbox(processor).put_event(event)

        return len(recipients)

    def execute_events(self, executor: Optional[ThreadPoolExecutor] = None) -> None:
        """Dispatcher loop: block on ready queue, dispatch entries, stop on sentinel."""
        if executor is None:
            # Convenience mode for tests/callers not providing an executor.
            with ThreadPoolExecutor(self.max_workers) as owned_executor:
                self.execute_events(owned_executor)
            return

        while True:
            entry = self.rdyq.get()
            if entry.processor is None:
                return
            if self.fatal_error is not None:
                return
            self.dispatch_entry(entry, executor)

    def dispatch_entry(
        self,
        entry: ProcessEntry,
        executor: ThreadPoolExecutor,
    ) -> None:
        """Submit one processor task to executor from a ready queue entry."""
        processor = entry.processor
        inbox = self.get_inbox(processor)

        try:
            # Pull the specific event assigned to this processor run.
            event = inbox.take_event()
        except Exception as exc:
            logger.exception("failed to take event for %s", processor)
            # Release the active flag so the inbox isn't wedged if the
            # pipeline is somehow resumed after a fatal error.
            try:
                inbox.reset_active()
            except Exception:
                logger.exception("failed to reset inbox active flag for %s", processor)
            self.set_fatal_error(exc)
            return

        # Fresh context per task to isolate emitted-event capture.
        context = Context(self, enforce_lifecycle=True)
        callback = functools.partial(
            context.done_callback,
            processor=processor,
            event=event,
            inbox=inbox,
        )

        try:
            # Submit user processor logic.
            def invoke() -> Any:
                context.activate()
                return processor.process(context, event)

            future = executor.submit(invoke)
            # Attach completion accounting/wakeup callback.
            future.add_done_callback(callback)
        except Exception as exc:
            # If submit itself fails, emulate failed future so callbacks still run.
            failure = Future()
            failure.set_exception(exc)
            callback(failure)

    def increment(self, by: int = 1) -> None:
        """Increase scheduled-job counter by `by`."""
        with self.cond:
            self.jobs += by

    def decrement(self) -> None:
        """Increase completed-job counter and notify waiters when balanced.

        No-ops once accounting has been poisoned by fatal/abort paths — the
        counters have already been force-balanced, so further increments would
        only drift ``done`` above ``jobs``.
        """
        with self.cond:
            if self._accounting_poisoned:
                return
            self.done += 1
            if self.done >= self.jobs:
                self.cond.notify_all()

    def is_idle(self) -> bool:
        """Return True when completed count has caught up with scheduled count."""
        with self.cond:
            return self.done >= self.jobs

    def wait(self) -> None:
        """Block caller until accounting reaches idle (`done >= jobs`)."""
        with self.cond:
            try:
                self.cond.wait_for(
                    lambda: self.done >= self.jobs or self.fatal_error is not None
                )
                if self.fatal_error is not None:
                    raise RuntimeError(
                        "pipeline wait aborted due to an internal dispatch error"
                    ) from self.fatal_error
            except KeyboardInterrupt:
                # Propagate interrupt so run() can drive graceful shutdown.
                raise

    @staticmethod
    def event_class_names(event: Event) -> list[str]:
        """Return event class names across MRO limited to Event subclasses."""
        names = [
            cls.__name__
            for cls in type(event).mro()
            if issubclass(cls, Event) and cls is not object
        ]
        return names or [event.__class__.__name__]

    def digest(self, obj) -> str:
        """Compute the dedupe/cache key for ``obj`` via the configured policy.

        Builders call this (rather than hashing directly) so the key strategy
        is a single injectable seam — see ``key_fn`` on ``__init__``.
        """
        return self.key_fn(obj)

    def archive(self, suffix: Optional[str], readonly: bool = False):
        """Get memoized archive handle for `(suffix, readonly)`.

        `suffix=None` maps to `"default"` when workspace persistence is enabled.
        """
        normalized = "default" if self.workspace is not None and suffix is None else suffix
        key = (normalized, bool(readonly))
        with self.archive_lock:
            archive = self.archives_by_key.get(key)
            if archive is None:
                archive = self.open_archive(normalized, readonly)
                self.archives_by_key[key] = archive
            return archive

    def open_archive(self, suffix: Optional[str], readonly: bool):
        """Create archive backend instance (in-memory or RocksDB)."""
        if self.workspace is None:
            # Non-persistent fallback storage.
            return InMemCache()

        # Persistent storage path under workspace root.
        path = self.workspace.joinpath(suffix)
        path.parent.mkdir(parents=True, exist_ok=True)

        archive = Rdict(
            str(path),
            access_type=AccessType.read_only() if readonly else AccessType.read_write(),
        )
        # Configure serialization for arbitrary Python objects.
        archive.set_dumps(dill.dumps)
        archive.set_loads(dill.loads)

        write_options = WriteOptions()
        write_options.sync = bool(self.archive_options.get("wal_sync", True))
        archive.set_write_options(write_opt=write_options)
        return archive


class AbstractProcessor(abc.ABC):
    """Base class for all pipeline processors."""

    def __init__(self, name: Optional[str] = None, priority: int = 0) -> None:
        # Routing interests inferred from `match` in `process`. The pipeline
        # re-infers in strict mode if configured, since the processor is
        # typically constructed before it knows which pipeline it joins.
        self.interests = frozenset(infer_interests(self.process))
        self.name = self.__class__.__name__ if name is None else name
        self.priority = priority

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        """Handle one event.

        Implementations typically use `match event:` patterns.
        """
        raise NotImplementedError

    #: Override on a subclass to opt out of source-text-based signatures.
    #: When set, :meth:`signature` uses this instead of hashing class source —
    #: useful when source is unstable (REPL/Jupyter) or you want explicit
    #: control.
    version: Optional[str] = None

    @functools.cache
    def signature(self) -> str:
        """Stable processor identity string used for cache namespacing."""
        cls = self.__class__
        if cls.version is not None:
            digest_input = f"version:{cls.__module__}.{cls.__qualname__}:{cls.version}"
            digest = hashlib.sha256(digest_input.encode()).hexdigest()
        else:
            digest = normalized_source_digest(cls)
            if digest is None:
                logger.warning(
                    "source unavailable for %s; falling back to module-qualified signature",
                    self,
                )
                digest_input = f"qualname:{cls.__module__}.{cls.__qualname__}"
                digest = hashlib.sha256(digest_input.encode()).hexdigest()
        return str(Path(self.name, digest))

    def archive_namespace(self) -> Optional[str]:
        """The shared persistent-archive namespace this processor claims, or
        None when it uses no shared persistent archive.

        Processors that persist cache/dedupe state into the pipeline's archive
        return a stable key here (typically their :meth:`signature`). Two
        processors that return the same non-None namespace would alias one
        archive — silently sharing a cache/dedupe space and corrupting resume
        — so :class:`Pipeline` rejects such a collision at construction.
        Default: None (most processors keep no shared archive)."""
        return None
