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
import logging
import threading
import time
from collections import UserDict, defaultdict
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue, SimpleQueue
from typing import Any, Optional

import dill
from rocksdict import Rdict, WriteOptions
from rocksdict.rocksdict import AccessType

# Re-export selected helpers so callers can import from framework.core directly.
from .utils import (
    caching,
    class_identifier,
    event_digest,
    find_match_stmt,
    get_source,
    in_jupyter_notebook,
    infer_interests,
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

    def close(self) -> None:
        # Release in-memory entries explicitly for symmetry with persistent stores.
        self.clear()

    def flush_wal(self, *args: Any, **kwargs: Any) -> None:
        # No-op compatibility method for RocksDB API parity.
        return None

    def flush(self, *args: Any, **kwargs: Any) -> None:
        # No-op compatibility method for RocksDB API parity.
        return None


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
        """Submit a downstream event and track it for replay/caching."""
        try:
            self.ensure_submission_is_safe()
            # Preserve emission order exactly as produced by the processor.
            self.events.append(event)
            # Return recipient count from pipeline routing.
            return self.pipeline.submit(event)
        except Exception:
            # Keep contextual logging close to producer call site.
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
            # Extract underlying exception from completed future, if any.
            exc = future.exception()
            # Ignore shutdown noise from executor teardown path.
            shutdown_error = isinstance(exc, RuntimeError) and (
                str(exc) == "cannot schedule new futures after shutdown"
            )
            # #concern: String-matching exception text is brittle across Python
            # versions/executor implementations.
            if exc is not None and not shutdown_error:
                logger.error(
                    "processor failed: %s on event %s, name: %s",
                    processor,
                    event.__class__.__name__,
                    event.name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
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


@dataclass(order=True, frozen=True)
class ProcessEntry:
    """Unit stored in ready queue.

    Ordering uses `priority` only. `processor` is excluded from comparisons.
    """

    # Lower value sorts first in PriorityQueue.
    # #concern: This is easy to misread as "higher number = higher priority".
    priority: int
    # Processor to dispatch when this entry is popped.
    processor: "AbstractProcessor" = field(compare=False)


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
        # Owning processor for this mailbox.
        self.processor = processor
        # Shared global ready queue where runnable processors are enqueued.
        self.ready_queue = ready_queue
        # FIFO of inbound events for this processor.
        self.events: SimpleQueue[Event] = SimpleQueue()
        # Number of queued-but-not-yet-taken events.
        self.pending = 0
        # Whether processor currently has an active or reserved task.
        self.active = False
        # Re-entrant lock guarding `pending`/`active` consistency.
        self.lock = threading.RLock()
        # #concern: `SimpleQueue` length is not introspectable, so `pending`
        # must remain perfectly synchronized manually.

    def signal_ready(self) -> None:
        """Publish a runnable processor entry to the global ready queue."""
        self.ready_queue.put(ProcessEntry(self.processor.priority, self.processor))

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


class Pipeline:
    """Global orchestrator that routes events and drives processor execution."""

    def __init__(
        self,
        processors: list["AbstractProcessor"],
        strict_interest_inference: bool = False,
        workspace: Optional[str | Path] = None,
        max_workers: Optional[int] = None,
        config: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Total scheduled processor executions since pipeline start.
        self.jobs = 0
        # Total completed processor executions since pipeline start.
        self.done = 0
        # Condition variable coordinating `wait()` and accounting updates.
        self.cond = threading.Condition()

        # Ready processors ordered by priority.
        self.rdyq: PriorityQueue[ProcessEntry] = PriorityQueue()
        # Pulse queue used to trigger dispatcher drain cycles.
        self.q: SimpleQueue[bool] = SimpleQueue()
        # Protects pulse de-duplication state below.
        self.dispatch_pulse_lock = threading.Lock()
        # True means there is already at least one pending dispatcher pulse.
        self.dispatch_pulse_enqueued = False

        # Lock protecting lazy inbox map creation.
        self.inbox_lock = threading.Lock()
        # Lock protecting archive cache map.
        self.archive_lock = threading.Lock()
        # Processor -> Inbox singleton map.
        self.inboxes: dict[AbstractProcessor, Inbox] = {}
        # (suffix, readonly) -> archive handle cache.
        self.archives_by_key: dict[tuple[Optional[str], bool], Any] = {}
        # Terminal internal failure captured by dispatcher/runtime paths.
        self.fatal_error: Optional[BaseException] = None
        # False once runtime begins shutdown/fatal termination.
        self.accepting_submissions = True

        # Controls whether generic interest fallback is allowed.
        self.strict_interest_inference = strict_interest_inference
        # Thread pool size for concurrent processor execution.
        self.max_workers = len(processors) if max_workers is None else max_workers
        # Opaque runtime config for user processors.
        self.config = {} if config is None else config.copy()

        # Interest index:
        # (event_class_name | None, event_name | None) -> processor set
        self.processors: defaultdict[tuple[Optional[str], Optional[str]], set[Any]] = defaultdict(set)
        for processor in processors:
            self.register_processor(processor)

        # Optional workspace for persistent archive storage.
        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

        # Bootstrap lifecycle so processors can initialize internal state.
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
        """Register processor under each inferred interest key."""
        for interest in processor.interests:
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
            self.q.put(False)
            dispatcher.join(timeout=5)
            if dispatcher.is_alive():
                self.set_fatal_error(RuntimeError("dispatcher thread did not terminate within timeout"))
            executor.shutdown(wait=True, cancel_futures=True)
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

            # Trigger dispatcher to drain currently queued work.
            self.pulse_dispatcher()
            # Wait until all currently known jobs complete.
            self.wait()
            logger.info(
                "ending phase: %02d; elapsed time: %.4f seconds",
                phase,
                time.perf_counter() - started,
            )

            # Snapshot cumulative jobs once queue is quiescent.
            baseline_jobs = self.jobs_snapshot()

            # Advance and broadcast next phase marker, then drain to quiescence.
            phase += 1
            phase_recipients = self.submit(
                Event(name="__PHASE__", phase=phase),
                _internal=True,
            )
            self.pulse_dispatcher()
            self.wait()

            # If no processor listens for __PHASE__, no phase-driven work can continue.
            if phase_recipients == 0:
                break

            jobs_after_phase = self.jobs_snapshot()
            phase_only_jobs = baseline_jobs + phase_recipients
            if jobs_after_phase < phase_only_jobs:
                # Deterministic fail-fast on accounting corruption.
                err = RuntimeError(
                    "job accounting drift detected: jobs_after_phase < baseline + phase_recipients"
                )
                self.set_fatal_error(err)
                self.raise_if_failed()

            # Continue only if phase processing produced additional work.
            if jobs_after_phase == phase_only_jobs:
                break

        # Final drain before poison-pill shutdown event.
        self.pulse_dispatcher()
        self.wait()
        self.run_poison_phase()

    def run_poison_phase(self) -> None:
        """Run the termination phase by emitting `__POISON__` and waiting for it."""
        logger.info("processing done; proceeding to shutdown phase")
        # Freeze all non-runtime submissions before termination.
        self.freeze_external_submissions()
        # Ask processors to finalize/flush via conventional poison event.
        self.submit(Event(name="__POISON__"), _internal=True)
        self.pulse_dispatcher()
        self.wait()
        logger.info("dispatcher terminated")

    def jobs_snapshot(self) -> int:
        """Read cumulative scheduled-job counter under lock."""
        with self.cond:
            return self.jobs

    def set_fatal_error(self, exc: BaseException) -> None:
        """Record terminal runtime failure and wake all blocked loops/waiters."""
        with self.cond:
            if self.fatal_error is None:
                self.fatal_error = exc
            self.accepting_submissions = False
            # Force waiters to unblock immediately.
            self.done = self.jobs
            self.cond.notify_all()
        with self.dispatch_pulse_lock:
            # Reset pulse state so shutdown pulses cannot be suppressed.
            self.dispatch_pulse_enqueued = False
        # Stop dispatcher loop as soon as possible.
        self.q.put(False)

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
        recipients = self.recipients_for_event(event)
        if not recipients:
            return 0

        with self.cond:
            if not _internal and not self.accepting_submissions:
                raise RuntimeError("pipeline is not accepting external submissions")
            # Reserve one completion slot per recipient task.
            self.jobs += len(recipients)
        for processor in recipients:
            self.get_inbox(processor).put_event(event)

        # Wake dispatcher to consume newly ready processors.
        self.pulse_dispatcher()
        return len(recipients)

    def pulse_dispatcher(self) -> None:
        """Enqueue a scheduler pulse.

        Dispatcher consumes pulses from `self.q` and drains until idle.
        """
        with self.dispatch_pulse_lock:
            if self.dispatch_pulse_enqueued:
                return
            self.dispatch_pulse_enqueued = True
        self.q.put(True)

    def execute_events(self, executor: Optional[ThreadPoolExecutor] = None) -> None:
        """Dispatcher loop consuming pulses and scheduling ready work."""
        if executor is None:
            # Convenience mode for tests/callers not providing an executor.
            with ThreadPoolExecutor(self.max_workers) as owned_executor:
                self.execute_events(owned_executor)
            return

        # Main dispatcher loop:
        # True pulse -> drain
        # False pulse -> stop
        while True:
            pulse = self.q.get()
            if not pulse:
                return
            with self.dispatch_pulse_lock:
                self.dispatch_pulse_enqueued = False
            self.dispatch_until_idle(executor)

    def dispatch_until_idle(self, executor: ThreadPoolExecutor) -> None:
        """Drain ready queue until no runnable work remains."""
        while True:
            self.raise_if_failed()
            entry = self.next_ready_entry(timeout=0.05)
            if entry is None:
                # Stop draining when no queued work and global accounting is idle.
                if self.is_idle():
                    return

                # Continue polling for new ready entries while work remains.
                continue

            self.dispatch_entry(entry, executor)

    def next_ready_entry(self, timeout: Optional[float] = None) -> Optional[ProcessEntry]:
        """Pop next ready processor entry if available, else None."""
        try:
            if timeout is None:
                return self.rdyq.get_nowait()
            return self.rdyq.get(timeout=timeout)
        except Empty:
            return None

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
        """Increase completed-job counter and notify waiters when balanced."""
        with self.cond:
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

        # Force WAL sync for durability.
        write_options = WriteOptions()
        write_options.sync = True
        archive.set_write_options(write_opt=write_options)
        # #concern: Synchronous WAL writes can reduce throughput significantly.
        return archive


class AbstractProcessor(abc.ABC):
    """Base class for all pipeline processors."""

    def __init__(self, name: Optional[str] = None, priority: int = 0) -> None:
        # Static-ish routing interests inferred from processor `match` AST.
        self.interests = frozenset(infer_interests(self.process))
        # Human-readable instance name; defaults to class name.
        self.name = self.__class__.__name__ if name is None else name
        # Scheduler priority (lower value dispatches first).
        self.priority = priority

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        """Handle one event.

        Implementations typically use `match event:` patterns.
        """
        raise NotImplementedError

    @functools.cache
    def signature(self) -> str:
        """Stable-ish processor identity string used for cache namespacing."""
        return str(
            Path(
                # Include logical name first for readability in storage paths.
                self.name,
                # Include source hash so cache namespace changes with code edits.
                hashlib.sha256(get_source(self.__class__).encode()).hexdigest(),
            )
        )
        # #concern: Source-based hashing can be unstable in interactive/runtime-
        # generated classes and may vary across packaging/import contexts.
