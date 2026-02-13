"""Core runtime primitives for the framework event pipeline.

The module defines:

* :class:`Event` – immutable-style messages flowing through pipelines.
* :class:`AbstractProcessor` – base class for stateful, single-threaded processors
  whose interests are inferred from structural pattern matching.
* :class:`Pipeline` – dispatcher that performs cooperative, phase-oriented scheduling.
* Caching, retry, and signal-handling utilities that keep long-running jobs resilient.
"""

import abc
import functools
import hashlib
import inspect
import logging
import threading
import time
from collections import defaultdict, UserDict, deque
from concurrent.futures import Future, ThreadPoolExecutor, CancelledError
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from queue import PriorityQueue, SimpleQueue
from typing import List, Optional

import deepdiff
import dill
from rocksdict import Rdict, WriteOptions
from rocksdict.rocksdict import AccessType

logger = logging.getLogger(__name__)


class Event:
    """Lightweight container for pipeline messages."""

    def __init__(self, name: str, **kwargs):
        self.name: str = name
        self.__dict__.update(kwargs)


class InMemCache(UserDict):
    """Drop-in mapping used when RocksDB persistence is not configured."""

    def close(self):
        self.clear()

    def flush_wal(self, *args, **kwargs):
        pass

    def flush(self, *args, **kwargs):
        pass


class NoOpCache(UserDict):
    """Mapping that swallows writes; useful when processors should never replay."""

    def close(self):
        self.clear()

    def __setitem__(self, key, value):
        pass  # do nothing

    def flush_wal(self, *args, **kwargs):
        pass

    def flush(self, *args, **kwargs):
        pass


@contextmanager
def timeit(name, logger: logging.Logger, logging_threshold: float = 5.0):
    """Context manager that logs elapsed time when it crosses a threshold."""
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        if (delta := end - start) > logging_threshold:
            logger.debug(f"{name} finished after {delta:.4f} seconds")


def retry(max_attempts):
    """Decorator factory that retries the wrapped callable up to ``max_attempts``."""

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                start = time.perf_counter()
                logger.debug(
                    f"attempt {attempt + 1} / {max_attempts} at {func.__qualname__} began"
                )
                try:
                    return func(*args, **kwargs)
                except Exception:
                    elapsed = time.perf_counter() - start
                    is_last_attempt = attempt >= max_attempts - 1
                    if is_last_attempt:
                        logger.error(
                            f"attempt {attempt + 1} / {max_attempts} at {func.__qualname__} failed after {elapsed:.4f}s ... escalating",
                            exc_info=True,
                        )
                        raise
                    logger.warning(
                        f"attempt {attempt + 1} / {max_attempts} at {func.__qualname__} failed after {elapsed:.4f}s ... retrying",
                        exc_info=True,
                    )

        return wrapper

    return decorator_retry


class Context:
    """Execution context shared with processors during ``process`` calls."""

    def __init__(self, pipeline):
        self.pipeline: Pipeline = pipeline
        self.events = []

    def submit(self, event: Event) -> int:
        """Emit a new event back into the pipeline and record it for caching. Returns the number of recipients that have registered interest of the event."""
        try:
            self.events.append(
                event
            )  # Maintain emission order for deterministic replay.
            return self.pipeline.submit(event)
        except Exception:
            logger.exception("failed to submit event %s", event)
            raise

    def done_callback(
        self,
        future: Future,
        processor: "AbstractProcessor",
        event: Event,
        inbox: "Inbox",
        q: Optional[SimpleQueue] = None,
    ):
        """Callback attached to processor futures to handle completion.

        - Logs any exception raised by the processor.
        - Always decrements the pipeline job counter.
        """
        try:
            exc = future.exception()
            match exc:
                case None:
                    pass
                case RuntimeError() as re if (
                    str(re) == "cannot schedule new futures after shutdown"
                ):
                    # logger.debug(f'scheduling skipped: {processor} on event {event.__class__.__name__} skipped due to executor having been shut down')
                    pass
                case _:
                    logger.error(
                        f"processor failed: {processor} on event {event.__class__.__name__}, name: {event.name}",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )
        except CancelledError:
            logger.info(
                f"processor cancelled: {processor} on event {event.__class__.__name__}, name: {event.name}",
            )
        finally:
            inbox.mark_task_done()
            self.pipeline.decrement()  # Ensure job accounting is balanced even on failure.
            if q is not None:
                # Wake execute_events compatibility loops, if present.
                q.put(True)


@dataclass(order=True, frozen=True)
class ProcessEntry:
    """Entry stored in the ready queue with ordering by processor priority."""

    priority: int
    processor: "AbstractProcessor" = field(
        compare=False,
    )


class Inbox:
    """Holds events for a single processor and exposes cooperative scheduling."""

    def __init__(
        self,
        processor: "AbstractProcessor",
        ready_queue: Optional[PriorityQueue[ProcessEntry]],
        concurrency: int = 1,
    ):
        self.processor = processor
        self.events = deque()
        self.jobs = 0
        self.active = False
        self.lock = threading.RLock()
        # Kept for compatibility with older tests and callers.
        self.concurrency = concurrency
        self.slots_left = concurrency
        self.ready_queue = ready_queue

    def put_event(self, event: Event):
        """Queue an event for this processor."""
        with self.lock:
            self.events.append(event)
            self.jobs += 1
            if self.ready_queue is not None and self.slots_left > 0:
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )

    def take_event(self):
        """Return the next event without blocking; raise if the inbox is empty."""
        with self.lock:
            if not self.events:
                msg = (
                    f"taking from empty inbox from {self.processor}; this should not "
                    "have happened"
                )
                logger.critical(msg)
                raise RuntimeError(msg)
            event = self.events.popleft()
            self.jobs -= 1
            return event

    def mark_task_done(self):
        """Compatibility behavior for callers that rely on ready-queue signaling."""
        with self.lock:
            self.slots_left = min(self.slots_left + 1, self.concurrency)
            if self.ready_queue is not None and self.jobs > 0:
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )

    def try_start_next(self) -> Optional[Event]:
        """Reserve processor execution and return next event if idle + non-empty."""
        with self.lock:
            if self.active or self.jobs == 0:
                return None
            self.active = True
            return self.take_event()

    def finish_current_and_take_next(self) -> Optional[Event]:
        """Advance mailbox after one task; keep active when more events are queued."""
        with self.lock:
            if self.jobs > 0:
                return self.take_event()
            self.active = False
            return None

    def restore_unstarted(self, event: Event):
        """Return an unscheduled event to the front and mark mailbox idle."""
        with self.lock:
            self.events.appendleft(event)
            self.jobs += 1
            self.active = False


class AbstractScheduler(abc.ABC):
    """Strategy interface for event scheduling/execution."""

    @abc.abstractmethod
    def bind_executor(self, executor: ThreadPoolExecutor):
        pass

    @abc.abstractmethod
    def restore_executor(self, previous):
        pass

    @abc.abstractmethod
    def on_submit(self, scheduled: list[tuple["AbstractProcessor", Inbox]]):
        pass

    @abc.abstractmethod
    def drain(self, q: Optional[SimpleQueue] = None):
        pass


class MailboxScheduler(AbstractScheduler):
    """Single-active-task-per-processor scheduler backed by inbox mailboxes."""

    def __init__(self, pipeline: "Pipeline"):
        self.pipeline = pipeline
        self._executor_lock = threading.Lock()
        self._executor: Optional[ThreadPoolExecutor] = None

    def bind_executor(self, executor: ThreadPoolExecutor):
        with self._executor_lock:
            previous = self._executor
            self._executor = executor
        self.drain()
        return previous

    def restore_executor(self, previous):
        with self._executor_lock:
            self._executor = previous

    def on_submit(self, scheduled: list[tuple["AbstractProcessor", Inbox]]):
        for processor, inbox in scheduled:
            self._start_if_idle(processor, inbox)

    def drain(self, q: Optional[SimpleQueue] = None):
        with self.pipeline._inbox_lock:
            inbox_items = list(self.pipeline._inboxes.items())
        for processor, inbox in inbox_items:
            self._start_if_idle(processor, inbox, q=q)

    def _current_executor(self) -> Optional[ThreadPoolExecutor]:
        with self._executor_lock:
            return self._executor

    def _start_if_idle(
        self,
        processor: "AbstractProcessor",
        inbox: Inbox,
        q: Optional[SimpleQueue] = None,
    ):
        event = inbox.try_start_next()
        if event is None:
            return

        executor = self._current_executor()
        if executor is None:
            inbox.restore_unstarted(event)
            return

        self._submit_processor_task(executor, processor, inbox, event, q=q)

    def _continue_processor(
        self,
        processor: "AbstractProcessor",
        inbox: Inbox,
        q: Optional[SimpleQueue] = None,
    ):
        event = inbox.finish_current_and_take_next()
        if event is None:
            return

        executor = self._current_executor()
        if executor is None:
            inbox.restore_unstarted(event)
            return

        self._submit_processor_task(executor, processor, inbox, event, q=q)

    def _submit_processor_task(
        self,
        executor: ThreadPoolExecutor,
        processor: "AbstractProcessor",
        inbox: Inbox,
        event: Event,
        q: Optional[SimpleQueue] = None,
    ):
        context = Context(self.pipeline)
        done_callback = functools.partial(
            context.done_callback,
            processor=processor,
            event=event,
            inbox=inbox,
            q=q,
        )

        def on_done(future: Future):
            try:
                done_callback(future)
            finally:
                self._continue_processor(processor, inbox, q=q)

        try:
            future = executor.submit(processor.process, context, event)
            future.add_done_callback(on_done)
        except Exception as exc:
            f = Future()
            f.set_exception(exc)
            on_done(f)


class Pipeline:
    """Route events to interested processors and run per-processor mailboxes.

    Each processor has one inbox and at most one in-flight task at a time.
    Submissions enqueue events and immediately schedule idle processors on a
    shared :class:`ThreadPoolExecutor`.

    Job accounting remains cumulative via ``jobs``/``done`` so :meth:`wait`
    blocks until quiescence (``done >= jobs``), including nested submissions
    emitted by worker threads.
    """

    def __init__(
        self,
        processors: List["AbstractProcessor"],
        strict_interest_inference=False,
        workspace: Optional[str | Path] = None,
        max_workers=None,
        config=None,
        scheduler: Optional[AbstractScheduler] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.jobs = 0
        self.done = 0
        self.cond = threading.Condition()
        self.rdyq = PriorityQueue()
        self.q = SimpleQueue()
        self._inbox_lock = threading.Lock()
        self._archive_lock = threading.Lock()
        self._inboxes = dict()
        self._archives_by_key = dict()
        self.scheduler: AbstractScheduler = (
            MailboxScheduler(self) if scheduler is None else scheduler
        )
        self.strict_interest_inference = strict_interest_inference
        self.max_workers = len(processors) if max_workers is None else max_workers
        self.config = dict() if config is None else config.copy()

        self.archives = list()

        self.processors = defaultdict(set)
        for processor in processors:
            self.register_processor(processor)

        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

        self.submit(Event(name="__INIT__"))

    def get_inbox(self, processor: "AbstractProcessor"):
        """Return (and memoize) the inbox dedicated to ``processor``."""
        with self._inbox_lock:
            inbox = self._inboxes.get(processor)
            if inbox is None:
                # run()/submit() scheduling no longer uses the ready queue.
                inbox = Inbox(processor, None, 1)
                self._inboxes[processor] = inbox
            return inbox

    def register_processor(self, processor: "AbstractProcessor"):
        """Register every declared interest emitted by ``infer_interests``."""
        # Register every declared interest for this processor.
        for interest in processor.interests:
            logger.debug(f"registering interest for {processor}: {interest}")
            self.processors[interest].add(processor)

    def run(self):
        """Drive the pipeline until all work (and phase-driven work) has completed."""
        executor = ThreadPoolExecutor(self.max_workers)
        previous_executor = self.scheduler.bind_executor(executor)

        try:
            aborting = threading.Event()
            with self.cond:
                last_jobs = self.jobs
                aborting.clear()

            phase = 0
            idle_jobs_increment = 0
            while not aborting.is_set():
                logger.info(f"starting phase: {phase:02d}")
                start = time.perf_counter()
                self.scheduler.drain()
                self.wait()

                with self.cond:
                    if self.jobs <= (last_jobs + idle_jobs_increment):
                        # this means that no other new jobs were launched during this phase.
                        # proceed to abort
                        aborting.set()
                    last_jobs = self.jobs
                end = time.perf_counter()
                logger.info(
                    f"ending phase: {phase:02d}; elapsed time: {end - start:.4f} seconds"
                )
                phase += 1

                idle_jobs_increment = (
                    self.submit(Event(name="__PHASE__", phase=phase)) + 1
                )  # the expected increment in jobs count when no other jobs are launched by the processors.

            # Drain any phasing-spawned events.
            self.scheduler.drain()
            self.wait()

            # Final phase / termination.
            logger.info("processing done; proceeding to shutdown phase")

            # Submit poison-pill so processors can flush/terminate.
            self.submit(Event(name="__POISON__"))

            # Wait until the poison event is processed.
            self.wait()

            logger.info("pipeline terminated")
        except KeyboardInterrupt:
            # Ensure anyone waiting on cond is released.
            with self.cond:
                self.done = self.jobs
                self.cond.notify_all()
            logger.info("pipeline interrupted; proceeding to shutdown")
        finally:
            self.scheduler.restore_executor(previous_executor)
            executor.shutdown(wait=True, cancel_futures=True)

            for suffix, archive in self.archives:
                logger.info(f"closing archive for archive: {suffix}")
                print(f"closing archive for archive: {suffix}")
                archive.close()

            logger.info("pipeline run completed")

    def submit(self, event: Event):
        """Submit an event to the queue and increment job count."""
        recipients = set()
        scheduled = []
        self.increment()
        # Collect all relevant recipients
        if not self.strict_interest_inference:
            recipients |= self.processors.get((None, None), set())

        for event_class in self._event_class_names(event):
            recipients |= self.processors.get((event_class, None), set())
            recipients |= self.processors.get((event_class, event.name), set())

        for processor in recipients:
            self.increment()
            inbox = self.get_inbox(processor)
            inbox.put_event(event)
            scheduled.append((processor, inbox))
        self.decrement()
        self.scheduler.on_submit(scheduled)
        return len(recipients)

    def execute_events(
        self, q: SimpleQueue, executor: Optional[ThreadPoolExecutor] = None
    ):
        """Compatibility scheduler loop driven by pulses on ``q``.

        When ``executor`` is provided, it is used as-is and is not shut down by this
        method. When omitted, a new executor is created for the duration of the
        loop.
        """

        if executor is None:
            with ThreadPoolExecutor(self.max_workers) as owned_executor:
                self.execute_events(q, owned_executor)
            return

        previous_executor = self.scheduler.bind_executor(executor)
        try:
            while q.get():
                self.scheduler.drain(q=q)
        finally:
            self.scheduler.restore_executor(previous_executor)

    def increment(self):
        """Increment job counter (called when new work enters the system)."""
        with self.cond:
            self.jobs += 1

    def decrement(self):
        """Increment the completion counter and notify ``wait``ers if balanced."""
        with self.cond:
            self.done += 1
            if self.done >= self.jobs:
                self.cond.notify_all()

    def wait(self):
        """Block until all submitted work has finished (jobs == done)."""
        with self.cond:
            try:
                self.cond.wait_for(lambda: self.done >= self.jobs)
            except KeyboardInterrupt:
                self.done = self.jobs
                self.cond.notify_all()
                raise

    @staticmethod
    def _event_class_names(event: Event) -> List[str]:
        """
        Return the MRO class names for the event limited to Event subclasses.
        Enables processors matching on base Event types to receive subclassed events.
        """
        names = []
        for cls in type(event).mro():
            if cls is object:
                break
            if issubclass(cls, Event):
                names.append(cls.__name__)
        return names or [event.__class__.__name__]

    def archive(self, suffix: Optional[str], readonly=False):
        """Return a cache/rocksdb archive keyed by ``suffix`` (memoized per suffix)."""
        normalized_suffix = "default" if (self.workspace and suffix is None) else suffix
        key = (normalized_suffix, bool(readonly))
        with self._archive_lock:
            cached = self._archives_by_key.get(key)
            if cached is not None:
                return cached

            if self.workspace is None:
                archive = InMemCache()
            else:
                path = self.workspace.joinpath(normalized_suffix)
                path.parent.mkdir(parents=True, exist_ok=True)
                rdict = Rdict(
                    str(path),
                    access_type=[
                        AccessType.read_write(),
                        AccessType.read_only(),
                    ][readonly],
                )
                rdict.set_dumps(dill.dumps)
                rdict.set_loads(dill.loads)
                wo = WriteOptions()
                wo.sync = True
                rdict.set_write_options(write_opt=wo)
                archive = rdict

            self._archives_by_key[key] = archive
            self.archives.append((normalized_suffix, archive))
            return archive


def parse_pattern(p):
    """Extract (event_class, event_name) pairs from a match-case AST node."""
    import ast

    def get_class_identifier(node):
        match node:
            case ast.Name(id=id):
                return id
            case ast.Attribute(attr=attr):
                return attr
            case _:
                return None

    match p:
        case ast.MatchClass(
            cls=cls,
            kwd_attrs=["name", *_],
            kwd_patterns=[ast.MatchValue(value=ast.Constant(value=event_name)), *_],
        ):
            class_id = get_class_identifier(cls)
            yield (class_id, event_name) if class_id else (None, None)
        case ast.MatchClass(cls=cls):
            class_id = get_class_identifier(cls)
            yield (class_id, None) if class_id else (None, None)
        case ast.MatchOr(patterns=patterns):
            for pattern in patterns:
                yield from parse_pattern(pattern)
        case ast.AST(pattern=pattern):
            yield from parse_pattern(pattern)
        case _:
            yield None, None


def infer_interests(func):
    """Inspect a processor's ``match`` statement to infer routing interests."""
    import ast
    import textwrap

    tree = ast.parse(textwrap.dedent(get_source(func)))
    match tree:
        case ast.Module(body=[ast.FunctionDef(body=[*_, ast.Match() as match_stmt])]):
            for case in match_stmt.cases:
                for interest in parse_pattern(case.pattern):
                    match interest:
                        case (None, None):
                            logger.warning(
                                f"failed to identify interests in processor {func}: there is a match-case clause but we couldn't identify a valid event-name combination."
                            )
                        case _:
                            pass
                    yield interest
        case _:
            logger.warning(
                f"The processor {func} does not seem to have declared any interest to events"
            )
            yield None, None


class AbstractProcessor(abc.ABC):
    """Base class for stateful processors executed within the pipeline."""

    def __init__(self, name: Optional[str] = None, priority: int = 0):
        self.interests = frozenset(infer_interests(self.process))
        self.name = self.__class__.__name__ if name is None else name
        self.priority = priority

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        """Handle a single event. Subclasses should use ``match`` on ``event``."""
        pass

    @functools.cache
    def signature(self):
        return str(
            Path(
                self.name,
                hashlib.sha256(get_source(self.__class__).encode()).hexdigest(),
            )
        )


def get_source(obj) -> str:
    """Return source text for ``obj`` even when running under IPython."""
    match in_jupyter_notebook():
        case True:
            from IPython.core import oinspect

            return oinspect.getsource(obj)
        case False:
            return inspect.getsource(obj)


def in_jupyter_notebook() -> bool:
    """Best-effort detection of IPython/Jupyter execution."""
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        return shell in {"ZMQInteractiveShell", "TerminalInteractiveShell"}
    except (NameError, ImportError):
        return False


def caching(
    func=None,
    *,
    debug: bool = True,
):
    """Decorator that caches emitted events per processor/event digest."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(
            self: AbstractProcessor, context: Context, event: Event, *args, **kwargs
        ):
            try:
                assert isinstance(self, AbstractProcessor)

                # Compute cache key from processor source and event
                cache_key = [
                    event,
                ]
                digest = deepdiff.DeepHash(cache_key)[cache_key]

                if debug:
                    logger.debug("digest %s", digest)

                archive = context.pipeline.archive(self.name)

                if digest in archive:
                    # Cache hit: replay archived events
                    if debug:
                        logger.debug("cache hit: %s", digest)
                    for cached_event in archive[digest]:
                        context.submit(cached_event)
                else:
                    # Cache miss: execute and cache results
                    func(self, context, event, *args, **kwargs)
                    archive[digest] = tuple(context.events)

            except Exception:
                logger.exception("caching wrapper failed")
                raise

        return wrapper

    return decorator if func is None else decorator(func)
