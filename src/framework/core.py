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
from collections import defaultdict, UserDict
from concurrent.futures import Future, ThreadPoolExecutor, CancelledError
from contextlib import contextmanager, AbstractContextManager
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, PriorityQueue, SimpleQueue
from typing import List, Optional
import signal
import sys
import blinker

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
        q: SimpleQueue,
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
            # Wake the dispatcher to continue scheduling.
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
        ready_queue: PriorityQueue[ProcessEntry],
        concurrency: int = 1,
    ):
        self.processor = processor
        self.events: SimpleQueue = SimpleQueue()
        self.jobs = 0
        self.slots_left = concurrency
        self.lock = threading.RLock()
        self.ready_queue = ready_queue

    def put_event(self, event: Event):
        """Queue an event and, if capacity exists, signal the dispatcher."""
        with self.lock:
            self.events.put(event)
            self.jobs += 1
            if self.slots_left > 0:  # Check if we have any slots left
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )

    def take_event(self):
        """Return the next event without blocking; raise if the inbox is empty."""
        try:
            event = self.events.get(block=False)
            with self.lock:
                self.jobs -= 1
                return event
        except Empty:
            msg = f"taking from empty inbox from {self.processor}; this should not have happened"
            logger.critical(msg)
            raise RuntimeError(msg)

    def mark_task_done(self):
        """Release a slot and re-signal when pending events remain."""
        with self.lock:
            self.slots_left += 1
            if self.jobs > 0:  # Check if we have any jobs left
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )


interrupted = blinker.signal("interrupted")


def chain_signal_handler(sig, new_handler):
    """Install ``new_handler`` while preserving the previous handler."""
    old_handler = signal.getsignal(sig)

    @functools.wraps(new_handler)
    def chained_handler(*args, **kwargs):
        new_handler(*args, **kwargs)
        if callable(old_handler):
            old_handler(*args, **kwargs)

    signal.signal(sig, chained_handler)


def pipeline_handler(signum, frame):
    """Relay a SIGINT into the ``interrupted`` blinker signal."""
    logging.info("propagating executor shutdown events...")
    try:
        interrupted.send()
    except Exception as e:
        print(e, file=sys.stderr)
        raise e


chain_signal_handler(signal.SIGINT, pipeline_handler)


class InterruptHandler(AbstractContextManager):
    """Context manager that cancels/joins executors when SIGINT is raised."""

    def __init__(self, executor: ThreadPoolExecutor):
        self.executor: ThreadPoolExecutor = executor

    def handler(self, sender):
        logger.info("shutting down task executor...")
        self.executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self):
        interrupted.connect(self.handler)
        return self

    def __exit__(self, typ, value, traceback):
        interrupted.disconnect(self.handler)
        return False


class Pipeline:
    """Dispatcher that routes events to processors based on declared interests.

    Scheduling overview (job counting + dispatch coordination):

    * Every call to :meth:`submit` increments ``jobs`` once, then increments once
      per recipient. Each processor inbox decrements when it finishes a task by
      calling :meth:`Context.done_callback`, which in turn calls
      :meth:`Pipeline.decrement`. The ``jobs`` counter therefore represents the
      total amount of work currently enqueued (including nested emissions).
    * ``done`` tracks completions. When ``done >= jobs`` the dispatcher knows the
      phase is quiescent and :meth:`wait` returns to the caller (either
      :meth:`run` or tests invoking ``run_dispatcher_once``).
    * ``q`` is a simple queue used as a pulse line: pushing ``True`` wakes the
      dispatcher loop, which drains ``rdyq`` (the priority queue of processors
      that have available slots). ``False`` stops the dispatcher thread.
    * Each :class:`Inbox` nominates itself by pushing a :class:`ProcessEntry`
      into ``rdyq`` whenever it has work *and* slots available. Because per
      processor concurrency is typically 1, this keeps processors single-threaded.
    * When the dispatcher drains ``rdyq`` it immediately submits work to the
      shared :class:`ThreadPoolExecutor`. Completion callbacks mark the inbox
      task done, decrement the pipeline counter, and push another pulse to ``q`` so
      additional processors can be scheduled without waiting for the next phase.

    The result is a feedback loop that never loses track of work: every enqueue
    increments ``jobs``; every completion increments ``done``; ``wait`` only
    returns when the two match; and pulses keep the executor busy without busy
    polling.
    """

    def __init__(
        self,
        processors: List["AbstractProcessor"],
        strict_interest_inference=False,
        workspace: Optional[str | Path] = None,
        max_workers=None,
        config=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.jobs = 0
        self.done = 0
        self.cond = threading.Condition()
        self.rdyq = PriorityQueue()
        self.q = SimpleQueue()
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

    @functools.cache
    def get_inbox(self, processor: "AbstractProcessor"):
        """Return (and memoize) the inbox dedicated to ``processor``."""
        return Inbox(processor, self.rdyq, 1)

    def register_processor(self, processor: "AbstractProcessor"):
        """Register every declared interest emitted by ``infer_interests``."""
        # Register every declared interest for this processor.
        for interest in processor.interests:
            logger.debug(f"registering interest for {processor}: {interest}")
            self.processors[interest].add(processor)

    def run(self):
        """Drive the dispatcher until all work (and phase-driven work) has completed."""
        try:
            with (
                ThreadPoolExecutor(1) as main_executor,
            ):
                q = self.q

                aborting = threading.Event()
                with self.cond:
                    last_jobs = self.jobs
                    aborting.clear()

                main_executor.submit(self.execute_events, q)

                phase = 0
                idle_jobs_increment = 0
                while not aborting.is_set():
                    logger.info(f"starting phase: {phase:02d}")
                    start = time.perf_counter()
                    q.put(True)
                    # Wait for all outstanding jobs (including nested emissions) to complete.
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

                    # let the current task executor finish, then immediately launch a task executor for the next phase
                    q.put(False)
                    main_executor.submit(self.execute_events, q)

                    idle_jobs_increment = (
                        self.submit(Event(name="__PHASE__", phase=phase)) + 1
                    )  # the expected increment in jobs count when no other jobs are launched by the processors.
                else:
                    q.put(True)
                    self.wait()  # wait for the phasing-spawned events to finish

                    # final phase
                    logger.info("processing done; proceeding to shutdown phase")

                    q.put(False)
                    main_executor.submit(self.execute_events, q)

                    # Submit poison-pill so the dispatcher can exit its blocking queue loop.
                    self.submit(Event(name="__POISON__"))

                    # Wake dispatcher to process the poison if it is currently waiting on q.get()
                    # with an empty ready queue.
                    q.put(True)

                    # Wait until the poison event is processed and the dispatcher exits.
                    self.wait()

                    q.put(False)

                    logger.info("dispatcher terminated")
        finally:
            for suffix, archive in self.archives:
                logger.info(f"closing archive for archive: {suffix}")
                print(f"closing archive for archive: {suffix}")
                archive.close()

            logger.info("pipeline run completed")

    def submit(self, event: Event):
        """Submit an event to the queue and increment job count."""
        recipients = set()
        try:
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
            self.decrement()
            return len(recipients)
        finally:
            if recipients:
                self.q.put(
                    True
                )  # force a tick; otherwise the pipeline will only tick upon task completion

    def execute_events(self, q: SimpleQueue):
        """Drain the ready queue whenever ``q`` signals a scheduling pulse."""
        with ThreadPoolExecutor(self.max_workers) as executor:
            with InterruptHandler(
                executor,
            ):
                # Drive scheduling by pulses on q; drain all currently-ready processors per pulse.
                while q.get():
                    try:
                        # Drain all ready processors without blocking on an empty ready queue.
                        while entry := self.rdyq.get_nowait():
                            processor = entry.processor
                            # One of the processors indicated readiness (has an event and an available slot).
                            inbox: Inbox = self.get_inbox(processor)
                            event: Event = inbox.take_event()
                            context = Context(self)
                            callback = functools.partial(
                                context.done_callback,
                                processor=processor,
                                event=event,
                                inbox=inbox,
                                q=q,
                            )
                            future = executor.submit(processor.process, context, event)
                            future.add_done_callback(callback)
                    except Empty:
                        continue
                    except Exception as e:
                        f = Future()
                        f.set_exception(e)
                        callback(
                            f
                        )  # manually invoke the call back function to ensure integrity of job counting

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
                pass

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

    @functools.cache
    def archive(self, suffix: Optional[str], readonly=False):
        """Return a cache/rocksdb archive keyed by ``suffix`` (memoized per suffix)."""
        if self.workspace is None:
            archive = InMemCache()
        else:
            path = self.workspace.joinpath(suffix)
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
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
            except Exception as e:
                raise e
        self.archives.append((suffix, archive))
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
