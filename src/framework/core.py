"""Core runtime primitives for the framework event pipeline."""

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

logger = logging.getLogger(__name__)

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
    def __init__(self, name: str, **kwargs: Any) -> None:
        self.name = name
        self.__dict__.update(kwargs)


class InMemCache(UserDict):
    def close(self) -> None:
        self.clear()

    def flush_wal(self, *args: Any, **kwargs: Any) -> None:
        return None

    def flush(self, *args: Any, **kwargs: Any) -> None:
        return None


class Context:
    def __init__(self, pipeline: "Pipeline") -> None:
        self.pipeline = pipeline
        self.events: list[Event] = []

    def submit(self, event: Event) -> int:
        try:
            self.events.append(event)
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
        signal: Optional[threading.Event] = None,
    ) -> None:
        try:
            exc = future.exception()
            shutdown_error = isinstance(exc, RuntimeError) and (
                str(exc) == "cannot schedule new futures after shutdown"
            )
            if exc is not None and not shutdown_error:
                logger.error(
                    "processor failed: %s on event %s, name: %s",
                    processor,
                    event.__class__.__name__,
                    event.name,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        except CancelledError:
            logger.info(
                "processor cancelled: %s on event %s, name: %s",
                processor,
                event.__class__.__name__,
                event.name,
            )
        finally:
            inbox.mark_task_done()
            self.pipeline.decrement()
            if signal is not None:
                signal.set()
            if q is not None:
                q.put(True)


@dataclass(order=True, frozen=True)
class ProcessEntry:
    priority: int
    processor: "AbstractProcessor" = field(compare=False)


class Inbox:
    """Single-processor mailbox that enforces at most one in-flight task."""

    def __init__(
        self,
        processor: "AbstractProcessor",
        ready_queue: PriorityQueue[ProcessEntry],
        ready_signal: Optional[threading.Event] = None,
    ) -> None:
        self.processor = processor
        self.ready_queue = ready_queue
        self.ready_signal = ready_signal
        self.events: SimpleQueue[Event] = SimpleQueue()
        self.pending = 0
        self.active = False
        self.lock = threading.RLock()

    def signal_ready(self) -> None:
        self.ready_queue.put(ProcessEntry(self.processor.priority, self.processor))
        if self.ready_signal is not None:
            self.ready_signal.set()

    def put_event(self, event: Event) -> None:
        should_signal = False
        with self.lock:
            self.events.put(event)
            self.pending += 1
            if not self.active:
                self.active = True
                should_signal = True
        if should_signal:
            self.signal_ready()

    def take_event(self) -> Event:
        try:
            event = self.events.get(block=False)
        except Empty:
            msg = f"taking from empty inbox from {self.processor}; this should not have happened"
            logger.critical(msg)
            raise RuntimeError(msg)
        with self.lock:
            self.pending -= 1
        return event

    def mark_task_done(self) -> None:
        should_signal = False
        with self.lock:
            if self.pending > 0:
                should_signal = True
            else:
                self.active = False
        if should_signal:
            self.signal_ready()


class Pipeline:
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
        self.jobs = 0
        self.done = 0
        self.cond = threading.Condition()

        self.rdyq: PriorityQueue[ProcessEntry] = PriorityQueue()
        self.q: SimpleQueue[bool] = SimpleQueue()
        self.ready_signal = threading.Event()

        self.inbox_lock = threading.Lock()
        self.archive_lock = threading.Lock()
        self.inboxes: dict[AbstractProcessor, Inbox] = {}
        self.archives_by_key: dict[tuple[Optional[str], bool], Any] = {}

        self.strict_interest_inference = strict_interest_inference
        self.max_workers = len(processors) if max_workers is None else max_workers
        self.config = {} if config is None else config.copy()

        self.processors: defaultdict[tuple[Optional[str], Optional[str]], set[Any]] = defaultdict(set)
        for processor in processors:
            self.register_processor(processor)

        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

        self.submit(Event(name="__INIT__"))

    def get_inbox(self, processor: "AbstractProcessor") -> Inbox:
        with self.inbox_lock:
            inbox = self.inboxes.get(processor)
            if inbox is None:
                inbox = Inbox(processor, self.rdyq, self.ready_signal)
                self.inboxes[processor] = inbox
            return inbox

    def register_processor(self, processor: "AbstractProcessor") -> None:
        for interest in processor.interests:
            logger.debug("registering interest for %s: %s", processor, interest)
            self.processors[interest].add(processor)

    def run(self) -> None:
        executor = ThreadPoolExecutor(self.max_workers)
        dispatcher = threading.Thread(target=self.execute_events, args=(self.q, executor), daemon=True)
        dispatcher.start()
        try:
            self.run_phases()
        except KeyboardInterrupt:
            self.abort_waiters()
            logger.info("pipeline interrupted; proceeding to shutdown")
        finally:
            self.q.put(False)
            dispatcher.join(timeout=5)
            executor.shutdown(wait=True, cancel_futures=True)
            self.close_archives()
            logger.info("pipeline run completed")

    def run_phases(self) -> None:
        phase = 0
        idle_jobs_increment = 0
        last_jobs = self.jobs_snapshot()
        while True:
            logger.info("starting phase: %02d", phase)
            started = time.perf_counter()
            self.pulse_dispatcher()
            self.wait()
            logger.info(
                "ending phase: %02d; elapsed time: %.4f seconds",
                phase,
                time.perf_counter() - started,
            )

            jobs = self.jobs_snapshot()
            should_abort = jobs <= (last_jobs + idle_jobs_increment)
            last_jobs = jobs
            phase += 1
            idle_jobs_increment = self.submit(Event(name="__PHASE__", phase=phase)) + 1
            if should_abort:
                break

        self.pulse_dispatcher()
        self.wait()
        logger.info("processing done; proceeding to shutdown phase")
        self.submit(Event(name="__POISON__"))
        self.pulse_dispatcher()
        self.wait()
        logger.info("dispatcher terminated")

    def jobs_snapshot(self) -> int:
        with self.cond:
            return self.jobs

    def abort_waiters(self) -> None:
        with self.cond:
            self.done = self.jobs
            self.cond.notify_all()

    def close_archives(self) -> None:
        for (suffix, readonly), archive in self.archives_by_key.items():
            logger.info("closing archive for archive: %s", suffix)
            try:
                archive.close()
            except Exception:
                logger.exception("failed to close archive: %s (readonly=%s)", suffix, readonly)

    def recipients_for_event(self, event: Event) -> set["AbstractProcessor"]:
        recipients: set[AbstractProcessor] = set()
        if not self.strict_interest_inference:
            recipients |= self.processors.get((None, None), set())
        for event_class in self.event_class_names(event):
            recipients |= self.processors.get((event_class, None), set())
            recipients |= self.processors.get((event_class, event.name), set())
        return recipients

    def submit(self, event: Event) -> int:
        recipients = self.recipients_for_event(event)
        if not recipients:
            return 0

        self.increment(len(recipients))
        for processor in recipients:
            self.get_inbox(processor).put_event(event)
        self.pulse_dispatcher()
        return len(recipients)

    def pulse_dispatcher(self) -> None:
        self.q.put(True)

    def execute_events(
        self, q: SimpleQueue, executor: Optional[ThreadPoolExecutor] = None
    ) -> None:
        if executor is None:
            with ThreadPoolExecutor(self.max_workers) as owned_executor:
                self.execute_events(q, owned_executor)
            return

        while q.get():
            self.dispatch_until_idle(executor)

    def dispatch_until_idle(self, executor: ThreadPoolExecutor) -> None:
        while True:
            entry = self.next_ready_entry()
            if entry is None:
                if self.is_idle():
                    return
                self.ready_signal.wait(timeout=0.05)
                self.ready_signal.clear()
                continue

            self.dispatch_entry(entry, executor)

    def next_ready_entry(self) -> Optional[ProcessEntry]:
        try:
            return self.rdyq.get_nowait()
        except Empty:
            return None

    def dispatch_entry(
        self,
        entry: ProcessEntry,
        executor: ThreadPoolExecutor,
        q: Optional[SimpleQueue] = None,
    ) -> None:
        processor = entry.processor
        inbox = self.get_inbox(processor)
        try:
            event = inbox.take_event()
        except Exception:
            logger.exception("failed to take event for %s", processor)
            try:
                inbox.mark_task_done()
            finally:
                self.decrement()
            return

        context = Context(self)
        callback = functools.partial(
            context.done_callback,
            processor=processor,
            event=event,
            inbox=inbox,
            q=q,
            signal=self.ready_signal,
        )

        try:
            future = executor.submit(processor.process, context, event)
            future.add_done_callback(callback)
        except Exception as exc:
            failure = Future()
            failure.set_exception(exc)
            callback(failure)

    def increment(self, by: int = 1) -> None:
        with self.cond:
            self.jobs += by

    def decrement(self) -> None:
        with self.cond:
            self.done += 1
            if self.done >= self.jobs:
                self.cond.notify_all()

    def is_idle(self) -> bool:
        with self.cond:
            return self.done >= self.jobs

    def wait(self) -> None:
        with self.cond:
            try:
                self.cond.wait_for(lambda: self.done >= self.jobs)
            except KeyboardInterrupt:
                self.done = self.jobs
                self.cond.notify_all()
                raise

    @staticmethod
    def event_class_names(event: Event) -> list[str]:
        names = [
            cls.__name__
            for cls in type(event).mro()
            if issubclass(cls, Event) and cls is not object
        ]
        return names or [event.__class__.__name__]

    def archive(self, suffix: Optional[str], readonly: bool = False):
        normalized = "default" if self.workspace is not None and suffix is None else suffix
        key = (normalized, bool(readonly))
        with self.archive_lock:
            archive = self.archives_by_key.get(key)
            if archive is None:
                archive = self.open_archive(normalized, readonly)
                self.archives_by_key[key] = archive
            return archive

    def open_archive(self, suffix: Optional[str], readonly: bool):
        if self.workspace is None:
            return InMemCache()
        path = self.workspace.joinpath(suffix)
        path.parent.mkdir(parents=True, exist_ok=True)
        archive = Rdict(
            str(path),
            access_type=AccessType.read_only() if readonly else AccessType.read_write(),
        )
        archive.set_dumps(dill.dumps)
        archive.set_loads(dill.loads)
        write_options = WriteOptions()
        write_options.sync = True
        archive.set_write_options(write_opt=write_options)
        return archive


class AbstractProcessor(abc.ABC):
    def __init__(self, name: Optional[str] = None, priority: int = 0) -> None:
        self.interests = frozenset(infer_interests(self.process))
        self.name = self.__class__.__name__ if name is None else name
        self.priority = priority

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        raise NotImplementedError

    @functools.cache
    def signature(self) -> str:
        return str(
            Path(
                self.name,
                hashlib.sha256(get_source(self.__class__).encode()).hexdigest(),
            )
        )
