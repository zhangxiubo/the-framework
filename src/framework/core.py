import abc
import functools
import hashlib
import inspect
import logging
import threading
import time
from collections import defaultdict, UserDict
from concurrent.futures import Future, ThreadPoolExecutor, CancelledError
from contextlib import contextmanager
from dataclasses import dataclass, field
from linecache import cache
from pathlib import Path
from queue import Empty, PriorityQueue, Queue, SimpleQueue
from typing import List, Optional

import deepdiff
import dill
from sqlitedict import SqliteDict

logger = logging.getLogger(__name__)


class Event:
    def __init__(self, name: str, **kwargs):
        self.name: str = name
        self.__dict__.update(kwargs)


class DummyCache(UserDict):

    def close(self):
        self.clear()

@contextmanager
def timeit(name, logger: logging.Logger):
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        logger.debug(f"{name} finished after {end - start:.4f} seconds")


def retry(max_attempts):
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
    def __init__(self, pipeline):
        self.pipeline: Pipeline = pipeline
        self.events = []

    def submit(self, event: Event):
        """Emit a new event back into the pipeline and record it for caching."""
        try:
            self.events.append(
                event
            )  # Maintain emission order for deterministic replay.
            self.pipeline.submit(event)
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
        except CancelledError as e:
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
    priority: int
    processor: "AbstractProcessor" = field(
        compare=False,
    )


class Inbox:
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
        with self.lock:
            self.events.put(event)
            self.jobs += 1
            if self.slots_left > 0:  # Check if we have any slots left
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )

    def take_event(self):
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
        with self.lock:
            self.slots_left += 1
            if self.jobs > 0:  # Check if we have any jobs left
                self.slots_left -= 1
                self.ready_queue.put(
                    ProcessEntry(self.processor.priority, self.processor)
                )


class Pipeline:
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
        self.strict_interest_inference = strict_interest_inference
        self.max_workers = len(processors) if max_workers is None else max_workers
        self.config = dict() if config is None else config.copy()

        self.processors = defaultdict(set)
        for processor in processors:
            self.register_processor(processor)

        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

    @functools.cache
    def get_inbox(self, processor: "AbstractProcessor"):
        return Inbox(processor, self.rdyq, 1)

    def register_processor(self, processor: "AbstractProcessor"):
        # Register every declared interest for this processor.
        for interest in processor.interests:
            logger.debug(f"registering interest for {processor}: {interest}")
            self.processors[interest].add(processor)

    @contextmanager
    def interrupt_handled(
        self,
        executor: ThreadPoolExecutor,
    ):
        import signal
        import sys

        old_handler = signal.getsignal(signal.SIGINT)

        def handler(signum, frame):
            print("main pipeline executor shutting down...", file=sys.stderr)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            executor.shutdown(cancel_futures=True, wait=False)

        signal.signal(signal.SIGINT, handler)
        try:
            yield
        finally:
            signal.signal(signal.SIGINT, old_handler)

    def run(self):
        with (
            ThreadPoolExecutor(1) as main_executor,
            ThreadPoolExecutor(self.max_workers) as task_executor,
        ):
            q = SimpleQueue()
            with self.interrupt_handled(task_executor):
                stop = threading.Event()
                with self.cond:
                    last_jobs = self.jobs
                    stop.clear()

                main_executor.submit(self.execute_events, task_executor, q)

                phase = 0
                noop_diff = 0
                while not stop.is_set():
                    print("phase:", phase)
                    q.put(True)
                    # Wait for all outstanding jobs (including nested emissions) to complete.
                    self.wait()

                    with self.cond:
                        if self.jobs == (last_jobs + noop_diff):
                            stop.set()
                        last_jobs = self.jobs

                    phase += 1
                    noop_diff = self.submit(Event(name="__PHASE__", phase=phase)) + 1
                else:
                    q.put(True)
                    self.wait()  # wait for the phasing-spawned events to finish

                    # final phase
                    logger.info("processing done; proceeding to shutdown phase")

                    # Submit poison-pill so the dispatcher can exit its blocking queue loop.
                    self.submit(Event(name="__POISON__"))

                    # Wake dispatcher to process the poison if it is currently waiting on q.get()
                    # with an empty ready queue.
                    q.put(True)

                    # Wait until the poison event is processed and the dispatcher exits.
                    self.wait()

                    q.put(False)

                    logger.info("dispatcher terminated")

        logger.info("pipeline run completed")

    def submit(self, event: Event):
        """Submit an event to the queue and increment job count."""
        self.increment()
        recipients = set()

        # Collect all relevant recipients
        if not self.strict_interest_inference:
            recipients |= self.processors.get((None, None), set())

        event_class = event.__class__.__name__
        recipients |= self.processors.get((event_class, None), set())
        recipients |= self.processors.get((event_class, event.name), set())

        for processor in recipients:
            self.increment()
            inbox = self.get_inbox(processor)
            inbox.put_event(event)
        self.decrement()
        return len(recipients)

    def execute_events(self, executor: ThreadPoolExecutor, q: SimpleQueue):
        with executor:
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
        with self.cond:
            self.jobs += 1

    def decrement(self):
        with self.cond:
            self.done += 1
            if self.done >= self.jobs:
                self.cond.notify_all()

    def wait(self):
        with self.cond:
            self.cond.wait_for(lambda: self.done >= self.jobs)


def parse_pattern(p):
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
    def __init__(self, priority: int = 0):
        self.interests = frozenset(infer_interests(self.process))
        self.priority = priority

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        pass

    @functools.cache  # so that we return the same cache/store/archive object even if workspace is None
    def archive(self, workspace: Path, suffix: Optional[str] = None, read_only: bool = False):
        match workspace:
            case None:
                return DummyCache()
            case _:
                return self._archive(workspace, suffix, read_only)

    @classmethod
    @functools.cache
    def _archive(
        cls, workspace: Path, suffix: Optional[str] = None, read_only: bool = False
    ):
        assert workspace is not None
        if suffix is None:
            source_hash = hashlib.sha256(get_source(cls).encode()).hexdigest()
            path = workspace.joinpath(cls.__name__)
            path.mkdir(parents=True, exist_ok=True)
            path = path.joinpath(source_hash)
        else:
            path = workspace.joinpath(suffix)
        return SqliteDict(
            filename=f"{path}.sqlite",
            encode=dill.dumps,
            decode=dill.loads,
            autocommit=True,
            journal_mode="WAL",
            flag='r' if read_only else 'c',
        )


def get_source(obj) -> str:
    match in_jupyter_notebook():
        case True:
            from IPython.core import oinspect

            return oinspect.getsource(obj)
        case False:
            return inspect.getsource(obj)


def in_jupyter_notebook() -> bool:
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

                archive = self.archive(context.pipeline.workspace)

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
