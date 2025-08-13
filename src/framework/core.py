import abc
import functools
import hashlib
import inspect
import logging
import queue
import threading
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from queue import Empty, Queue, SimpleQueue
from typing import List

import dill
import klepto
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)


class Event(BaseModel):
    """Immutable event passed through the runtime.

    Attributes:
        name: The logical name of the event (often used to refine interests).
    Notes:
        - model_config uses frozen=True so instances are immutable and hashable by value.
        - extra="allow" lets events carry additional dynamic fields as needed.
    """

    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


def wrap(func):
    """Middleware wrapper for processor callables.

    Wraps a callable to surface and log exceptions with thread context,
    without altering the return value or re-raising semantics.

    Args:
        func: The function to wrap (e.g., AbstractProcessor.process).

    Returns:
        A callable that delegates to func and logs exceptions at DEBUG level.

    Concurrency:
        - Intended to run inside worker threads managed by the Pipeline's shared executor.
          Per-processor ordering is enforced by Inbox slot gating (no reentrancy per instance).

    Error handling:
        - Exceptions are logged with traceback and re-raised unchanged.
    """

    @functools.wraps(func)
    def middleware(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.debug(
                f"exception calling {func.__qualname__} from thread {threading.current_thread().name}",
                exc_info=True,
            )
            raise

    return middleware


def retry(retry):
    """Decorator that retries a callable up to `retry` times.

    Args:
        retry: Number of attempts. Last failure is logged at error level and re-raised.

    Returns:
        A decorator that wraps the target function with retry logic.

    Concurrency:
        - No internal synchronization; suitable to be used in executor threads.

    Error handling:
        - Logs warnings for intermediate failures and an error on final failure, then re-raises.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retry):
                logger.debug(f"attempt {i + 1} / {retry} at {func.__qualname__}")
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if i >= retry - 1:
                        logger.error(
                            f"attempt {i + 1} / {retry} at {func.__qualname__} failed... escalating",
                            exc_info=True,
                        )
                        raise
                    else:
                        logger.warning(
                            f"attempt {i + 1} / {retry} at {func.__qualname__} failed... retrying",
                            exc_info=True,
                        )

        return wrapper

    return decorator_retry


class Context:
    """Execution context provided to processors while handling an event.

    Responsibilities:
        - Provide submit() API to emit follow-up events back into the pipeline.
        - Collect emitted events in-order for possible caching/replay.
        - Provide a done_callback used by the pipeline to handle accounting and errors.

    Concurrency:
        - Instances are created per dispatch; used within a worker thread managed by the shared executor.
          Emissions are thread-safe via Pipeline.submit.

    Error handling:
        - Exceptions in submit() are surfaced; done_callback logs processor failures.
    """

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
        context: "Context",
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
            if exc is not None:
                logger.error(
                    f"processor failed: {processor} on event {event}",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        finally:
            inbox.mark_task_done()
            self.pipeline.decrement()  # Ensure job accounting is balanced even on failure.
            # Wake the dispatcher to continue scheduling.
            q.put(True)


class Inbox:
    def __init__(
        self, processor: "AbstractProcessor", ready_queue: Queue, concurrency: int = 1
    ):
        self.processor = processor
        self.events: queue.SimpleQueue = queue.SimpleQueue()
        self.jobs = 0
        self.slots_left = concurrency
        self.lock = threading.RLock()
        self.ready_queue = ready_queue

    def put_event(self, event: Event):
        with self.lock:
            self.events.put(event)
            self.jobs += 1
            if (
                self.slots_left > 0
            ):  # we definitely has at least one job, check if we have any slots left
                self.slots_left -= 1
                self.ready_queue.put(self.processor)

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
            if (
                self.jobs > 0
            ):  # we definitely has at least one slot, check if we have any jobs left
                self.slots_left -= 1
                self.ready_queue.put(self.processor)


class Pipeline:
    """Event dispatch pipeline.

    Registers processors by inferred interests and dispatches incoming events to the appropriate
    recipients. Maintains global job accounting and provides an orderly shutdown protocol.

    Args:
        processors: List of processors whose interests are inferred and registered.
        strict_interest_inference: When True, disable wildcard delivery. Only explicit matches
            from interest inference receive events.
        workspace: Optional directory path for caching archives.

    Concurrency and ordering:
        - A single dispatcher thread drains the ready queue in arrival order.
        - Work executes on a shared ThreadPoolExecutor; per-processor single-concurrency and ordering
          are enforced by Inbox slot gating (no reentrancy per processor instance).
        - Global job accounting (jobs, cond) tracks all queued and executing tasks including
          nested emissions, enabling graceful shutdown.

    Shutdown:
        - A special "__POISON__" event is enqueued to terminate the dispatcher after all work drains.
    """

    def __init__(
        self,
        processors: List["AbstractProcessor"],
        strict_interest_inference=False,
        workspace=None,
    ):
        # Global job accounting guarded by a condition variable.
        self.jobs = 0
        self.cond = threading.Condition()
        self.rdyq = Queue()
        self.strict_interest_inference = strict_interest_inference

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

    def run(self):
        """Run the dispatcher loop until the queue drains, then shut down executors.

        Behavior:
            - Start a single-thread executor for the dispatcher.
            - Wait until global jobs drop to zero (all work including nested emissions) to reach quiescence.
            - Submit "__POISON__" so the dispatcher loop can observe termination.
            - Wait again for job counter to reach zero, ensuring poison handling and final tasks complete.
            - Executors exit via context managers upon loop termination.

        Notes:
            - A special "__POISON__" event is consumed by execute_events(), which breaks its loop.
            - Progress and termination are reported via the module logger.
        """
        with ThreadPoolExecutor(1) as executor:
            q = SimpleQueue()
            q.put(True)
            executor.submit(self.execute_events, q)
            # Wait for all outstanding jobs (including nested emissions) to complete.
            self.wait()
            logger.info("first stage processing done; proceeding to termination")
            # Submit poison-pill so the dispatcher can exit its blocking queue loop.
            self.register_processor(Terminator(q))
            self.submit(Event(name="__POISON__"))
            # Wake dispatcher to process the poison if it is currently waiting on q.get()
            # with an empty ready queue.
            q.put(True)
            # Wait until the poison event is processed and the dispatcher exits.
            self.wait()
            logger.info("dispatcher terminated")

        logger.info("pipeline run completed")

    def submit(self, event: Event):
        """Submit an event to the queue and increment job count."""
        self.increment()
        recipients = set()
        # Wildcard delivery is disabled when strict interest inference is requested.
        recipients |= (
            self.processors[(None, None)]
            if not self.strict_interest_inference
            else set()
        )
        # Deliver to handlers that match on Event class irrespective of name.
        recipients |= self.processors.get((event.__class__.__name__, None), set())
        # Deliver to the most specific class+name handlers.
        recipients |= self.processors.get((event.__class__.__name__, event.name), set())
        logger.debug("submitting %s", event)
        for processor in recipients:
            self.increment()
            inbox: Inbox = self.get_inbox(processor)
            inbox.put_event(event)
        self.decrement()

    def execute_events(self, q: SimpleQueue):
        with ThreadPoolExecutor(4) as executor:
            # Drive scheduling by pulses on q; drain all currently-ready processors per pulse.
            while q.get():
                try:
                    # Drain all ready processors without blocking on an empty ready queue.
                    while processor := self.rdyq.get_nowait():
                        # One of the processors indicated readiness (has an event and an available slot).
                        inbox: Inbox = self.get_inbox(processor)
                        event: Event = inbox.take_event()
                        context = Context(self)
                        future = executor.submit(processor.process, context, event)
                        future.add_done_callback(
                            functools.partial(
                                context.done_callback,
                                processor=processor,
                                context=context,
                                event=event,
                                inbox=inbox,
                                q=q,
                            )
                        )
                except Empty:
                    continue
                except Exception:
                    logger.critical(
                        "unexpected error in dispatcher loop", exc_info=True
                    )
                    raise

    def increment(self):
        """Increment global job counter."""
        with self.cond:
            self.jobs += 1

    def decrement(self):
        """Decrement global job counter and notify waiters when it reaches zero."""
        with self.cond:
            self.jobs -= 1
            if self.jobs <= 0:
                self.cond.notify_all()

    def wait(self):
        with self.cond:
            self.cond.wait_for(lambda: self.jobs <= 0)


def parse_pattern(p):
    """Extract an (event_class_id, event_name) pair from a match-case pattern AST.

    Recognized shapes:
        - ast.MatchClass with kwd_attrs including "name" and a constant pattern for it.
        - ast.MatchClass without a name constraint (treated as (ClassName, None)).
        - Nested AST nodes that wrap a pattern (e.g., ast.AST(pattern=...)).

    Returns:
        Tuple[str | None, str | None]: (ClassIdentifier, EventName) where either may be None.
        Returns (None, None) when the pattern cannot be interpreted.

    Limitations:
        - Only a subset of conventional patterns is recognized. Exotic or dynamic patterns
          may not be interpreted and will result in (None, None).
    """
    import ast

    def cls_id(n):
        # Extract a class identifier from Name or Attribute; otherwise return None.
        match n:
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
            cid = cls_id(cls)
            yield (cid, event_name) if cid else (None, None)
        case ast.MatchClass(cls=cls):
            cid = cls_id(cls)
            yield (cid, None) if cid else (None, None)
        case ast.MatchOr(patterns=pps):
            for pp in pps:
                yield from parse_pattern(pp)
        case ast.AST(pattern=pp):
            # Some wrappers carry the actual pattern in their `pattern` field.
            yield from parse_pattern(pp)
        case _:
            yield None, None


def infer_interests(func):
    """Infer processor interests from the AST of its process() function.

    Expectation:
        - A top-level match statement over the Event parameter with cases that match on
          Event class and optionally the 'name' attribute.

    Yields:
        Tuples of (event_class_id | None, event_name | None) for each case discovered.

    Behavior:
        - If no recognizable match-case is found, yields (None, None) and logs a warning.
        - If a case cannot be interpreted, logs a warning and yields (None, None) for that case.

    Limitations:
        - Unusual control-flow, guards, or patterns may not be recognized and will produce
          (None, None). In strict_interest_inference mode, such processors will not receive
          events via wildcard delivery.
    """
    # note that it is expected that processors will use a match-case clause as THE way to indicate interest.

    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    match tree:
        case ast.Module(body=[ast.FunctionDef(body=[*_, ast.Match() as m])]):
            for c in m.cases:
                for interest in parse_pattern(c.pattern):
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
                f"the processor {func} does not seem to have declared any interest to events;"
            )
            yield None, None


class AbstractProcessor(abc.ABC):
    """Base class for processors that handle events.

    Responsibilities:
        - Declare interests (inferred at construction from process()).
        - Provide an archive() utility to access the caching backend.

    Concurrency:
        - Per-processor single-concurrency and ordering are enforced by the Pipeline via Inbox gating.
          Processor instances do not own executors; work is executed on a shared thread pool.

    Notes:
        - interests is a frozen set of interest tuples inferred by infer_interests().
    """

    def __init__(self):
        self.interests = frozenset(infer_interests(self.process))

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        """Handle an incoming event and optionally emit follow-up events via context.submit().

        Ordering:
            - Invocations are serialized per processor instance.

        Error handling:
            - Exceptions propagate to the future; the pipeline logs them in done_callback.
        """
        pass

    @functools.cache
    def archive(self, workspace: Path):
        """Return the klepto archive for this processor instance.

        Args:
            workspace: The root directory for the archive, or None to disable persistence.

        Returns:
            A klepto archive object. Uses sql_archive per processor class name when workspace
            is provided; otherwise a null_archive.

        Notes:
            - The sql_archive backend persists to a sqlite file named after the class. It stores
              serialized values and is reused across runs when the same workspace is provided.
        """
        if workspace is None:
            return klepto.archives.null_archive()
        else:
            return klepto.archives.sql_archive(
                name=f"sqlite:///{workspace.joinpath(self.__class__.__name__)}.sqlite",
                cached=False,
                serialized=True,
            )


class Terminator(AbstractProcessor):
    def __init__(self, q: SimpleQueue):
        super().__init__()
        self.q = q

    def process(self, context, event):
        match event:
            case Event(name="__POISON__"):
                logger.debug("terminator processing %s", event)
                self.q.put(False)


def caching(
    func=None,
    *,
    debug: bool = False,
):
    """Decorator to cache and deterministically replay emitted events.

    Key:
        - Digest = sha256(dill.dumps([class_source, event_json])).
          Using the processor class' source ties the cache to the exact implementation,
          ensuring cache invalidation when code changes. Event JSON provides the input key.

    Behavior:
        - On cache hit: resubmit the archived sequence of emitted events to the pipeline.
        - On cache miss: run the function, collect context.events in order, and archive the tuple.

    Args:
        func: The function to decorate (processor.process). When omitted, returns a configured decorator.
        debug: If True, logs digest values and cache hits at DEBUG level.

    Concurrency:
        - Safe to use within the single-threaded processor executor; submissions are delegated
          to the pipeline which handles synchronization.

    Caveats:
        - Only emitted events are captured. External side effects are not recorded and will
          not be replayed. Emission order should be deterministic for meaningful replay.

    Archive backend:
        - Obtained via AbstractProcessor.archive(workspace) which uses a klepto sql_archive
          when a workspace directory is configured, keyed per processor class.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, context: Context, event: Event, *args, **kwargs):
            try:
                assert isinstance(self, AbstractProcessor)
                # Compute digest over processor class source + input event JSON.
                digest = hashlib.sha256(
                    dill.dumps(
                        [
                            inspect.getsource(self.__class__),
                            event.model_dump_json(),
                        ]
                    )
                ).hexdigest()
                if debug:
                    logger.debug("digest %s", digest)
                archive = self.archive(context.pipeline.workspace)
                if digest in archive:
                    # Cache hit: replay by resubmitting archived events in original order.
                    if debug:
                        logger.debug("cache hit: %s", digest)
                    for e in archive[digest]:
                        context.submit(e)
                else:
                    # Cache miss: execute and archive emitted events as an ordered tuple.
                    func(self, context, event, *args, **kwargs)
                    archive[digest] = tuple(context.events)
            except Exception:
                # Log failure details while preserving behavior.
                logger.exception("caching wrapper failed")
                raise

        return wrapper

    return decorator if func is None else decorator(func)
