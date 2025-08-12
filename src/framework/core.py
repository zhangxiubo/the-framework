"""Event-driven runtime core.

This module provides:
- An immutable Event model (pydantic BaseModel with frozen=True) as the lingua franca.
- A processor abstraction (AbstractProcessor) with a single-threaded executor per processor
  to avoid reentrancy and concurrent state mutation inside a processor.
- A Pipeline that dispatches events to processors based on "interests" inferred from the
  processor's match-case patterns over Event instances. Interests are keyed as:
    (None, None)                         -> wildcard/default interest
    (EventClassName, None)               -> any name for a given event class
    (EventClassName, "specific_name")    -> specific name for a given event class
- An optional caching decorator that records emitted events keyed by the processor class'
  source code + input event JSON to enable deterministic replay.


Interest inference:
- Processors typically use Python's match-case on the immutable Event object. This module inspects
  the AST of the processor's process function to infer which (event class, name) pairs a processor
  is interested in. Only common/standard match-case shapes are recognized (e.g., MatchClass with an
  optional 'name' kwd). Non-standard constructs, deeply dynamic patterns, or patterns that do not
  fit expected shapes will yield (None, None), and the pipeline may fall back to default delivery
  (unless strict interest inference is enabled).

Caching decorator:
- The caching decorator composes a digest from: [processor class source, event JSON].
- It stores the tuple of events emitted during processing and, on cache hit, replays them by
  resubmitting the events to the pipeline. dill is used to serialize the inputs (class source,
  event contents) to ensure a stable digest.
- Caveat: Only emitted events are cached/replayed. Any external side effects are not captured
  and therefore will not be replayed deterministically.

Light notes / TODOs:
- Extension points: support additional interest inference strategies beyond AST of match-case,
  allow configurable multi-threaded processors with explicit synchronization.
- Archive backend: the current workspace cache uses klepto's sql_archive persisted per
  processor class name; alternative backends could be made pluggable.
"""

import abc
import functools
import hashlib
import inspect
import itertools
import logging
import sys
import threading
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from queue import Empty, Queue
import time
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
        A callable that delegates to func and logs exceptions at debug level.

    Concurrency:
        - Intended to run inside executor threads. No ordering guarantees beyond the
          per-processor single-thread executor.

    Error handling:
        - Exceptions are logged with traceback and re-raised unchanged.
    """

    @functools.wraps(func)
    def middleware(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.debug(
                f"exception calling {func.__qualname__} from thread {threading.current_thread().name}",
                exc_info=True,
            )
            raise e

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
                except Exception as e:
                    if i >= retry - 1:
                        logger.error(
                            f"attempt {i + 1} / {retry} at {func.__qualname__} failed... escalating",
                            exc_info=True,
                        )
                        raise e
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
        - Instances are created per dispatch; used within a processor's executor thread.
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
        except Exception as e:
            print(e)
            raise e

    def done_callback(
        self,
        future: Future,
        processor: "AbstractProcessor",
        context: "Context",
        event: Event,
        semaphore: threading.Semaphore
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
            semaphore.release()
            self.pipeline.decrement()  # Ensure job accounting is balanced even on failure.


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
        - A single dispatcher thread drains the queue in arrival order.
        - Each processor has its own single-thread executor, guaranteeing per-processor ordering
          and preventing reentrancy.
        - Global job accounting (jobs, cond) tracks all queued and executing tasks including
          nested emissions, enabling graceful shutdown.

    Shutdown:
        - A special POISON event is enqueued to terminate the dispatcher after all work drains.
    """

    def __init__(
        self,
        processors: List["AbstractProcessor"],
        strict_interest_inference=False,
        workspace=None,
    ):
        self.processors = defaultdict(set)
        for processor in processors:
            # Register every declared interest for this processor.
            for interest in processor.interests:
                logger.debug(f"registering interest for {processor}: {interest}")
                self.processors[interest].add(processor)

        # Global job accounting guarded by a condition variable.
        self.jobs = 0
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.POISON = Event(
            name="__POISON__"
        )  # Poison-pill sentinel used to stop the dispatcher.
        self.strict_interest_inference = strict_interest_inference
        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)
        self.inboxes = defaultdict(lambda: (Queue(), threading.Semaphore(1)))

    def run(self):
        """Run the dispatcher loop until the queue drains, then shut down executors.

        Behavior:
            - Start a single-thread executor for the dispatcher.
            - Wait until global jobs drop to zero (all work including nested emissions done).
            - Submit POISON to allow the dispatcher loop to observe termination.
            - Wait again for job counter to reach zero, ensuring POISON handling and final tasks complete.
            - Finally, shut down all per-processor executors.

        Notes:
            - POISON is used as a clean termination signal read by dispatch_events(), which breaks its loop.
        """
        with ThreadPoolExecutor(1) as executor:
            # executor.submit(self.dispatch_events)
            stop = threading.Event()
            executor.submit(self.execute_events, stop)
            # Wait for all outstanding jobs (including nested emissions) to complete.
            with self.cond:
                while self.jobs > 0:
                    self.cond.wait()
            # Submit poison-pill so the dispatcher can exit its blocking queue loop.
            self.submit(self.POISON)
            print('queued poison')
            stop.set()
            # Wait until the poison event is processed and the dispatcher exits.
            with self.cond:
                while self.jobs > 0:
                    self.cond.wait()
        print('exited main executor')

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
        print('submitting', event)
        for processor in recipients:
            self.increment()
            inbox, semaphore = self.inboxes[processor]
            inbox.put(event)
        self.decrement()


    def execute_events(self, stop: threading.Event):
        with ThreadPoolExecutor(4) as executor:
            while not stop.is_set():
                for processor, (inbox, semaphore) in self.inboxes.items():
                    processor: AbstractProcessor
                    inbox: Queue
                    semaphore: threading.Semaphore
                    if semaphore.acquire(blocking=False):
                        event = None
                        try:
                            event = inbox.get(block=False)
                            print(event)
                            context = Context(self)
                            future = executor.submit(processor.process, context, event)
                            future.add_done_callback(
                                functools.partial(
                                    context.done_callback,
                                    processor=processor,
                                    context=context,
                                    event=event,
                                    semaphore=semaphore
                                )
                            )
                        except Empty:
                            semaphore.release()

    def increment(self):
        """Increment global job counter."""
        with self.cond:
            self.jobs += 1
            print('jobs increased:', self.jobs)

    def decrement(self):
        """Decrement global job counter and notify waiters when it reaches zero."""
        with self.cond:
            self.jobs -= 1
            print('jobs decreased:', self.jobs)
            if self.jobs <= 0:
                self.cond.notify_all()


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
            yield parse_pattern(pp)
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
        - Execute work in a single-threaded executor to maintain per-processor ordering.
        - Provide an archive() utility to access the caching backend.

    Concurrency:
        - Each instance owns a ThreadPoolExecutor(max_workers=1). This prevents concurrent
          execution of process() within the same processor instance.

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
        debug: If True, prints digest values and cache hits to stdout.

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
                    print("digest", digest)
                archive = self.archive(context.pipeline.workspace)
                if digest in archive:
                    # Cache hit: replay by resubmitting archived events in original order.
                    if debug:
                        print("cache hit:", digest)
                    for e in archive[digest]:
                        context.submit(e)
                else:
                    # Cache miss: execute and archive emitted events as an ordered tuple.
                    func(self, context, event, *args, **kwargs)
                    archive[digest] = tuple(context.events)
            except Exception as e:
                # Surface failure details to stderr while preserving behavior.
                print(type(e), e, file=sys.stderr)
                raise e

        return wrapper

    return decorator if func is None else decorator(func)
