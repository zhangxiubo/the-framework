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
from queue import Queue
from typing import List

import dill
import klepto
from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)


class Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


def wrap(func):
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
    def __init__(self, pipeline):
        self.pipeline: Pipeline = pipeline
        self.events = []

    def submit(self, event: Event):
        try:
            self.events.append(event)
            self.pipeline.submit(event)
        except Exception as e:
            print(e)
            raise e

    def done_callback(self, future: Future, processor: "AbstractProcessor", context: "Context", event: Event):
        try:
            exc = future.exception()
            if exc is not None:
                logger.error(
                    f"processor failed: {processor} on event {event}",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        finally:
            self.pipeline.decrement()


class Pipeline:
    def __init__(self, processors: List["AbstractProcessor"], strict_interest_inference=False, workspace=None):
        self.queue = Queue()
        self.processors = defaultdict(set)
        for processor in processors:
            for interest in processor.interests:
                logger.info(f"registering interest for {processor}: {interest}")
                print(f"registering interest for {processor}: {interest}")
                self.processors[interest].add(processor)

        self.jobs = 0
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.POISON = Event(name="__POISON__")
        self.strict_interest_inference = strict_interest_inference
        self.workspace = None if workspace is None else Path(workspace)
        if self.workspace is not None:
            self.workspace.mkdir(parents=True, exist_ok=True)

    def run(self):
        with ThreadPoolExecutor(1) as executor:
            executor.submit(self.dispatch_events)
            with self.cond:
                while self.jobs > 0:
                    self.cond.wait()
            self.submit(self.POISON)
            with self.cond:
                while self.jobs > 0:
                    self.cond.wait()
        # After dispatcher drained and all jobs completed, shutdown processor executors
        for proc in set(itertools.chain.from_iterable(self.processors.values())):
            try:
                proc.executor.shutdown(wait=True)
            except Exception:
                logger.error("error shutting down executor for %r", proc, exc_info=True)

    def submit(self, event: Event):
        self.increment()
        self.queue.put(event)

    def dispatch_events(self):
        while event := self.queue.get():
            try:
                recipients = set()
                recipients |= self.processors[(None, None)] if not self.strict_interest_inference else set()
                recipients |= self.processors.get((event.__class__.__name__, None), set())
                recipients |= self.processors.get((event.__class__.__name__, event.name), set())
                for processor in recipients:
                    context = Context(self)
                    try:
                        future = processor.executor.submit(
                            wrap(processor.process),
                            context,
                            event,
                        )
                        self.increment()
                        future.add_done_callback(
                            functools.partial(
                                context.done_callback,
                                processor=processor,
                                context=context,
                                event=event,
                            )
                        )
                    except Exception:
                        logger.error("failed to submit event to %r", processor, exc_info=True)
                if event is self.POISON:
                    break
            except Exception as e:
                logger.error(e, exc_info=True)
            finally:
                self.decrement()

    def increment(self):
        with self.cond:
            self.jobs += 1

    def decrement(self):
        with self.cond:
            self.jobs -= 1
            if self.jobs <= 0:
                self.cond.notify_all()


def parse_pattern(p):
    import ast

    def cls_id(n):
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
            return (cid, event_name) if cid else (None, None)
        case ast.MatchClass(cls=cls):
            cid = cls_id(cls)
            return (cid, None) if cid else (None, None)
        case ast.AST(pattern=pp):
            return parse_pattern(pp)
        case _:
            return None, None


def infer_interests(func):
    # note that it is expected that processors will use a match-case clause as THE way to indicate interest.

    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    match tree:
        case ast.Module(body=[ast.FunctionDef(body=[*_, ast.Match() as m])]):
            for c in m.cases:
                match interest := parse_pattern(c.pattern):
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
    def __init__(self):
        self.interests = frozenset(infer_interests(self.process))
        self.executor = ThreadPoolExecutor(1)

    @abc.abstractmethod
    def process(self, context: Context, event: Event):
        pass

    @functools.cache
    def archive(self, workspace: Path):
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
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, context: Context, event: Event, *args, **kwargs):
            try:
                assert isinstance(self, AbstractProcessor)
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
                    if debug:
                        print("cache hit:", digest)
                    for e in archive[digest]:
                        context.submit(e)
                else:
                    func(self, context, event, *args, **kwargs)
                    archive[digest] = tuple(context.events)
            except Exception as e:
                print(type(e), e, file=sys.stderr)
                raise e

        return wrapper

    return decorator if func is None else decorator(func)


