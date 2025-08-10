import abc
from collections import defaultdict
import functools
import hashlib
import inspect
import itertools
import logging
from pathlib import Path
import random
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from os import PathLike
from queue import Queue
from typing import Any, Collection, Dict, List, Set, Optional, Union

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
        self.events = list()

    def submit(self, event: Event):
        try:
            self.events.append(event)
            self.pipeline.submit(event)
        except Exception as e:
            print(e)
            raise e

    def done_callback(self, future: Future, processor: 'AbstractProcessor', context: 'Context', event: Event):
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
    def __init__(
        self, processors: List["AbstractProcessor"], strict_interest_inference=False, workspace=None
    ):
        self.queue = Queue()
        self.processors = defaultdict(set)
        for processor in processors:
            for interest in processor.interests:
                logger.info(f"registering interest for {processor}: {interest}")
                print(f"registering interest for {processor}: {interest}")
                self.processors[interest].add(processor)

        self.jobs = 0
        self.lock = threading.Lock()
        self.cond = threading.Condition()
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
            # Removed brittle queue.empty() assert
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
                recipients = set() if self.strict_interest_inference else set(self.processors[(None, None)])
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
        with self.lock:
            self.jobs += 1

    def decrement(self):
        with self.lock:
            self.jobs -= 1
            if self.jobs <= 0:
                with self.cond:
                    self.cond.notify_all()


def parse_pattern(p):
    import ast

    def cls_id(n):
        match n:
            case ast.Name(id=id):
                return id
            case ast.Attribute(attr):
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
        case ast.pattern(pattern=pp):
            return parse_pattern(pp)
        case _:
            return None, None


def find_interests(func):
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
        self.interests = frozenset(find_interests(self.process))
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


class AbstractBuilder(AbstractProcessor):
    def __init__(
        self, provides: Collection[str], requires: Collection[str], cache: bool
    ):
        super().__init__()
        self.current_states: Set = {self.new, self.waiting}
        self.provides = list(provides)
        self.requires = list(requires)
        self.prerequisites: Dict[str, Any] = dict()
        self.rsvp = []

    def waiting(self, context, event):
        match event:
            case Event(name="resolve", target=target, sender=reply_to) if (
                target in self.provides
            ):
                # we know how to build this, however we don't have the artifact ready yet
                # store the incoming request so that we can respond to it later
                self.rsvp.append((reply_to, target))
            case Event(name="ready", sender=sender, to=to, artifact=artifact) if (
                sender is self and to is self
            ):
                # the artifact has been built and we are ready to deliver them to whoever requested them.
                # transit from self.waiting to self.ready
                self.current_states -= {self.waiting}
                self.current_states |= {self.ready}
                self.artifact = artifact
                while self.rsvp:
                    reply_to, target = self.rsvp.pop()
                    context.submit(
                        Event(
                            name="built",
                            target=target,
                            sender=self,
                            to=reply_to,
                            artifact=self.artifact,
                        )
                    )

    def ready(self, context, event):
        match event:
            case Event(name="resolve", target=target, sender=reply_to) if (
                target in self.provides
            ):
                # we now have the artifact ready to hand back to anyone who is asking for it
                context.submit(
                    Event(
                        name="built",
                        target=target,
                        sender=self,
                        to=reply_to,
                        artifact=self.artifact,
                    )
                )

    def new(self, context, event):
        match event:
            case Event(name="resolve", target=target) if (
                target in self.provides
            ):  # monitor for resolve requests that we know how to build
                with self.check_prerequisites(context):
                    self.current_states -= {self.new}
                    self.current_states |= {self.collecting}
                    for target in self.requires:
                        context.submit(
                            Event(name="resolve", target=target, sender=self)
                        )  # kick off resolution of pre-requisites to other builders

    def collecting(self, context, event):
        match event:
            case Event(name="built", target=target, artifact=artifact, to=to) if (
                to is self and target in self.requires
            ):
                with self.check_prerequisites(context):
                    self.prerequisites[target] = artifact

    def building(self, context: Context, event):
        match event:
            case Event(
                name="build",
                sender=sender,
                to=to,
                target=target,
                prerequisites=prerequisites,
            ) if sender is self and to is self:
                # we have everything we need to build the artifact
                try:
                    artifact = self.build(
                        context, target, *[prerequisites[r] for r in self.requires]
                    )
                    for provide in self.provides:
                        context.submit(
                            Event(
                                name="ready",
                                sender=self,
                                to=self,
                                target=provide,
                                artifact=artifact,
                            )
                        )
                except Exception as e:
                    print(e)
                    raise e

    @abc.abstractmethod
    def build(self, context, target, *args, **kwargs):
        """
        Override to implement build logic
        """
        pass

    @contextmanager
    def check_prerequisites(self, context: Context):
        yield
        if len(set(self.requires) - set(self.prerequisites)) <= 0:
            self.current_states -= {self.new, self.collecting}
            self.current_states |= {self.building}
            for target in self.provides:
                # now that we have everything, initialise the build
                context.submit(
                    Event(
                        name="build",
                        sender=self,
                        to=self,
                        target=target,
                        prerequisites=self.prerequisites,
                    )
                )

    def process(self, context, event):
        for state in list(self.current_states):
            state(context, event)


def builder(provides: str, requires: List[str], cache=False):
    def decorator(cls):
        assert issubclass(cls, AbstractBuilder)
        cls.__init__ = functools.partialmethod(
            cls.__init__,
            provides=[provides],
            requires=list(requires),
            cache=cache,  # TODO: cache is not used
        )
        return cls

    return decorator


class JsonLLoader(AbstractProcessor):
    def __init__(
        self, name: str, filepath: str, item_type, attribute: Optional[str] = None
    ):
        super().__init__()
        self.name = name
        self.filepath = filepath
        self.file = open(self.filepath, "rt")
        from pydantic import BaseModel

        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute

    class LoadJsonL(Event):
        def __init__(self, name, limit=None, sample=1.0, **kwargs):
            limit = float("inf") if limit is None else int(limit)
            super().__init__(
                name=name,
                sample=sample,
                limit=limit,
                **kwargs,
            )

    def process(self, context, event):
        match event:
            case self.LoadJsonL(name=self.name) as e:
                this_logger = logger.getChild(f"{self.__class__.__name__}.{event.name}")
                this_logger.debug(
                    f"loading {self.item_type} from jsonl file: {self.filepath}; limit: {e.limit}, sample: {e.sample}"
                )
                r = random.Random()
                line_count = 0
                loaded_count = 0
                for line in self.file:
                    line_count += 1
                    try:
                        if r.uniform(0, 1) > (1.0 - e.sample):
                            context.submit(
                                Event(
                                    name=self.name,
                                    **{
                                        self.attribute: self.item_type.model_validate_json(
                                            line
                                        )
                                    },
                                )
                            )
                            loaded_count += 1
                            if loaded_count >= e.limit:
                                break
                    except Exception as e:
                        raise e
                this_logger.debug(
                    f"finished loading jsonl... lines loaded {loaded_count} / {line_count} (ratio: {(loaded_count * 100 / line_count) if line_count > 0 else 0:.2f}%)"
                )
                context.submit(
                    Event(
                        name=self.name + "$",
                    )
                )
            case Event(name='__POISON__'):
                self.file.close()


class JsonLWriter(AbstractProcessor):
    def __init__(
        self,
        name,
        filepath: Union[PathLike, str],
        item_type,
        attribute: Optional[str] = None,
    ):
        super().__init__()
        self.name = name
        self.filepath = filepath
        assert issubclass(item_type, BaseModel)
        self.item_type = item_type
        self.attribute = name if attribute is None else attribute
        self.file = open(self.filepath, "wt")

    def process(self, context, event):
        match event:
            case (
                Event(
                    name=self.name,
                ) as e
            ) if isinstance(getattr(e, self.attribute, None), self.item_type):
                payload = getattr(e, self.attribute)
                self.file.writelines((payload.model_dump_json(), "\n"))
                self.file.flush()
            case Event(name='__POISON__'):
                self.file.close()
