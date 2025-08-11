
import abc
import functools
from contextlib import contextmanager
from typing import Any, Collection, Dict, List, Set

from .core import AbstractProcessor, Context, Event

class AbstractBuilder(AbstractProcessor):
    def __init__(self, provides: Collection[str], requires: Collection[str], cache: bool):
        super().__init__()
        self.current_states: Set = {self.new, self.waiting}
        self.provides = list(provides)
        self.requires = list(requires)
        self.prerequisites: Dict[str, Any] = {}
        self.rsvp = []

    def waiting(self, context, event):
        match event:
            case Event(name="resolve", target=target, sender=reply_to) if (target in self.provides):
                # we know how to build the target, however we don't have the artifact ready yet
                # store any incoming request so that we can respond to it later
                self.rsvp.append((reply_to, target))
            case Event(name="ready", sender=sender, to=to, artifact=artifact) if (sender is self and to is self):
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
            case Event(name="resolve", target=target, sender=reply_to) if (target in self.provides):
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
            case Event(name="resolve", target=target) if (target in self.provides):  # monitor for resolve requests that we know how to build
                with self.check_prerequisites(context):
                    self.current_states -= {self.new}
                    self.current_states |= {self.collecting}
                    for target in self.requires:
                        context.submit(Event(name="resolve", target=target, sender=self))  # kick off resolution of pre-requisites to other builders

    def collecting(self, context, event):
        match event:
            case Event(name="built", target=target, artifact=artifact, to=to) if (to is self and target in self.requires):
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
                    artifact = self.build(context, target, *[prerequisites[r] for r in self.requires])
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
