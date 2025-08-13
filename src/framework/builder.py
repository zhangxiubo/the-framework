"""
Builder state-machine: dependency-driven construction over an event-driven runtime.

This module implements a small, non-blocking state machine that coordinates building
a single artifact for a single provided target, after collecting its prerequisites.

Each builder instance declares:
- provides: the single target it can produce
- requires: the prerequisite target(s) it depends on

Lifecycle and event protocol:
- Incoming: resolve(target) - Request to obtain an artifact for the provided target
- Outgoing to prerequisites: resolve(require_i) - Fan out resolve events for each prerequisite
- Incoming from prerequisites: built(target=..., to=self, artifact=...) - Reply with built events
- Internal trigger to self: build(target, prerequisites) - Execute build after prerequisites collected
- Internal: ready(target, artifact) - Transition to ready state after build completes
- Outgoing responses: built(target, artifact, to=requestor) - Respond to resolve requests

Concurrency model:
- Each builder runs within the AbstractProcessor's single-threaded executor, so state
  mutations and transitions are serialized per instance. No explicit locking is needed.

RSVP handling:
- Resolve requests that arrive before the artifact is ready are queued (RSVP). When the
  artifact becomes ready, the queue is drained by emitting built to each requestor in FIFO order.
"""

import abc
import functools
import logging
import types
from contextlib import contextmanager
from collections import deque
from typing import Any, Collection, Dict, List, Set

from pydantic import BaseModel, ConfigDict

from .core import AbstractProcessor, Context

logger = logging.getLogger(__name__)


class BuilderEvent(BaseModel):
    """Immutable event used by the builder system; virtually identical to core.Event"""

    model_config = ConfigDict(frozen=True, extra="allow")
    name: str


class AbstractBuilder(AbstractProcessor):
    """Dependency-aware builder implemented as a small event-driven state machine.

    A builder transitions through states to accept resolve requests, gather required
    prerequisites, build the artifact, and then serve built replies to requestors.
    State transitions are driven by events on the same single-threaded executor
    (inherited from AbstractProcessor), ensuring serialized mutation per instance.

    States:
    - new: has not yet fanned out prerequisite resolution
    - waiting: can accept resolve requests, queues RSVP until artifact is ready
    - collecting: gathering built events for required targets
    - building: executing build after prerequisites are complete
    - ready: artifact available; respond immediately to resolve with built
    """

    def __init__(self, provides: str, requires: Collection[str], cache: bool):
        """Initialize the builder with its single provided target and runtime state.

        Parameters:
        - provides: the target name this builder can produce
        - requires: collection of prerequisite target names that must be built first
        - cache: placeholder for future caching behavior (currently unused)
        """
        super().__init__()
        self.current_states: Set = {self.new, self.waiting}
        self.provides = provides
        self.requires = list(dict.fromkeys(requires))  # Deduplicate while preserving order
        self.prerequisites: Dict[str, Any] = {}
        self.rsvp = deque()  # Queue for early resolve requests
        self.artifact = None

    def waiting(self, context, event):
        """State: waiting for the artifact to become available.

        Handles resolve requests while build is pending and transitions to ready when complete.
        """
        match event:
            case BuilderEvent(name="resolve", target=target, sender=reply_to) if target == self.provides:
                # Queue the request to respond once artifact is ready
                self.rsvp.append((reply_to, target))
                
            case BuilderEvent(name="ready", sender=sender, to=to, artifact=artifact) if sender is self and to is self:
                # Transition to ready state
                self.current_states -= {self.waiting, self.building}
                self.current_states |= {self.ready}
                self.artifact = artifact
                
                # Respond to all queued requests
                while self.rsvp:
                    reply_to, target = self.rsvp.popleft()
                    context.submit(
                        BuilderEvent(
                            name="built",
                            target=target,
                            sender=self,
                            to=reply_to,
                            artifact=self.artifact,
                        )
                    )

    def ready(self, context, event):
        """State: artifact is available. Immediately respond to resolve requests."""
        match event:
            case BuilderEvent(name="resolve", target=target, sender=reply_to) if target == self.provides:
                context.submit(
                    BuilderEvent(
                        name="built",
                        target=target,
                        sender=self,
                        to=reply_to,
                        artifact=self.artifact,
                    )
                )

    def new(self, context, event):
        """State: initial state before prerequisites have been requested.
        
        Transitions to collecting state and fans out resolve requests for prerequisites.
        """
        match event:
            case BuilderEvent(name="resolve", target=target) if target == self.provides:
                with self.check_prerequisites(context):
                    # Transition to collecting state
                    self.current_states -= {self.new}
                    self.current_states |= {self.collecting}
                    
                    # Fan out resolve requests for all prerequisites
                    for required_target in self.requires:
                        context.submit(
                            BuilderEvent(name="resolve", target=required_target, sender=self)
                        )

    def collecting(self, context, event):
        """State: collect prerequisite artifacts as they are built.
        
        Stores artifacts and triggers build when all prerequisites are satisfied.
        """
        match event:
            case BuilderEvent(name="built", target=target, artifact=artifact, to=to) if to is self and target in self.requires:
                with self.check_prerequisites(context):
                    self.prerequisites[target] = artifact

    def building(self, context: Context, event):
        """State: perform the build once all prerequisites are available."""
        match event:
            case BuilderEvent(name="build", sender=sender, to=to, target=target, prerequisites=prerequisites) if sender is self and to is self:
                try:
                    # Build the artifact with prerequisites in required order
                    artifact = self.build(
                        context, target, *[prerequisites[r] for r in self.requires]
                    )
                    
                    # Convert generators to lists to avoid serialization issues
                    if isinstance(artifact, types.GeneratorType):
                        artifact = list(artifact)
                    
                    # Signal that the artifact is ready
                    context.submit(
                        BuilderEvent(
                            name="ready",
                            sender=self,
                            to=self,
                            target=target,
                            artifact=artifact,
                        )
                    )
                except Exception as e:
                    logger.error(f"Build failed for target {target}: {e}", exc_info=True)
                    raise

    @abc.abstractmethod
    def build(self, context, target, *args, **kwargs):
        """Construct and return the artifact for the given target.

        Args:
            context: The execution context
            target: The target to build
            *args: Prerequisites in the exact order of self.requires
            **kwargs: Additional parameters (unused by framework)
            
        Returns:
            The constructed artifact
        """
        pass

    @contextmanager
    def check_prerequisites(self, context: Context):
        """Context manager that checks if all prerequisites are satisfied after the wrapped block."""
        yield
        
        # Check if all required prerequisites have been collected
        missing_prerequisites = set(self.requires) - set(self.prerequisites)
        if not missing_prerequisites:
            # Transition to building state
            self.current_states -= {self.new, self.collecting}
            self.current_states |= {self.building}
            
            # Trigger the build
            if self.provides:
                context.submit(
                    BuilderEvent(
                        name="build",
                        sender=self,
                        to=self,
                        target=self.provides,
                        prerequisites=self.prerequisites,
                    )
                )

    def process(self, context, event):
        """Dispatch the incoming event to all currently active state handlers."""
        match event:
            case BuilderEvent(name=name) if name in ("resolve", "build", "built", "ready"):
                # Snapshot current_states to avoid mutation during iteration
                for state in list(self.current_states):
                    state(context, event)


def builder(provides: str, requires: List[str], cache=False):
    """Class decorator to declare a single provided target and its requirements.

    Example:
        @builder(provides="target", requires=["a", "b"])
        class MyBuilder(AbstractBuilder):
            def build(self, context, target, a, b):
                return a + b

    Args:
        provides: The target name this builder can produce
        requires: List of prerequisite target names
        cache: Reserved for future caching behavior (currently unused)
    """
    def decorator(cls):
        assert issubclass(cls, AbstractBuilder)
        cls.__init__ = functools.partialmethod(
            cls.__init__,
            provides=provides,
            requires=list(requires),
            cache=cache,
        )
        return cls

    return decorator
