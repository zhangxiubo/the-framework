"""
Builder state-machine: dependency-driven construction over an event-driven runtime.

This module implements a small, non-blocking state machine that coordinates building
a single artifact for a single provided target, after collecting its prerequisites.

Each builder instance declares:
- provides: the single target it can produce
- requires: the prerequisite target(s) it depends on

Lifecycle and event protocol:
- Incoming: resolve(target)
  A request to obtain an artifact for the provided target.
- Outgoing to prerequisites: resolve(require_i)
  When a builder accepts a resolve for what it provides, it fans out resolve events
  for each prerequisite it requires.
- Incoming from prerequisites: built(target=..., to=self, artifact=...)
  Prerequisite builders reply with built events addressed back to the requester.
- Internal trigger to self: build(target, prerequisites)
  Once all prerequisites are collected, the builder posts exactly one internal build event to
  itself to execute the build in its own executor.
- Internal: ready(target, artifact)
  After build completes, the builder posts exactly one ready to itself to transition to ready state.
- Outgoing responses: built(target, artifact, to=requestor)
  When ready (either immediately or after transitioning), respond to any resolve requests
  with built.

Concurrency model:
- Each builder runs within the AbstractProcessor’s single-threaded executor, so state
  mutations and transitions are serialized per instance. No explicit locking is needed.

RSVP handling:
- Resolve requests that arrive before the artifact is ready are queued (RSVP). When the
  artifact becomes ready, the queue is drained by emitting built to each requestor in FIFO order.

Notes:
- The builder() decorator helps declare single-provide builders. The cache argument
  is currently not used; it is reserved for future extension.
  TODO: Evaluate wiring the cache parameter into a caching policy (e.g., memoization,
  invalidation) or remove if not needed.

Potential extensions:
- Support multiple provided targets and/or per-target artifacts
- Incremental builds or invalidation/cancellation of ongoing work
- De-duplication of duplicate resolve requests while a build is in-flight
- Robustness around missing/unexpected built events and error propagation
"""

import abc
import functools
import types
from contextlib import contextmanager
from collections import deque
from typing import Any, Collection, Dict, List, Set

from .core import AbstractProcessor, Context, Event


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

        Runtime structures:
        - current_states: a set of active state handlers; multiple states can be
          active during transitions (e.g., new + waiting initially)
        - prerequisites: map from required target -> built artifact collected so far
        - rsvp: FIFO queue of (reply_to, target) tuples for resolve requests that arrived
          before the artifact became ready

        Note:
        - TODO: The cache parameter is not used; consider adding a caching policy
          or removing the parameter to simplify the API.
        """
        super().__init__()
        self.current_states: Set = {self.new, self.waiting}
        self.provides = provides
        self.requires = list(dict.fromkeys(requires))
        self.prerequisites: Dict[str, Any] = {}
        self.rsvp = deque()
        self.artifact = None

    def waiting(self, context, event):
        """State: waiting for the artifact to become available.

        - On resolve for a provided target: store the request (RSVP) to reply later.
        - On ready from self to self: transition to ready, publish built to all
          queued requestors (FIFO), and retain the artifact for immediate future replies.

        This state exists to accept early resolve requests while a build is pending
        and to ensure they are answered once the artifact is produced.
        """
        match event:
            case Event(name="resolve", target=target, sender=reply_to) if (
                target == self.provides
            ):
                # We know how to build the target, but the artifact is not ready yet.
                # Queue the request so we can respond once the artifact is ready.
                # This avoids blocking and allows multiple callers to be answered later.
                self.rsvp.append((reply_to, target))
            case Event(name="ready", sender=sender, to=to, artifact=artifact) if (
                sender is self and to is self
            ):
                # Our own build has completed and signaled readiness.
                # Transition the state machine: remove waiting and building, add ready.
                # This mutation of current_states moves the instance to "ready" so future
                # resolve events can be answered immediately with built.
                self.current_states -= {self.waiting, self.building}
                self.current_states |= {self.ready}
                # Capture the produced artifact for reuse in all replies.
                self.artifact = artifact
                # Drain the RSVP queue in FIFO order: respond to all callers who resolved early.
                while self.rsvp:
                    reply_to, target = self.rsvp.popleft()
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
        """State: artifact is available.

        - On resolve for a provided target: immediately respond with built using
          the cached artifact produced previously.
        """
        match event:
            case Event(name="resolve", target=target, sender=reply_to) if (
                target == self.provides
            ):
                # Artifact is already ready; reply immediately without queueing.
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
        """State: initial state before prerequisites have been requested.

        - On resolve for a provided target: transition to collecting and fan out
          resolve for each required prerequisite.
        """
        match event:
            case Event(name="resolve", target=target) if (
                target == self.provides
            ):  # monitor for resolve requests that we know how to build
                with self.check_prerequisites(context):
                    # Leave "new" and start "collecting" prerequisites.
                    self.current_states -= {self.new}
                    self.current_states |= {self.collecting}
                    # Fan out resolve to all prerequisites. The order of "requires"
                    # matters because it determines the argument order passed to build().
                    # Note: the local loop variable name "target" shadows the outer
                    # "target" intentionally; we preserve the original behavior.
                    for target in self.requires:
                        context.submit(
                            Event(name="resolve", target=target, sender=self)
                        )  # kick off resolution of pre-requisites to other builders

    def collecting(self, context, event):
        """State: collect prerequisite artifacts as they are built.

        - On built addressed to self for a required target: store artifact and
          re-check if all prerequisites are now satisfied.
        - The guard via check_prerequisites may trigger the build exactly once
          when the final prerequisite arrives.
        """
        match event:
            case Event(name="built", target=target, artifact=artifact, to=to) if (
                to is self and target in self.requires
            ):
                with self.check_prerequisites(context):
                    # Store the arrived prerequisite and then check if we have them all.
                    # When the last one lands, check_prerequisites transitions to building.
                    self.prerequisites[target] = artifact

    def building(self, context: Context, event):
        """State: perform the build once all prerequisites are available.

        - On internal build event addressed to self: invoke build() with prerequisites
          ordered according to self.requires; emit a single ready event addressed to self
          for the provided target.
        - Errors during build are surfaced after logging.
        """
        match event:
            case Event(
                name="build",
                sender=sender,
                to=to,
                target=target,
                prerequisites=prerequisites,
            ) if sender is self and to is self:
                # We have everything we need to build the artifact.
                try:
                    artifact = self.build(
                        context, target, *[prerequisites[r] for r in self.requires]
                    )
                    if isinstance(artifact, types.GeneratorType):
                        artifact = list(artifact)
                    # Emit a single ready event addressed to self for the provided target.
                    context.submit(
                        Event(
                            name="ready",
                            sender=self,
                            to=self,
                            target=target,
                            artifact=artifact,
                        )
                    )
                except Exception as e:
                    # Propagate errors to the runtime after logging for visibility.
                    print(e)
                    raise e

    @abc.abstractmethod
    def build(self, context, target, *args, **kwargs):
        """Construct and return the artifact for the given target.

        Implementations must:
        - Use "args" corresponding to prerequisites in the exact order of self.requires.
        - Optionally use "kwargs" for additional parameters (unused by the framework).
        - Return the constructed artifact. Exceptions will propagate and fail the build.

        Semantics:
        - Pure or idempotent behavior is encouraged but not enforced.
        - Any side effects should be confined to this builder's responsibility.
        """
        pass

    @contextmanager
    def check_prerequisites(self, context: Context):
        """Context manager that re-checks prerequisites after the wrapped block.

        Semantics:
        - After yielding to the body (which typically records a newly built prerequisite),
          verify whether all required targets have been collected.
        - If complete, transition from {new, collecting} to {building} and submit
          exactly one internal build event (addressed to self) using the provided target.
          Submitting events to self ensures execution stays on this processor's
          own single-thread executor, preserving serialized state transitions.
        """
        yield
        if len(set(self.requires) - set(self.prerequisites)) <= 0:
            # Transition logic: drop "new" and "collecting", enter "building".
            self.current_states -= {self.new, self.collecting}
            self.current_states |= {self.building}
            # Submit exactly one internal build event addressed to self using the provided target.
            if self.provides:
                context.submit(
                    Event(
                        name="build",
                        sender=self,
                        to=self,
                        target=self.provides,
                        prerequisites=self.prerequisites,
                    )
                )
            else:
                # No provided target; nothing to build.
                return

    def _process(self, context, event):
        for state in list(
            self.current_states
        ):  # snapshot to avoid mutation during iteration
            state(context, event)

    def process(self, context, event):
        """Dispatch the incoming event to all currently active state handlers.

        Note:
        - We snapshot current_states via list(...) to avoid iteration issues while
          the set mutates during handling (states may add/remove themselves).
        """
        match event:
            case Event(name="resolve"):
                self._process(context, event)
            case Event(name="build"):
                self._process(context, event)
            case Event(name="built"):
                self._process(context, event)
            case Event(name="ready"):
                self._process(context, event)


def builder(provides: str, requires: List[str], cache=False):
    """Class decorator to declare a single provided target and its requirements.

    This decorator standardizes subclass construction by partially binding the
    __init__ of AbstractBuilder with provides/requires/cache, so users can write:

        @builder(provides="target", requires=["a", "b"])
        class MyBuilder(AbstractBuilder):
            ...

    and construct MyBuilder() without passing provides/requires explicitly.

    Notes:
    - functools.partialmethod pre-binds provides/requires/cache into __init__.
    - TODO: cache is currently unused; consider integrating a caching policy or
      removing the parameter to avoid confusion.
    """

    def decorator(cls):
        assert issubclass(cls, AbstractBuilder)
        cls.__init__ = functools.partialmethod(
            cls.__init__,
            provides=provides,
            requires=list(requires),
            cache=cache,  # TODO: cache is not used
        )
        return cls

    return decorator
