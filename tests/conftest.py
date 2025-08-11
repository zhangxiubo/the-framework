from typing import List, Callable, Optional

import pytest

from framework.core import Pipeline, AbstractProcessor
from framework.builder import (
    BuilderEvent,
    AbstractBuilder,
    builder as builder_decorator,
)


class RecordingSink(AbstractProcessor):
    """
    A permissive sink that records every event it receives, in order.
    It declares no match-case filters in process; with strict_interest_inference=False,
    it should receive all events as a wildcard listener.
    """

    def __init__(self, name: str = "sink"):
        super().__init__()
        self.name = name
        self.events: List[BuilderEvent] = []

    def process(self, context, event):
        # Record all events, in arrival order
        self.events.append(event)

    # Convenience filters for assertions
    def by_name(self, name: str) -> List[BuilderEvent]:
        return [e for e in self.events if getattr(e, "name", None) == name]

    def built_to(self, to_obj) -> List[BuilderEvent]:
        return [
            e
            for e in self.events
            if getattr(e, "name", None) == "built" and getattr(e, "to", None) is to_obj
        ]

    def resolves_from(self, sender_obj) -> List[BuilderEvent]:
        return [
            e
            for e in self.events
            if getattr(e, "name", None) == "resolve"
            and getattr(e, "sender", None) is sender_obj
        ]

    def first_index(self, predicate: Callable[[BuilderEvent], bool]) -> Optional[int]:
        for idx, e in enumerate(self.events):
            if predicate(e):
                return idx
        return None

    def indices(self, predicate: Callable[[BuilderEvent], bool]) -> List[int]:
        return [idx for idx, e in enumerate(self.events) if predicate(e)]


class TriggerAfterReadySink(AbstractProcessor):
    """
    A sink that triggers a follow-up resolve for target on the specified builder
    after observing the builder's internal 'ready' event. This allows testing
    ready()-path immediate replies within a single Pipeline.run() cycle.
    """

    def __init__(self, target: str, builder_obj: AbstractBuilder):
        super().__init__()
        self.target = target
        self.builder_obj = builder_obj
        self.events: List[BuilderEvent] = []
        self.did_trigger = False

    def process(self, context, event):
        self.events.append(event)
        if (
            not self.did_trigger
            and getattr(event, "name", None) == "ready"
            and getattr(event, "to", None) is self.builder_obj
        ):
            # Trigger the second resolve only after the builder reports ready to itself
            self.did_trigger = True
            context.submit(
                BuilderEvent(name="resolve", target=self.target, sender=self)
            )


# Concrete builders used by tests


class LeafBuilder(AbstractBuilder):
    """
    Leaf builder that returns an uppercase form of its provided target tag,
    e.g. provides='a' -> artifact='A'.
    """

    def build(self, context, target, *args, **kwargs):
        return str(self.provides).upper()


class PairBuilder(AbstractBuilder):
    """
    Pair builder that composes two prerequisites into "A|B" format.
    """

    def build(self, context, target, a, b, *args, **kwargs):
        return f"{a}|{b}"


class CountingPairBuilder(AbstractBuilder):
    """
    Pair builder variant that increments a class counter on each build invocation,
    returning the same artifact format as PairBuilder. The class variable is reset
    per-test by fixture usage.
    """

    calls = 0

    def build(self, context, target, a, b, *args, **kwargs):
        type(self).calls += 1
        return f"{a}|{b}"


class ErrorBuilder(AbstractBuilder):
    """
    Builder that always raises to test error propagation behavior.
    """

    def build(self, context, target, *args, **kwargs):
        raise RuntimeError("boom")


@builder_decorator(provides="d", requires=["a", "b"], cache=False)
class DecoratorBuilder(AbstractBuilder):
    """
    Builder declared via @builder() to validate decorator wiring of __init__.
    """

    def build(self, context, target, a, b, *args, **kwargs):
        return f"D:{a}+{b}"


@pytest.fixture
def make_pipeline():
    """
    Returns a factory function to construct a Pipeline with strict interest inference
    disabled (wildcard delivery permitted). Always construct a fresh Pipeline per test.
    """

    def _make(*processors: AbstractProcessor) -> Pipeline:
        return Pipeline(
            processors=list(processors), strict_interest_inference=False, workspace=None
        )

    return _make


@pytest.fixture
def run_to_quiescence():
    """
    Returns a function that runs the pipeline to completion deterministically, without sleeps.
    Note: Pipeline.run() tears down executors; do not reuse a pipeline after a run.
    """

    def _run(p: Pipeline):
        p.run()

    return _run


@pytest.fixture(autouse=True)
def reset_counters():
    """
    Ensure CountingPairBuilder counters are reset for each test to avoid cross-test contamination.
    """
    CountingPairBuilder.calls = 0
    yield
    CountingPairBuilder.calls = 0
