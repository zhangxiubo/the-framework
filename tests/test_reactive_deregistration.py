from __future__ import annotations

from collections.abc import Iterable
from typing import List

import pytest

from framework.core import Pipeline, AbstractProcessor
from framework.reactive import ReactiveBuilder, ReactiveEvent, reactive


class RecordingSinkReactive(AbstractProcessor):
    """
    A permissive sink that records every event it receives, in order.
    It declares no match-case filters in process; with strict_interest_inference=False,
    it should receive all events as a wildcard listener.
    """

    def __init__(self, name: str = "sink"):
        super().__init__()
        self.name = name
        self.events: List[ReactiveEvent] = []

    def process(self, context, event):
        # Record all events, in arrival order
        self.events.append(event)


class ReactiveSource(AbstractProcessor):
    """
    Simple reactive source that, upon resolve(target=provides), emits configured artifacts
    as ReactiveEvent(name="built", target=provides, artifact=item).
    """

    def __init__(self, provides: str, artifacts: List[object]):
        super().__init__()
        self.provides = provides
        self.artifacts = list(artifacts)

    def process(self, context, event):
        match event:
            case ReactiveEvent(name="resolve", target=target) if target == self.provides:
                for a in self.artifacts:
                    context.submit(
                        ReactiveEvent(name="built", target=self.provides, artifact=a)
                    )


@reactive(provides="A", requires=["X", "Y"], cache=False)
class Joiner(ReactiveBuilder):
    """
    Joiner that yields one artifact per unique combination (X, Y).
    Uses the framework combo-level DeepHash key to avoid rebuilding duplicate combos.
    """

    calls = 0

    def build(self, context, target, x, y):
        type(self).calls += 1
        yield (x, y)


def test_join_builds_once_per_unique_combo():
    """
    - Sources produce duplicates for X and Y.
    - Joiner must build once per unique combo only.
    - Built events for target "A" contain only the unique combos.
    """
    # X has duplicates 1, 1; Y has duplicates "p", "p", and unique "q"
    src_x = ReactiveSource("X", [1, 1])
    src_y = ReactiveSource("Y", ["p", "p", "q"])
    join = Joiner()
    sink = RecordingSinkReactive()

    p = Pipeline(
        processors=[src_x, src_y, join, sink],
        strict_interest_inference=False,
        workspace=None,
    )

    # Kick off the join workflow by resolving target "A"
    p.submit(ReactiveEvent(name="resolve", target="A"))
    p.run()

    # Collect built events for the joiner's provided target ("A")
    a_artifacts = [
        e.artifact
        for e in sink.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "A"
    ]

    # Expect exactly two unique combos: (1, "p") and (1, "q")
    assert set(a_artifacts) == {(1, "p"), (1, "q")}
    # Ensure build() was invoked only for unique combinations
    assert Joiner.calls == 2


# Unit test for in-memory reply replay without Pipeline
class DummyContext:
    """Minimal context stub sufficient for ReactiveBuilder under cache=False/True."""

    class PipelineStub:
        def __init__(self, workspace=None):
            self.workspace = workspace

    def __init__(self, workspace=None):
        self.pipeline = DummyContext.PipelineStub(workspace=workspace)
        self.events = []

    def submit(self, event):
        # Record but don't dispatch; sufficient for reply() assertions.
        self.events.append(event)


@reactive(provides="B", requires=[], cache=False)
class TwoArtifactBuilder(ReactiveBuilder):
    """
    Builder that emits two artifacts on first resolve("B").
    Subsequent resolve("B") should replay cached artifacts via reply() without rebuilding.
    """

    calls = 0

    def build(self, context, target: str, *args, **kwargs) -> Iterable[object]:
        type(self).calls += 1
        yield "u"
        yield "v"


def test_reply_replays_cached_artifacts_without_rebuild():
    """
    Second resolve("B") should:
      - Not call build() again.
      - Replay all cached artifacts via reply() in emission order.
    """
    b = TwoArtifactBuilder()
    ctx = DummyContext()

    # First resolve populates the cache and emits built events via publish
    b.process(ctx, ReactiveEvent(name="resolve", target="B"))
    assert TwoArtifactBuilder.calls == 1
    # Ignore the first emission; focus on the reply path
    ctx.events.clear()

    # Second resolve triggers reply() replay from cache
    b.process(ctx, ReactiveEvent(name="resolve", target="B"))

    built = [
        e
        for e in ctx.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "B"
    ]
    assert [e.artifact for e in built] == ["u", "v"]
    # No new build occurred
    assert TwoArtifactBuilder.calls == 1