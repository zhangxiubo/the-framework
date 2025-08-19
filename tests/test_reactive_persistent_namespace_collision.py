from __future__ import annotations

from typing import List

from pydantic import BaseModel

from framework.core import Pipeline, AbstractProcessor
from framework.reactive import ReactiveBuilder, ReactiveEvent, reactive


class RecordingSinkReactive(AbstractProcessor):
    """
    Records all ReactiveEvent emissions for assertions.
    """

    def __init__(self, name: str = "sink"):
        super().__init__()
        self.name = name
        self.events: List[ReactiveEvent] = []

    def process(self, context, event):
        self.events.append(event)


@reactive(provides="A", requires=[], cache=True)
class PersistentUnitBuilder(ReactiveBuilder):
    """
    A builder with no prerequisites that yields a single constant artifact.
    With cache=True, it should persist its per-target combo cache across Pipeline runs
    (under workspace/target/ClassName/source_hash.sqlite).
    """

    calls = 0

    def build(self, context, target, *args, **kwargs):
        type(self).calls += 1
        yield "ONE"


def test_persistent_cache_replay_across_runs(tmp_path):
    """
    Validates persistent combo cache replay across runs:
      1) First run: submit resolve("A") -> build executes once and emits "ONE".
      2) Second run (fresh pipeline): with same workspace and class source, no new build:
         reply() replays cached artifact "ONE" for resolve("A").
    """
    # First run — build executes and persists
    sink1 = RecordingSinkReactive()
    b1 = PersistentUnitBuilder()
    p1 = Pipeline(
        processors=[b1, sink1], strict_interest_inference=False, workspace=tmp_path
    )
    p1.submit(ReactiveEvent(name="resolve", target="A"))
    p1.run()

    built1 = [
        e
        for e in sink1.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "A"
    ]
    assert [e.artifact for e in built1] == ["ONE"]
    assert PersistentUnitBuilder.calls == 1

    # Second run — no new build, cache replay via reply()
    sink2 = RecordingSinkReactive()
    b2 = PersistentUnitBuilder()
    p2 = Pipeline(
        processors=[b2, sink2], strict_interest_inference=False, workspace=tmp_path
    )
    p2.submit(ReactiveEvent(name="resolve", target="A"))
    p2.run()

    built2 = [
        e
        for e in sink2.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "A"
    ]
    assert [e.artifact for e in built2] == ["ONE"]
    # No additional build calls should have occurred; still exactly 1
    assert PersistentUnitBuilder.calls == 1