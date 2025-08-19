from __future__ import annotations

from typing import List, Tuple, Set

from framework.core import Pipeline, AbstractProcessor
from framework.reactive import ReactiveEvent, Collector


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


def test_collector_emits_on_phase_when_values_changed():
    """
    Collector behavior derived from implementation:
      - Listens to built() on each topic in 'topics' (requires).
      - Accumulates args in self.values via build() (which yields nothing).
      - On __PHASE__, for each known target (made known on resolve(forwards_to)),
        if values != last, emits built(forwards_to, artifact=values.copy()) and updates last.
      - Initial last=None ensures at least one emission after first changes.
    """
    src_u = ReactiveSource("U", ["u1"])
    src_v = ReactiveSource("V", ["v1"])
    collector = Collector(forwards_to="A", topics=["U", "V"])
    sink = RecordingSinkReactive()

    p = Pipeline(
        processors=[src_u, src_v, collector, sink],
        strict_interest_inference=False,
        workspace=None,
    )

    # Kick off the collector by resolving the forward target; it will fan out resolves to U and V
    p.submit(ReactiveEvent(name="resolve", target="A"))
    p.run()

    # Collect built events for "A" emitted by Collector during phase
    built_events = [
        e
        for e in sink.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "A"
    ]
    assert len(built_events) >= 1

    # The last emission should reflect the single combo (U x V) accumulated via publish():
    # values is a list of args-tuples passed to build(); for Collector with requires ["U", "V"]
    # and one artifact each, we expect one tuple ("u1", "v1")
    last_artifact = built_events[-1].artifact
    assert isinstance(last_artifact, list)
    assert last_artifact == [("u1", "v1")]