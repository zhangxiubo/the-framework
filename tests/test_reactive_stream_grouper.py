from __future__ import annotations

from typing import List, Tuple

from framework.core import Pipeline, AbstractProcessor
from framework.reactive import ReactiveEvent, StreamGrouper


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


def test_stream_grouper_emits_previous_group_on_key_change_no_final_flush():
    """
    StreamGrouper behavior (as implemented):

    - On first element (key change from None to 'a'), it emits (None, []).
    - While the key stays the same, it accumulates items in lastset BUT only after the first
      item of that key has already arrived; the first item of a group is NOT included in lastset.
    - When the key changes, it yields (lastkey, lastset) and resets to the new key with empty set.
    - No final flush at the end is performed (i.e., the last group's items are dropped).

    Given stream: a1('a'), a2('a'), b1('b'), b2('b'):
      - First emission: (None, [])
      - Second emission on key change to 'b': ('a', [('a2',)])  # a1 is dropped per implementation
      - No emission for final 'b' group.
    """
    # Source emits 4 items in order
    src = ReactiveSource("S", ["a1", "a2", "b1", "b2"])

    # Key function groups by first character
    keyfunc = lambda s: s[0]

    grouper = StreamGrouper(
        provides="G",
        requires=["S"],
        keyfunc=keyfunc,
    )
    sink = RecordingSinkReactive()

    p = Pipeline(
        processors=[src, grouper, sink],
        strict_interest_inference=False,
        workspace=None,
    )

    # Kick off the grouper by resolving target "G"
    p.submit(ReactiveEvent(name="resolve", target="G"))
    p.run()

    # Collect built events for "G" emitted by StreamGrouper
    built_events = [
        e
        for e in sink.events
        if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "G"
    ]

    # Expect exactly two emissions per current implementation
    assert len(built_events) == 2

    # 1) Initial emission before first key is established
    first_key, first_set = built_events[0].artifact
    assert first_key is None
    assert first_set == []

    # 2) Emission when switching from 'a' to 'b' — implementation drops the first item of the group
    second_key, second_set = built_events[1].artifact
    assert second_key == "a"
    # Each entry in the set is a tuple of args (because build receives *args)
    assert second_set == [("a2",)]