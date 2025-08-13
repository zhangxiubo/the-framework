from typing import Any, List, Tuple, Set

from framework.core import Pipeline
from framework.reactive import ReactiveEvent, ReactiveBuilder, reactive
from framework.core import AbstractProcessor, Context


class ReactiveRecordingSink(AbstractProcessor):
    """Collects built() events for a given target (broadcast by topic)."""

    def __init__(self, target: str):
        super().__init__()
        self.target = target
        self.events: List[ReactiveEvent] = []

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if (
                target == self.target
            ):
                self.events.append(event)


class LeafProducer(AbstractProcessor):
    """Produces a fixed sequence of values for a topic when resolved."""

    def __init__(self, topic: str, values: List[Any]):
        super().__init__()
        self.topic = topic
        self.values = list(values)

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target, sender=reply_to) if target == self.topic:
                for v in self.values:
                    context.submit(
                        ReactiveEvent(
                            name="built",
                            target=self.topic,
                            artifact=v,
                            sender=self,
                            to=reply_to,
                        )
                    )


@reactive(provides="A", requires=["X"])
class AddTen(ReactiveBuilder):
    def build(self, context: Context, target: str, x: int):
        # Single-element iterable
        yield x + 10


@reactive(provides="P", requires=["X", "Y"])
class PairJoin(ReactiveBuilder):
    def build(self, context: Context, target: str, x: Any, y: Any):
        # Cartesian product emission per input pair
        yield (x, y)


@reactive(provides="Z", requires=[])
class ZeroPrereqOnce(ReactiveBuilder):
    def build(self, context: Context, target: str):
        # No prerequisites: emit once on first resolve (handled in 'new' state)
        yield "only-once"


def test_reactive_single_source_streams_and_deduplicates():
    sink = ReactiveRecordingSink(target="A")
    builder = AddTen()
    # Duplicate "2" should be de-duplicated by value digest
    prodX = LeafProducer(topic="X", values=[1, 2, 2, 3])

    p = Pipeline([sink, builder, prodX])
    # Subscribe sink and kick off
    p.submit(ReactiveEvent(name="resolve", target="A", sender=sink))
    p.run()

    artifacts = [e.artifact for e in sink.events]
    assert len(artifacts) == 3
    assert set(artifacts) == {11, 12, 13}


def test_reactive_cartesian_product_across_two_sources():
    sink = ReactiveRecordingSink(target="P")
    builder = PairJoin()
    prodX = LeafProducer(topic="X", values=[1, 2])
    # Includes duplicate 'a' to exercise per-topic de-duplication
    prodY = LeafProducer(topic="Y", values=["a", "b", "a"])

    p = Pipeline([sink, builder, prodX, prodY])
    p.submit(ReactiveEvent(name="resolve", target="P", sender=sink))
    p.run()

    artifacts: Set[Tuple[Any, Any]] = {e.artifact for e in sink.events}
    assert len(artifacts) == 4
    assert artifacts == {(1, "a"), (1, "b"), (2, "a"), (2, "b")}


def test_reactive_zero_prerequisites_invokes_once_and_broadcasts():
    sink1 = ReactiveRecordingSink(target="Z")
    sink2 = ReactiveRecordingSink(target="Z")
    builder = ZeroPrereqOnce()

    p = Pipeline([sink1, sink2, builder])
    # Resolve is a kick-off signal only; build is invoked exactly once and the result
    # is broadcast on topic 'Z' to all interested listeners.
    p.submit(ReactiveEvent(name="resolve", target="Z", sender=sink1))
    p.submit(ReactiveEvent(name="resolve", target="Z", sender=sink2))
    p.run()

    a1 = [e.artifact for e in sink1.events]
    a2 = [e.artifact for e in sink2.events]
    assert a1 == ["only-once"]
    assert a2 == ["only-once"], "Broadcast semantics: all listeners receive the emission"