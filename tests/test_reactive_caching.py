from typing import Any, List

from framework.core import Pipeline, AbstractProcessor, Context
from framework.reactive import ReactiveEvent, ReactiveBuilder, reactive


class TopicSink(AbstractProcessor):
    """Collect 'built' events for a given topic."""

    def __init__(self, target: str):
        super().__init__()
        self.target = target
        self.events: List[ReactiveEvent] = []

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if target == self.target:
                self.events.append(event)


class Producer(AbstractProcessor):
    """Produces a fixed sequence of values on built(topic, artifact=v) when resolved."""

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


@reactive(provides="C", requires=["X"], cache=True)
class CountingReactive(ReactiveBuilder):
    calls = 0

    def build(self, context: Context, target: str, x: int):
        type(self).calls += 1
        yield f"C:{x}"


@reactive(provides="G", requires=["X"], cache=False)
class NonCachingReactive(ReactiveBuilder):
    calls = 0

    def build(self, context: Context, target: str, x: int):
        type(self).calls += 1
        yield f"G:{x}"


def test_reactive_persistent_caching_hit_and_miss(tmp_path):
    # Reset counters
    CountingReactive.calls = 0

    # First run: miss -> execute -> cache
    sink1 = TopicSink("C")
    rx1 = CountingReactive()
    prod1 = Producer("X", [1])
    p1 = Pipeline([sink1, rx1, prod1], workspace=tmp_path)
    # Kick-off with a resolve for 'C'; no sender to keep event JSON simple for the cache key
    p1.submit(ReactiveEvent(name="resolve", target="C"))
    p1.run()
    assert CountingReactive.calls == 1
    assert [e.artifact for e in sink1.events] == ["C:1"]

    # Second run: hit -> replay -> no new build() invocation
    sink2 = TopicSink("C")
    rx2 = CountingReactive()
    prod2 = Producer("X", [1])
    p2 = Pipeline([sink2, rx2, prod2], workspace=tmp_path)
    p2.submit(ReactiveEvent(name="resolve", target="C"))
    p2.run()
    assert CountingReactive.calls == 1  # unchanged
    assert [e.artifact for e in sink2.events] == ["C:1"]

    # Third run with different input: miss -> execute -> calls increments
    sink3 = TopicSink("C")
    rx3 = CountingReactive()
    prod3 = Producer("X", [2])
    p3 = Pipeline([sink3, rx3, prod3], workspace=tmp_path)
    p3.submit(ReactiveEvent(name="resolve", target="C"))
    p3.run()
    assert CountingReactive.calls == 2
    assert [e.artifact for e in sink3.events] == ["C:2"]


def test_reactive_cache_gate_disabled_does_not_persist(tmp_path):
    NonCachingReactive.calls = 0

    sink1 = TopicSink("G")
    rx1 = NonCachingReactive()
    prod1 = Producer("X", [1])
    p1 = Pipeline([sink1, rx1, prod1], workspace=tmp_path)
    p1.submit(ReactiveEvent(name="resolve", target="G"))
    p1.run()
    assert NonCachingReactive.calls == 1
    assert [e.artifact for e in sink1.events] == ["G:1"]

    sink2 = TopicSink("G")
    rx2 = NonCachingReactive()
    prod2 = Producer("X", [1])
    p2 = Pipeline([sink2, rx2, prod2], workspace=tmp_path)
    p2.submit(ReactiveEvent(name="resolve", target="G"))
    p2.run()
    assert NonCachingReactive.calls == 2
    assert [e.artifact for e in sink2.events] == ["G:1"]