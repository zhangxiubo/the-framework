import tempfile
from typing import Any, List

from framework.core import Pipeline, AbstractProcessor, Context
from framework.reactive import ReactiveEvent, ReactiveBuilder, reactive

# A simple sink to collect results for verification.
class TestSink(AbstractProcessor):
    def __init__(self, topic: str):
        super().__init__()
        self.topic = topic
        self.artifacts = []

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact) if target == self.topic:
                self.artifacts.append(artifact)

# A simple producer to generate input data.
class TestProducer(AbstractProcessor):
    def __init__(self, topic: str, values: List[Any]):
        super().__init__()
        self.topic = topic
        self.values = values

    def process(self, context: Context, event: ReactiveEvent):
        match event:
            case ReactiveEvent(name="resolve", target=target) if target == self.topic:
                for v in self.values:
                    context.submit(ReactiveEvent(name="built", target=self.topic, artifact=v))

# The reactive builder under test, with caching enabled.
@reactive(provides="B", requires=["A"], cache=True)
class SimpleCacheableBuilder(ReactiveBuilder):
    build_call_count = 0

    def build(self, context: Context, target: str, a: int):
        SimpleCacheableBuilder.build_call_count += 1
        yield f"processed:{a}"

def test_persistent_caching_avoids_rebuild():
    # Reset the counter for a clean test run.
    SimpleCacheableBuilder.build_call_count = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # --- First run: Cache Miss ---
        sink1 = TestSink("B")
        p1 = Pipeline(
            processors=[
                TestProducer("A", [1, 2]),
                SimpleCacheableBuilder(),
                sink1,
            ],
            workspace=tmpdir,
        )
        p1.submit(ReactiveEvent(name="resolve", target="B"))
        p1.run()

        # Check that the build method was called for both inputs.
        assert SimpleCacheableBuilder.build_call_count == 2
        assert sorted(sink1.artifacts) == ["processed:1", "processed:2"]

        # --- Second run: Cache Hit ---
        # We create a new pipeline with new component instances, but use the same workspace.
        sink2 = TestSink("B")
        p2 = Pipeline(
            processors=[
                TestProducer("A", [1, 2]),
                SimpleCacheableBuilder(),
                sink2,
            ],
            workspace=tmpdir,
        )
        p2.submit(ReactiveEvent(name="resolve", target="B"))
        p2.run()

        # The key assertion: the build method should NOT have been called again.
        assert SimpleCacheableBuilder.build_call_count == 2

        # And the sink should have received the results, which were served from the cache.
        assert sorted(sink2.artifacts) == ["processed:1", "processed:2"]
