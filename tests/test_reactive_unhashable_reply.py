from collections.abc import Iterable

from framework.reactive import ReactiveBuilder, ReactiveEvent


class DummyContext:
    """Minimal context for ReactiveBuilder with cache=False."""

    class PipelineStub:
        def __init__(self, workspace=None):
            self.workspace = workspace

    def __init__(self, workspace=None):
        self.pipeline = DummyContext.PipelineStub(workspace=workspace)
        self.events = []

    def submit(self, event):
        self.events.append(event)


class DictArtifactBuilder(ReactiveBuilder):
    """
    Provides 'A' with no prerequisites and emits two unhashable duplicate artifacts (dict).
    Validates that reply() replays all cached artifacts without deduplication and without error.
    """

    def __init__(self):
        super().__init__(provides="A", requires=[], cache=False)

    def build(self, context, target: str, *args, **kwargs) -> Iterable[object]:
        # Emit two duplicate unhashable artifacts
        yield {"value": 1}
        yield {"value": 1}


def test_reply_replays_unhashable_and_duplicates_without_error():
    """
    After removing uniqueness filtering in reply(), it should:
    - Not raise on unhashable artifacts (dicts).
    - Re-emit all cached artifacts including duplicates upon resolve("A") requests.
    """
    builder = DictArtifactBuilder()
    ctx = DummyContext()

    # First resolve populates the cache and emits built events via publish; ignore those.
    builder.process(ctx, ReactiveEvent(name="resolve", target="A"))
    ctx.events.clear()

    # Second resolve should replay two duplicate dict artifacts from the cache via reply()
    builder.process(ctx, ReactiveEvent(name="resolve", target="A"))

    # We expect two events with duplicate dict artifacts
    built = [e for e in ctx.events if isinstance(e, ReactiveEvent) and e.name == "built" and e.target == "A"]
    assert len(built) == 2
    assert built[0].artifact == {"value": 1}
    assert built[1].artifact == {"value": 1}