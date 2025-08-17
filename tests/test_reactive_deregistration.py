from collections.abc import Iterable

from framework.reactive import ReactiveBuilder, ReactiveEvent


class DummyContext:
    """Minimal context stub sufficient for ReactiveBuilder under cache=False."""

    class PipelineStub:
        def __init__(self, workspace=None):
            self.workspace = workspace

    def __init__(self, workspace=None):
        self.pipeline = DummyContext.PipelineStub(workspace=workspace)
        self.events = []

    def submit(self, event):
        # Record but don't dispatch; not needed for this regression reproduction.
        self.events.append(event)


class ProbeBuilder(ReactiveBuilder):
    """ReactiveBuilder subclass that counts how many times new() is invoked."""

    def __init__(self):
        super().__init__(provides="A", requires=["X"], cache=False)
        self.new_calls = 0

    def new(self, context, event):
        match event:
            case ReactiveEvent(name="resolve", target=target) if self.matcher.fullmatch(
                target
            ):
                self.new_calls += 1
        return super().new(context, event)

    def build(self, context, target: str, *args, **kwargs) -> Iterable[object]:
        if False:
            yield  # pragma: no cover


def test_new_handler_is_not_deregistered_for_bound_methods():
    """
    Demonstrates the deregistration bug in ReactiveBuilder.new():
    - handlers is a set of bound methods {self.new, self.reply, self.listen}
    - subtracting {self.new} does not remove the originally stored bound method
      because a new bound method object is created per attribute access
    - result: new() runs again on subsequent resolve("A")
    """
    builder = ProbeBuilder()
    ctx = DummyContext()

    builder.process(ctx, ReactiveEvent(name="resolve", target="A"))
    assert builder.new_calls == 1, "sanity: first resolve calls new exactly once"

    builder.process(ctx, ReactiveEvent(name="resolve", target="A"))
    # If deregistration worked, new_calls would remain 1. It increments to 2 currently.
    assert builder.new_calls == 1, (
        "BUG: `new` handler was not deregistered and was invoked again on second resolve"
    )