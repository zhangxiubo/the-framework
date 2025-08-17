import pytest
from framework.reactive import reactive, ReactiveBuilder, ReactiveEvent
from framework.core import Pipeline, AbstractProcessor


@reactive(provides=r".*", requires=[], cache=True)
class BuildItem(ReactiveBuilder):
    def build(self, context, target):
        yield f"built-{target}"

class Sink(AbstractProcessor):
    def __init__(self):
        super().__init__()
        self.events = []

    def process(self, context, event):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=artifact):
                self.events.append((target, artifact))


def test_per_target_cache_isolated(tmp_path):
    builder = BuildItem()
    sink = Sink()
    pipeline = Pipeline([builder, sink], strict_interest_inference=False, workspace=tmp_path)

    pipeline.submit(ReactiveEvent(name="resolve", target="A"))
    pipeline.submit(ReactiveEvent(name="resolve", target="B"))
    pipeline.run()

    assert sink.events == [("A", "built-A"), ("B", "built-B")]
    # Ensure per-target subdirectories are used for persistence and contain SQLite archives.
    assert (tmp_path / "A").is_dir()
    assert (tmp_path / "B").is_dir()
    assert (tmp_path / "A" / "BuildItem.sqlite").is_file()
    assert (tmp_path / "B" / "BuildItem.sqlite").is_file()


@reactive(provides="dict", requires=[], cache=True)
class BuildDict(ReactiveBuilder):
    def build(self, context, target):
        yield {"result": target}


def test_unhashable_artifacts(tmp_path):
    builder = BuildDict()
    sink = Sink()
    pipeline = Pipeline([builder, sink], strict_interest_inference=False, workspace=tmp_path)

    pipeline.submit(ReactiveEvent(name="resolve", target="dict"))
    pipeline.run()
    assert sink.events == [("dict", {"result": "dict"})]

    # second run should replay from cache without crashing on unhashable artifacts
    sink.events.clear()
    pipeline.submit(ReactiveEvent(name="resolve", target="dict"))
    pipeline.run()
    assert sink.events == [("dict", {"result": "dict"})]
