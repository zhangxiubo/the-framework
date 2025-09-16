import threading
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue
from typing import MutableMapping


# Ensure src is importable regardless of packaging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[1] / "src"))

from framework.core import AbstractProcessor, Event, Pipeline  # noqa: E402
from framework.reactive import (  # noqa: E402
    ReactiveBuilder,
    ReactiveEvent,
    StreamGrouper,
    Collector,
)


def run_dispatcher_once(pipeline: Pipeline, pulses: int = 1):
    q = SimpleQueue()

    def runner():
        with ThreadPoolExecutor(max_workers=2) as executor:
            pipeline.execute_events(executor, q)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    try:
        for _ in range(pulses):
            q.put(True)
            pipeline.wait()
    finally:
        q.put(False)
        t.join(timeout=5)


def test_reactive_source_publishes_on_resolve():
    # Intended: if requires == [], a resolve should trigger publish/build
    collected = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="A", artifact=a):
                    collected.append(a)

    class SourceA(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="A", requires=[], persist=False)

        def build(self, context, *args, **kwargs):
            yield from [1, 2, 3]

    pipe = Pipeline([SourceA(), Sink()])
    pipe.submit(ReactiveEvent(name="resolve", target="A"))
    run_dispatcher_once(pipe)

    # Intended assertion: should have published 1,2,3
    assert collected == [1, 2, 3]  # Expected to fail with current code


def test_reactive_new_handler_runs_once():
    calls = {"new": 0}

    class Bldr(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="B", requires=[], persist=False)

        def build(self, context, *args, **kwargs):
            yield from []

        def new(self, context, event):
            calls["new"] += 1
            super().new(context, event)

    pipe = Pipeline([Bldr()])
    pipe.submit(ReactiveEvent(name="resolve", target="B"))
    run_dispatcher_once(pipe)
    # second resolve
    pipe.submit(ReactiveEvent(name="resolve", target="B"))
    run_dispatcher_once(pipe)

    # Intended: new() runs once
    assert calls["new"] == 1  # Expected to fail due to handler removal bug


def test_stream_grouper_emits_previous_group_on_key_change():
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="G", artifact=artifact):
                    # ignore the initial (None, []) group
                    if artifact[0] is not None:
                        seen.append(artifact)

    g = StreamGrouper(provides="G", requires=["X"], keyfunc=lambda x: x // 10)
    pipe = Pipeline([g, Sink()])

    pipe.submit(ReactiveEvent(name="built", target="X", artifact=11))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=12))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=25))
    pipe.submit(ReactiveEvent(name="resolve", target="G"))
    run_dispatcher_once(pipe)

    assert seen == [(1, [(11,), (12,)])]


def test_stream_grouper_flushes_trailing_group_on_phase():
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="G", artifact=artifact):
                    if artifact[0] is not None:
                        seen.append(artifact)

    g = StreamGrouper(provides="G", requires=["X"], keyfunc=lambda x: x // 10)
    pipe = Pipeline([g, Sink()])

    pipe.submit(ReactiveEvent(name="built", target="X", artifact=31))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=32))
    pipe.submit(ReactiveEvent(name="resolve", target="G"))
    # phase should flush trailing group
    pipe.submit(Event(name="__PHASE__", phase=1))
    run_dispatcher_once(pipe)

    # Intended: trailing group (3, [(31,), (32,)]) emitted
    assert seen == [
        (3, [(31,), (32,)])
    ]  # Expected to fail; phase() yields instead of submitting


def test_collector_emits_on_phase_when_changed():
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="COL", artifact=a):
                    seen.append(a)

    col = Collector(forwards_to="COL", topics=["X"])
    pipe = Pipeline([col, Sink()])

    pipe.submit(ReactiveEvent(name="built", target="X", artifact=7))
    pipe.submit(ReactiveEvent(name="resolve", target="COL"))

    pipe.submit(Event(name="__PHASE__", phase=1))
    run_dispatcher_once(pipe, pulses=2)

    assert seen == [[(7,)]]

    # No change; another phase should not emit
    pipe.submit(Event(name="__PHASE__", phase=2))
    run_dispatcher_once(pipe)

    assert seen == [[(7,)]]

    # Add new item; next phase should emit new list
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=8))
    pipe.submit(Event(name="__PHASE__", phase=3))
    run_dispatcher_once(pipe, pulses=2)

    assert seen == [[(7,)], [(8,)]]


def test_listen_ignores_non_required_targets():
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="Y", artifact=a):
                    seen.append(a)

    class Bldr(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="Y", requires=["X"], persist=False)
            self.build_calls = 0

        def build(self, context, *args, **kwargs):
            self.build_calls += 1
            # if called, emit something (should not be called for unrelated targets)
            yield 999

    b = Bldr()
    pipe = Pipeline([b, Sink()])

    # send built for unrelated target; builder should ignore it
    pipe.submit(ReactiveEvent(name="built", target="Z", artifact=1))
    run_dispatcher_once(pipe)

    assert b.build_calls == 0
    assert seen == []


def test_get_cache_memoization_persist_false():
    # get_cache should return the same dict instance each time for persist=False
    class Bldr(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="Y", requires=["X"], persist=False)

        def build(self, context, *args, **kwargs):
            yield from []

    from framework.core import Context

    p = Pipeline([Bldr()])
    b = (
        p.processors[(None, None)].pop()
        if (None, None) in p.processors
        else list(p.processors.values())[0].pop()
    )
    # The above retrieves the single builder instance from the pipeline's registry
    ctx = Context(p)

    c1 = ctx.pipeline.archive(b.signature())
    c2 = ctx.pipeline.archive(b.signature())
    assert c1 is c2
    assert isinstance(c1, MutableMapping)


def test_get_cache_memoization_persist_true(tmp_path):
    # get_cache should return the same archive object each time for persist=True
    class Bldr(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="Y", requires=["X"], persist=True)

        def build(self, context, *args, **kwargs):
            yield from []

    from framework.core import Context

    p = Pipeline([Bldr()], workspace=tmp_path)
    # retrieve the single builder instance
    b = next(iter(next(iter(p.processors.values()))))
    ctx = Context(p)

    c1 = ctx.pipeline.archive(b.signature())
    c2 = ctx.pipeline.archive(b.signature())
    assert c1 is c2
    # klepto archive behaves like a mapping
    assert hasattr(c1, "__contains__") and hasattr(c1, "__getitem__")


def test_stream_grouper_multiple_transitions_without_phase_flush():
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="G", artifact=artifact):
                    if artifact[0] is not None:
                        seen.append(artifact)

    g = StreamGrouper(provides="G", requires=["X"], keyfunc=lambda x: x // 10)
    pipe = Pipeline([g, Sink()])

    # keys: 0, 1, 1, 2, 2 -> emissions on changes: (0, [(5,)]), (1, [(12,), (13,)])
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=5))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=12))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=13))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=27))
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=28))
    pipe.submit(ReactiveEvent(name="resolve", target="G"))
    run_dispatcher_once(pipe)

    # The first emission is for key None (ignored), then key changes to 1 -> emits key 0 group
    # Next key change to 2 -> emits key 1 group
    assert seen == [
        (0, [(5,)]),
        (1, [(12,), (13,)]),
    ]


def test_reactive_persist_true_reply_replays_from_cache(tmp_path):
    seen = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case ReactiveEvent(name="built", target="Y", artifact=a):
                    seen.append(a)

    class Bldr(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="Y", requires=["X"], persist=True)

        def build(self, context, x):
            # build emits x * 10
            yield x * 10

    b = Bldr()
    pipe = Pipeline([b, Sink()], workspace=tmp_path)

    # first, feed X -> triggers build and caches result
    pipe.submit(ReactiveEvent(name="built", target="X", artifact=2))

    run_dispatcher_once(pipe)

    assert seen == []

    # later, a resolve for Y should replay c    ached artifacts via reply()
    pipe.submit(ReactiveEvent(name="resolve", target="Y"))
    run_dispatcher_once(pipe)

    assert seen == [20]
