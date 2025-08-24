import threading
from concurrent.futures import ThreadPoolExecutor
from queue import SimpleQueue

import pytest
from pydantic import BaseModel

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
    JsonLSink,
    JsonLSource,
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
    # phase should flush trailing group
    pipe.submit(Event(name="__PHASE__", phase=1))
    run_dispatcher_once(pipe)

    # Intended: trailing group (3, [(31,), (32,)]) emitted
    assert seen == [(3, [(31,), (32,)])]  # Expected to fail; phase() yields instead of submitting


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

    assert seen == [[(7,)], [(7,), (8,)]]


def test_jsonlsink_writes_and_closes_on_poison(tmp_path):
    class Item(BaseModel):
        a: int

    out_path = tmp_path / "out.jsonl"
    sink = JsonLSink(name="X", filepath=str(out_path))
    pipe = Pipeline([sink])

    pipe.submit(ReactiveEvent(name="built", target="X", artifact=Item(a=1)))
    run_dispatcher_once(pipe)

    # send poison to trigger on_terminate (close)
    pipe.submit(Event(name="__POISON__"))
    run_dispatcher_once(pipe)

    text = out_path.read_text().strip().splitlines()
    assert text == ['{"a":1}']


def test_jsonlsource_build_yields_items_direct_call(tmp_path):
    class Item(BaseModel):
        a: int

    p = tmp_path / "in.jsonl"
    p.write_text('{"a":1}\n{"a":2}\n{"a":3}\n')

    src = JsonLSource(name="X", filepath=str(p), itemtype=Item, limit=2)
    items = list(src.build(context=None))
    assert [i.a for i in items] == [1, 2]