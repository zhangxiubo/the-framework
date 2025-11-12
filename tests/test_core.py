import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from queue import PriorityQueue, SimpleQueue

import pytest

# Ensure src is importable regardless of packaging
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[1] / "src"))

from framework.core import (  # noqa: E402
    AbstractProcessor,
    Context,
    Event,
    Inbox,
    Pipeline,
    caching,
    retry,
)


def run_dispatcher_once(pipeline: Pipeline):
    """
    Start a dispatcher thread, pulse once, wait for all work,
    then stop dispatcher.
    """
    q = SimpleQueue()

    def runner():
        with ThreadPoolExecutor(max_workers=2) as executor:
            pipeline.execute_events(executor, q)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    try:
        q.put(True)
        pipeline.wait()
    finally:
        q.put(False)
        t.join(timeout=5)


def test_retry_succeeds_before_max_attempts(caplog):
    caplog.set_level(logging.DEBUG)

    calls = {"n": 0}

    @retry(3)
    def sometimes():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("not yet")
        return "ok"

    assert sometimes() == "ok"
    assert calls["n"] == 3
    # ensure logs mention attempts
    attempt_logs = [rec for rec in caplog.records if "attempt" in rec.getMessage()]
    assert any("attempt 1 / 3" in r.getMessage() for r in attempt_logs)


def test_retry_raises_after_exhaustion(caplog):
    caplog.set_level(logging.DEBUG)

    @retry(2)
    def always_fail():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        always_fail()

    msgs = [r.getMessage() for r in caplog.records]
    assert any("attempt 1 / 2" in m and "retrying" in m for m in msgs)
    assert any("attempt 2 / 2" in m and "escalating" in m for m in msgs)


def test_context_submit_records_and_delegates():
    class PipeStub:
        def __init__(self):
            self.received = []

        def submit(self, e):
            self.received.append(e)
            return 0

    pipe = PipeStub()
    ctx = Context(pipe)
    e1 = Event(name="A", x=1)
    e2 = Event(name="B", y=2)

    ctx.submit(e1)
    ctx.submit(e2)

    assert ctx.events == [e1, e2]
    assert pipe.received == [e1, e2]


def test_done_callback_marks_done_and_wakes_dispatcher():
    class InboxStub:
        def __init__(self):
            self.count = 0

        def mark_task_done(self):
            self.count += 1

    class PipeStub:
        def __init__(self):
            self.dec = 0

        def decrement(self):
            self.dec += 1

    class P(AbstractProcessor):
        def process(self, context, event):
            pass

    pipe = PipeStub()
    ctx = Context(pipe)
    inbox = InboxStub()
    q = SimpleQueue()
    proc = P()
    fut = Future()
    fut.set_result(None)

    ctx.done_callback(fut, proc, Event(name="X"), inbox, q)

    assert inbox.count == 1
    assert pipe.dec == 1
    assert q.get(timeout=1) is True


def test_inbox_scheduling_concurrency_one():
    ready = PriorityQueue()

    class DummyProc:
        def __init__(self, priority):
            self.priority = priority

        def __repr__(self):
            return "DummyProc"

    inbox = Inbox(DummyProc(priority=5), ready, concurrency=1)

    e1 = Event(name="E1")
    e2 = Event(name="E2")

    inbox.put_event(e1)
    # first put should signal readiness
    entry = ready.get_nowait()
    assert entry.processor.priority == 5
    # second put should NOT enqueue because slot is taken
    inbox.put_event(e2)
    with pytest.raises(Exception):
        # queue is empty now
        ready.get_nowait()
    # marking done should enqueue again because there are pending jobs
    inbox.mark_task_done()
    entry2 = ready.get_nowait()
    assert entry2.processor.priority == 5


def test_pipeline_interest_inference_and_submit_routing():
    class ProcX(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass

    class ProcGeneric(AbstractProcessor):
        def process(self, context, event):
            # No match-case -> (None, None) interest
            return None

    p_strict = Pipeline([ProcX(), ProcGeneric()], strict_interest_inference=True)
    p_default = Pipeline([ProcX(), ProcGeneric()], strict_interest_inference=False)

    assert p_strict.submit(Event(name="X")) == 1  # only ProcX
    assert p_default.submit(Event(name="X")) == 2  # ProcX + generic


def test_pipeline_execute_events_processes_ready_processors():
    ran = []

    class RecorderProc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    ran.append("ok")

    pipe = Pipeline([RecorderProc()])
    pipe.submit(Event(name="X"))
    run_dispatcher_once(pipe)
    assert ran == ["ok"]
    # all jobs accounted
    assert pipe.done >= pipe.jobs


def test_pipeline_run_emits_phase_events():
    # Intended: run() should submit __PHASE__ events at least once
    phases = []

    class PhaseRecorder(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__PHASE__", phase=p):
                    phases.append(p)

    pipe = Pipeline([PhaseRecorder()])
    pipe.run()
    # Intended assertion: at least one phase should be observed
    assert len(phases) >= 1  # Expected to fail with current code


def test_caching_persists_and_replays_with_workspace(tmp_path):
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class CachedProc(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.calls = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="DO", ):
                    self.calls += 1
                    context.submit(Event(name="OUT", value=42))

    cp = CachedProc()
    pipe = Pipeline([cp, Sink()], workspace=tmp_path)

    # First submission: executes and emits
    pipe.submit(Event(name="DO", payload=1))
    run_dispatcher_once(pipe)

    # Second submission: should be replayed from cache
    pipe.submit(Event(name="DO", payload=1))
    run_dispatcher_once(pipe)

    assert cp.calls == 1
    assert outs == [42, 42]
def test_infer_interests_match_or_routing():
    ran = []

    class OrProc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="A") | Event(name="B"):
                    ran.append(event.name)

    class Sink(AbstractProcessor):
        def process(self, context, event):
            pass

    p = Pipeline([OrProc(), Sink()], strict_interest_inference=True)
    p.submit(Event(name="A"))
    p.submit(Event(name="B"))
    run_dispatcher_once(p)
    assert ran == ["A", "B"]


def test_inbox_take_event_empty_raises():
    from queue import PriorityQueue

    class Proc:
        def __init__(self):
            self.priority = 0
        def __repr__(self):
            return "Proc"

    inbox = Inbox(Proc(), PriorityQueue(), concurrency=1)
    import pytest
    with pytest.raises(RuntimeError):
        inbox.take_event()


def test_timeit_logs_debug(caplog):
    import logging
    from framework.core import timeit as timeit_ctx

    logger = logging.getLogger("framework.core.tests.timeit")
    caplog.set_level(logging.DEBUG, logger=logger.name)

    with timeit_ctx("unit-test-block", logger):
        pass

    msgs = [r.getMessage() for r in caplog.records if r.name == logger.name]
    assert any("unit-test-block finished after" in m for m in msgs)


def test_caching_no_workspace_no_replay():
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class CachedNoWS(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.calls = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="IN", data=d):
                    self.calls += 1
                    context.submit(Event(name="OUT", value=d * 2))

    proc = CachedNoWS()
    p = Pipeline([proc, Sink()])  # workspace=None -> no replay
    p.submit(Event(name="IN", data=1))
    run_dispatcher_once(p)
    p.submit(Event(name="IN", data=1))
    run_dispatcher_once(p)

    assert proc.calls == 1
    assert outs == [2, 2]


def test_caching_exception_then_replay(tmp_path):
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class SometimesFails(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.calls = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="TRIGGER", ):
                    self.calls += 1
                    if self.calls == 1:
                        raise RuntimeError("first attempt fails")
                    context.submit(Event(name="OUT", value=99))

    proc = SometimesFails()
    p = Pipeline([proc, Sink()], workspace=tmp_path)

    # 1st call: fails, no cache
    import pytest
    with pytest.raises(RuntimeError):
        # submit+dispatcher to surface exception via done_callback logging; we directly invoke process to raise in test
        proc.process(Context(p), Event(name="TRIGGER", key="same"))  # raise directly

    # 2nd call: success and caches events
    p.submit(Event(name="TRIGGER", key="same"))
    run_dispatcher_once(p)

    # 3rd call: should replay, not execute process
    p.submit(Event(name="TRIGGER", key="same"))
    run_dispatcher_once(p)

    assert proc.calls == 2  # third call replayed
    assert outs == [99, 99]
