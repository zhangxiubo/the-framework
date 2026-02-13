import logging
import threading
from concurrent.futures import Future
from queue import Empty, PriorityQueue

import pytest

from framework.core import (
    AbstractProcessor,
    Context,
    Event,
    Inbox,
    Pipeline,
    ProcessEntry,
    caching,
    retry,
)


def run_dispatcher_once(pipeline: Pipeline):
    """
    Start a dispatcher thread, pulse once, wait for all work,
    then stop dispatcher.
    """
    q = pipeline.q

    def runner():
        pipeline.execute_events()

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
    signal = threading.Event()
    proc = P()
    fut = Future()
    fut.set_result(None)

    ctx.done_callback(fut, proc, Event(name="X"), inbox, signal=signal)

    assert inbox.count == 1
    assert pipe.dec == 1
    assert signal.is_set()


def test_inbox_single_active_execution_model():
    class DummyProc:
        def __init__(self, priority):
            self.priority = priority

        def __repr__(self):
            return "DummyProc"

    ready = PriorityQueue()
    inbox = Inbox(DummyProc(priority=5), ready)

    e1 = Event(name="E1")
    e2 = Event(name="E2")
    inbox.put_event(e1)
    inbox.put_event(e2)

    # First enqueue should make processor ready.
    first_ready = ready.get_nowait()
    assert first_ready.processor.priority == 5

    # Slot is occupied; second enqueue should not add another ready entry yet.
    with pytest.raises(Exception):
        ready.get_nowait()

    # Dispatcher takes first event and completion re-signals readiness for second.
    assert inbox.take_event() is e1
    inbox.mark_task_done()
    second_ready = ready.get_nowait()
    assert second_ready.processor.priority == 5

    assert inbox.take_event() is e2
    inbox.mark_task_done()


def test_pipeline_respects_processor_priority_order():
    seen = []

    class HighPriority(AbstractProcessor):
        def __init__(self):
            super().__init__(priority=10)

        def process(self, context, event):
            match event:
                case Event(name="X"):
                    seen.append("high")

    class LowPriority(AbstractProcessor):
        def __init__(self):
            super().__init__(priority=0)

        def process(self, context, event):
            match event:
                case Event(name="X"):
                    seen.append("low")

    # max_workers=1 makes dispatch order observable and deterministic.
    p = Pipeline(
        [HighPriority(), LowPriority()],
        strict_interest_inference=True,
        max_workers=1,
    )
    p.submit(Event(name="X"))
    run_dispatcher_once(p)

    assert seen == ["low", "high"]


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


def test_strict_interest_matches_event_subclasses():
    class CustomEvent(Event):
        pass

    seen = []

    class BaseMatcher(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="ping"):
                    seen.append(event)
         

    pipe = Pipeline([BaseMatcher()], strict_interest_inference=True)
    pipe.submit(CustomEvent(name="ping"))
    run_dispatcher_once(pipe)

    # BaseMatcher should see subclassed events even though it pattern-matches Event.
    assert seen


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
    phases = []

    class PhaseRecorder(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__PHASE__", phase=p):
                    phases.append(p)

    pipe = Pipeline([PhaseRecorder()])
    pipe.run()
    assert phases


def test_pipeline_run_fixed_point_stops_after_single_phase_when_no_phase_work():
    phases = []

    class PhaseRecorder(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__PHASE__", phase=p):
                    phases.append(p)

    pipe = Pipeline([PhaseRecorder()], strict_interest_inference=True)
    pipe.run()

    assert phases == [1]


def test_pipeline_run_fixed_point_advances_until_phase_generated_work_stops():
    phases = []
    pings = []

    class PhaseProducer(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__PHASE__", phase=p):
                    phases.append(p)
                    if p == 1:
                        context.submit(Event(name="PING"))
                case Event(name="PING"):
                    pings.append("seen")

    pipe = Pipeline([PhaseProducer()], strict_interest_inference=True)
    pipe.run()

    assert phases == [1, 2]
    assert pings == ["seen"]


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
    class Proc:
        def __init__(self):
            self.priority = 0

        def __repr__(self):
            return "Proc"

    inbox = Inbox(Proc(), PriorityQueue())
    with pytest.raises(RuntimeError):
        inbox.take_event()


def test_pipeline_pulse_dispatcher_deduplicates_pending_pulse():
    class Proc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass

    pipe = Pipeline([Proc()], strict_interest_inference=True)

    # Drain any bootstrap pulse left from initialization.
    while True:
        try:
            pipe.q.get(block=False)
        except Empty:
            break

    pipe.pulse_dispatcher()
    pipe.pulse_dispatcher()
    pipe.pulse_dispatcher()

    assert pipe.q.get(block=False) is True
    with pytest.raises(Empty):
        pipe.q.get(block=False)


def test_pipeline_sets_fatal_error_on_stale_ready_entry():
    class Proc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass

    processor = Proc()
    pipe = Pipeline([processor], strict_interest_inference=True)
    stale_entry = ProcessEntry(priority=processor.priority, processor=processor)

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=1) as executor:
        pipe.dispatch_entry(stale_entry, executor)

    with pytest.raises(RuntimeError, match="internal dispatch error"):
        pipe.wait()


def test_context_blocks_late_submission_after_task_completion():
    captured = {}

    class Proc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    captured["context"] = context

    pipe = Pipeline([Proc()], strict_interest_inference=True)
    pipe.submit(Event(name="X"))
    run_dispatcher_once(pipe)

    with pytest.raises(RuntimeError, match="late/unsafe context.submit"):
        captured["context"].submit(Event(name="Y"))


def test_context_blocks_cross_thread_submission_while_task_active():
    captures = {"errors": []}

    class Proc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    done = threading.Event()

                    def worker():
                        try:
                            context.submit(Event(name="Y"))
                        except RuntimeError as exc:
                            captures["errors"].append(str(exc))
                        finally:
                            done.set()

                    t = threading.Thread(target=worker)
                    t.start()
                    done.wait(timeout=2)
                    t.join(timeout=2)

    pipe = Pipeline([Proc()], strict_interest_inference=True)
    pipe.submit(Event(name="X"))
    run_dispatcher_once(pipe)

    assert captures["errors"]
    assert "cross-thread submissions are forbidden" in captures["errors"][0]


def test_timeit_logs_debug(caplog):
    import logging
    from framework.core import timeit as timeit_ctx

    logger = logging.getLogger("framework.core.tests.timeit")
    caplog.set_level(logging.DEBUG, logger=logger.name)

    with timeit_ctx("unit-test-block", logger, logging_threshold=0):
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
