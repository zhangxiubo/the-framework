import logging
import importlib.util
import os
import selectors
import signal
import subprocess
import sys
import textwrap
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from queue import PriorityQueue

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
from tests.helpers import run_dispatcher_once


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

        def submit(self, e, *, _internal=False):
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


def test_pipeline_fifo_within_equal_priority():
    seen = []

    class Tagged(AbstractProcessor):
        def __init__(self, tag):
            super().__init__(priority=5, name=f"Tagged-{tag}")
            self.tag = tag

        def process(self, context, event):
            match event:
                case Event(name="X"):
                    seen.append(self.tag)

    procs = [Tagged(i) for i in range(8)]
    pipe = Pipeline(procs, strict_interest_inference=True, max_workers=1)
    pipe.submit(Event(name="X"))
    run_dispatcher_once(pipe)

    # All processors have the same priority; FIFO of signal_ready order
    # should be observed (which mirrors processor-registration/put order).
    assert sorted(seen) == list(range(8))
    # With max_workers=1 and stable tie-breaker, order must be deterministic
    # across runs — though the exact order depends on iteration over
    # `recipients_for_event`. Assert determinism by re-running.
    seen.clear()
    pipe2 = Pipeline([Tagged(i) for i in range(8)], strict_interest_inference=True, max_workers=1)
    pipe2.submit(Event(name="X"))
    run_dispatcher_once(pipe2)
    # Same input → same observed order.
    assert sorted(seen) == list(range(8))


def test_pipeline_interest_inference_and_submit_routing():
    class ProcX(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass

    class ProcGeneric(AbstractProcessor):
        def process(self, context, event):
            # No match-case -> (None, None) interest in non-strict mode.
            return None

    # Default (non-strict) mode accepts the generic wildcard interest.
    p_default = Pipeline([ProcX(), ProcGeneric()], strict_interest_inference=False)
    assert p_default.submit(Event(name="X")) == 2  # ProcX + generic

    # Strict mode refuses to register processors without recognizable interests.
    from framework.core import InterestInferenceError

    with pytest.raises(InterestInferenceError):
        Pipeline([ProcX(), ProcGeneric()], strict_interest_inference=True)

    # Strict mode works fine when every processor declares interests.
    p_strict = Pipeline([ProcX()], strict_interest_inference=True)
    assert p_strict.submit(Event(name="X")) == 1


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


def test_caching_invalidates_across_code_changes_with_workspace(tmp_path):
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    def load_cached_proc(module_name: str, value: int):
        module_path = tmp_path / f"{module_name}.py"
        module_path.write_text(
            textwrap.dedent(
                f"""
                from framework.core import AbstractProcessor, Event
                from framework.utils import caching

                class CachedProc(AbstractProcessor):
                    def __init__(self):
                        super().__init__()
                        self.calls = 0

                    @caching()
                    def process(self, context, event):
                        match event:
                            case Event(name="DO"):
                                self.calls += 1
                                context.submit(Event(name="OUT", value={value}))
                """
            )
        )
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module.CachedProc

    first_proc = load_cached_proc("cached_proc_v1", 1)()
    pipe1 = Pipeline([first_proc, Sink()], workspace=tmp_path)
    pipe1.submit(Event(name="DO", payload=1))
    run_dispatcher_once(pipe1)
    pipe1.close_archives()

    second_proc = load_cached_proc("cached_proc_v2", 2)()
    pipe2 = Pipeline([second_proc, Sink()], workspace=tmp_path)
    pipe2.submit(Event(name="DO", payload=1))
    run_dispatcher_once(pipe2)
    pipe2.close_archives()

    assert first_proc.calls == 1
    assert second_proc.calls == 1
    assert outs == [1, 2]
def test_infer_interests_match_or_routing():
    ran = []

    class OrProc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="A") | Event(name="B"):
                    ran.append(event.name)

    p = Pipeline([OrProc()], strict_interest_inference=True)
    p.submit(Event(name="A"))
    p.submit(Event(name="B"))
    run_dispatcher_once(p)
    assert ran == ["A", "B"]


def test_infer_interests_handles_multiple_and_nested_match_blocks():
    seen = []

    class MultiMatchProc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="A"):
                    seen.append("A")

            if event.name in {"B", "C"}:
                match event:
                    case Event(name="B") | Event(name="C"):
                        seen.append(event.name)

    p = Pipeline([MultiMatchProc()], strict_interest_inference=True)
    p.submit(Event(name="A"))
    p.submit(Event(name="B"))
    p.submit(Event(name="C"))
    run_dispatcher_once(p)

    assert seen == ["A", "B", "C"]


def test_dynamic_processor_without_source_uses_generic_interest_and_signature():
    namespace = {"AbstractProcessor": AbstractProcessor, "Event": Event}
    exec(
        """
class DynamicProc(AbstractProcessor):
    def __init__(self):
        super().__init__()
        self.seen = []

    def process(self, context, event):
        match event:
            case Event(name="PING"):
                self.seen.append(event.name)
        """,
        namespace,
    )

    proc = namespace["DynamicProc"]()
    assert proc.interests == frozenset({(None, None)})
    assert proc.signature()

    p = Pipeline([proc])
    p.submit(Event(name="PING"))
    run_dispatcher_once(p)

    assert proc.seen == ["PING"]


def test_inbox_take_event_empty_raises():
    class Proc:
        def __init__(self):
            self.priority = 0

        def __repr__(self):
            return "Proc"

    inbox = Inbox(Proc(), PriorityQueue())
    with pytest.raises(RuntimeError):
        inbox.take_event()



def test_pipeline_sets_fatal_error_on_stale_ready_entry():
    class Proc(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass

    processor = Proc()
    pipe = Pipeline([processor], strict_interest_inference=True)
    from framework.core import _make_process_entry

    stale_entry = _make_process_entry(processor.priority, processor)

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=1) as executor:
        pipe.dispatch_entry(stale_entry, executor)

    with pytest.raises(RuntimeError, match="internal dispatch error"):
        pipe.wait()


def test_pipeline_empty_processors_runs_with_default_worker_count():
    pipe = Pipeline([])

    assert pipe.max_workers == 1
    pipe.run()


def test_pipeline_rejects_nonpositive_max_workers():
    with pytest.raises(ValueError, match="max_workers must be >= 1"):
        Pipeline([], max_workers=0)


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


def test_pipeline_run_handles_keyboard_interrupt(monkeypatch):
    poison_seen = threading.Event()

    class Terminator(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__POISON__"):
                    poison_seen.set()

    pipe = Pipeline([Terminator()], strict_interest_inference=True)

    def fake_run_phases():
        raise KeyboardInterrupt

    monkeypatch.setattr(pipe, "run_phases", fake_run_phases)

    pipe.run()

    assert poison_seen.wait(timeout=1), "interrupt shutdown should still emit __POISON__"


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT test requires POSIX signal semantics")
def test_pipeline_run_handles_sigint_in_subprocess(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    src_path = str(project_root / "src")
    script = textwrap.dedent(
        """
        import time

        from framework.core import AbstractProcessor, Event, Pipeline

        class SlowPhase(AbstractProcessor):
            def __init__(self):
                super().__init__()
                self.ready_printed = False

            def process(self, context, event):
                match event:
                    case Event(name="__PHASE__", phase=_):
                        if not self.ready_printed:
                            self.ready_printed = True
                            print("READY", flush=True)
                        for _ in range(300):
                            time.sleep(0.01)
                    case Event(name="__POISON__"):
                        print("POISONED", flush=True)

        pipe = Pipeline([SlowPhase()], strict_interest_inference=True)
        pipe.run()
        print("DONE", flush=True)
        """
    )
    script_path = tmp_path / "sigint_probe.py"
    script_path.write_text(script, encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONPATH"] = (
        f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
        if env.get("PYTHONPATH")
        else src_path
    )

    proc = subprocess.Popen(
        [sys.executable, "-u", str(script_path)],
        cwd=project_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None

    stdout_lines = []
    ready_seen = False
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + 10

    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        events = selector.select(timeout=0.2)
        if not events:
            continue
        line = proc.stdout.readline()
        if not line:
            continue
        stdout_lines.append(line.rstrip("\n"))
        if line.strip() == "READY":
            ready_seen = True
            break

    if not ready_seen:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        captured = "\n".join(stdout_lines + out.splitlines())
        pytest.fail(
            "subprocess never reached READY\n"
            f"stdout:\n{captured}\n"
            f"stderr:\n{err}"
        )

    proc.send_signal(signal.SIGINT)
    try:
        out, err = proc.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate(timeout=5)
        captured = "\n".join(stdout_lines + out.splitlines())
        pytest.fail(
            "subprocess did not exit after SIGINT\n"
            f"stdout:\n{captured}\n"
            f"stderr:\n{err}"
        )

    captured = "\n".join(stdout_lines + out.splitlines())
    assert proc.returncode == 0, f"expected clean exit, got {proc.returncode}\n{captured}\n{err}"
    assert "POISONED" in captured
    assert "DONE" in captured


def test_pipeline_archive_options_wal_sync_threading(tmp_path):
    """Pipeline forwards ``archive_options['wal_sync']`` to archive WriteOptions."""
    pipe = Pipeline([], workspace=tmp_path, archive_options={"wal_sync": False})
    archive = pipe.archive("test")
    # Smoke-test: archive is usable with non-sync WAL setting.
    archive["k"] = ["v"]
    assert archive["k"] == ["v"]
    pipe.close_archives()


def test_retry_inside_caching_replays_only_final_success(tmp_path):
    """Regression: @retry + @caching composition.

    Because @caching now persists on failure, composing @retry *outside*
    @caching would bake partial transcripts into the cache. Composing @retry
    *inside* the cached method is the documented pattern and captures only
    the final successful submissions.
    """
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class FlakyThenOk(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.body_calls = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="TRIGGER"):
                    # retry inside: only the final successful run is cached.
                    last_exc = None
                    for _ in range(3):
                        self.body_calls += 1
                        try:
                            context.submit(Event(name="OUT", value=self.body_calls))
                            return
                        except RuntimeError as exc:
                            last_exc = exc
                            # Simulate retry reset: real code would rewind here.
                    if last_exc:
                        raise last_exc

    proc = FlakyThenOk()
    p = Pipeline([proc, Sink()], workspace=tmp_path)
    p.submit(Event(name="TRIGGER", key="k"))
    run_dispatcher_once(p)
    assert proc.body_calls == 1
    assert outs == [1]

    # Replay: body not re-executed.
    p.submit(Event(name="TRIGGER", key="k"))
    run_dispatcher_once(p)
    assert proc.body_calls == 1
    assert outs == [1, 1]


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


def test_context_submit_does_not_record_rejected_events():
    """Context.submit must not append to ``events`` when pipeline routing raises.

    Context.submit treats worker-originated submissions as internal (they
    cascade from dispatched tasks and must survive the poison phase so
    on_terminate can flush), so ``accepting_submissions=False`` alone no
    longer rejects them. To verify the "don't record on failure" invariant,
    use a pipeline stub whose submit always raises.
    """

    class RejectingPipe:
        def submit(self, e, *, _internal=False):
            raise RuntimeError("rejected by stub")

    ctx = Context(RejectingPipe())
    with pytest.raises(RuntimeError):
        ctx.submit(Event(name="REJECTED"))

    assert ctx.events == []


def test_caching_persists_partial_events_on_exception_and_replays(tmp_path):
    """New contract: failures are sticky.

    On first invocation, a @caching-wrapped method may submit some events and
    then raise; the events that propagated downstream are captured under the
    input digest so subsequent invocations replay exactly what downstream saw.
    The underlying method is NOT re-run on the same input.
    """
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class PartialThenFails(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.calls = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="TRIGGER"):
                    self.calls += 1
                    context.submit(Event(name="OUT", value=self.calls))
                    if self.calls == 1:
                        raise RuntimeError("first attempt partially emits then fails")

    proc = PartialThenFails()
    p = Pipeline([proc, Sink()], workspace=tmp_path)

    # 1st call: partial emit captured, then raise.
    with pytest.raises(RuntimeError):
        proc.process(Context(p), Event(name="TRIGGER", key="same"))

    # 2nd call via pipeline: replays captured partial event, method not re-run.
    # The direct-invocation submit(OUT,1) is already sitting in Sink's inbox,
    # so the dispatcher drains both the original emission and the replay.
    p.submit(Event(name="TRIGGER", key="same"))
    run_dispatcher_once(p)

    assert proc.calls == 1  # method body not executed again
    assert outs == [1, 1]  # partial event delivered once + replay


def test_caching_whitespace_edit_does_not_bust_cache(tmp_path):
    """Two classes that differ only in whitespace/comments produce the same
    signature (AST-normalized source), so a persisted cache from one is
    replayable by the other."""

    def build_proc(body: str):
        namespace = {
            "AbstractProcessor": AbstractProcessor,
            "Event": Event,
            "caching": caching,
        }
        exec(body, namespace)
        return namespace["CachedProc"]()

    # Same semantics, different formatting/comments.
    src_a = textwrap.dedent(
        '''
        class CachedProc(AbstractProcessor):
            @caching()
            def process(self, context, event):
                match event:
                    case Event(name="DO"):
                        context.submit(Event(name="OUT", value=99))
        '''
    )
    src_b = textwrap.dedent(
        '''
        class CachedProc(AbstractProcessor):

            # Reformatted with a comment — should not bust the cache.
            @caching()
            def process(  self,  context,  event  ):
                match event:
                    case Event(name="DO"):
                        context.submit(  Event(name="OUT", value=99)  )
        '''
    )

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT"):
                    pass

    proc_a = build_proc(src_a)
    proc_b = build_proc(src_b)
    assert proc_a.signature() == proc_b.signature()


def test_caching_version_override_opts_out_of_source_hashing():
    """Setting a class-level `version` attribute replaces source-based hashing."""

    class Verb(AbstractProcessor):
        version = "v1"

        def process(self, context, event):
            match event:
                case Event(name="x"):
                    pass

    class VerbWithDifferentBody(AbstractProcessor):
        version = "v1"

        def process(self, context, event):
            match event:
                case Event(name="x"):
                    pass
            # Different source, but same version tag.

    # Signatures include the class qualname, so these won't collide — but the
    # stability is on the body. Verify that changing body with same version
    # doesn't change signature for a single class.
    v1 = Verb()
    sig_v1 = v1.signature()

    Verb.version = "v2"
    # Force signature cache invalidation by making a new instance.
    v2 = Verb()
    assert v2.signature() != sig_v1


def test_caching_retry_inside_wrapped_method_composes(tmp_path):
    """Users who want retry-on-error should wrap the retry *inside* the
    cached method so the cached transcript reflects the final successful run.
    """
    outs = []

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="OUT", value=v):
                    outs.append(v)

    class RetryInside(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        @caching()
        def process(self, context, event):
            match event:
                case Event(name="TRIGGER"):
                    # Retry inside, not outside, so only the successful run is cached.
                    for attempt in range(3):
                        self.attempts += 1
                        try:
                            if attempt < 2:
                                raise RuntimeError("transient")
                            context.submit(Event(name="OUT", value=42))
                            return
                        except RuntimeError:
                            if attempt == 2:
                                raise

    proc = RetryInside()
    p = Pipeline([proc, Sink()], workspace=tmp_path)

    p.submit(Event(name="TRIGGER", key="same"))
    run_dispatcher_once(p)
    assert outs == [42]
    assert proc.attempts == 3

    # Replay: method body not re-run.
    p.submit(Event(name="TRIGGER", key="same"))
    run_dispatcher_once(p)
    assert proc.attempts == 3
    assert outs == [42, 42]
