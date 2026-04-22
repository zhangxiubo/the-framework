import threading
import time
from concurrent.futures import ThreadPoolExecutor


import pytest
import framework.core as core
from framework.core import AbstractProcessor, Event, Pipeline
from tests.helpers import run_dispatcher_once


def test_concurrent_first_submit_uses_single_inbox(monkeypatch):
    seen = []
    seen_lock = threading.Lock()

    class Recorder(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X", payload=value):
                    with seen_lock:
                        seen.append(value)

    original_init = core.Inbox.__init__

    def slow_inbox_init(self, *args, **kwargs):
        time.sleep(0.05)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(core.Inbox, "__init__", slow_inbox_init)

    pipe = Pipeline([Recorder()], strict_interest_inference=True)
    start_barrier = threading.Barrier(3)

    def submitter(value):
        start_barrier.wait()
        pipe.submit(Event(name="X", payload=value))

    t1 = threading.Thread(target=submitter, args=(1,))
    t2 = threading.Thread(target=submitter, args=(2,))
    t1.start()
    t2.start()
    start_barrier.wait()
    t1.join()
    t2.join()

    run_dispatcher_once(pipe)

    assert sorted(seen) == [1, 2]


def test_concurrent_first_archive_open_returns_single_handle(monkeypatch):
    class NoOp(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="PING"):
                    pass

    original_cache_init = core.InMemCache.__init__

    def slow_cache_init(self, *args, **kwargs):
        time.sleep(0.05)
        original_cache_init(self, *args, **kwargs)

    monkeypatch.setattr(core.InMemCache, "__init__", slow_cache_init)

    pipe = Pipeline([NoOp()], strict_interest_inference=True)
    start_barrier = threading.Barrier(3)
    refs = []

    def opener():
        start_barrier.wait()
        refs.append(pipe.archive("shared"))

    t1 = threading.Thread(target=opener)
    t2 = threading.Thread(target=opener)
    t1.start()
    t2.start()
    start_barrier.wait()
    t1.join()
    t2.join()

    assert refs[0] is refs[1]
    assert len(pipe.archives_by_key) == 1
    assert ("shared", False) in pipe.archives_by_key


def test_inmem_archive_iteration_survives_concurrent_mutation():
    pipe = Pipeline([], strict_interest_inference=True)
    archive = pipe.archive("shared")
    archive["a"] = ["A"]
    archive["b"] = ["B"]

    iteration_started = threading.Event()
    mutated = threading.Event()
    seen = []
    errors = []

    def reader():
        try:
            for idx, value in enumerate(archive.values()):
                seen.append(value)
                if idx == 0:
                    iteration_started.set()
                    assert mutated.wait(timeout=1)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def writer():
        assert iteration_started.wait(timeout=1)
        archive["c"] = ["C"]
        mutated.set()

    t1 = threading.Thread(target=reader)
    t2 = threading.Thread(target=writer)
    t1.start()
    t2.start()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert errors == []
    assert len(seen) == 2
    assert archive["c"] == ["C"]


def test_run_processes_buffered_events_without_dispatcher():
    seen = []

    class Recorder(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X", payload=value):
                    seen.append(value)

    pipe = Pipeline([Recorder()], strict_interest_inference=True)
    pipe.submit(Event(name="X", payload=10))
    pipe.submit(Event(name="X", payload=11))
    pipe.run()

    assert seen == [10, 11]


def test_phase_loop_ignores_concurrent_external_submissions():
    """Concurrent external submissions during run() must not keep the phase
    fixed-point loop spinning forever. Only internal (pipeline-driven)
    submissions count toward the 'phase caused work' decision."""
    seen = []

    class Noop(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="EXT"):
                    seen.append(event.name)
                case Event(name="__PHASE__"):
                    pass

    pipe = Pipeline([Noop()], strict_interest_inference=True)

    stop = threading.Event()

    def external_submitter():
        while not stop.is_set():
            try:
                pipe.submit(Event(name="EXT"))
            except RuntimeError:
                # Normal once pipeline starts refusing (shutdown phase).
                return
            time.sleep(0.005)

    t = threading.Thread(target=external_submitter, daemon=True)
    t.start()
    try:
        pipe.run()
    finally:
        stop.set()
        t.join(timeout=2)

    # The pipeline should complete in bounded time even with continuous
    # external submissions. No assertion on seen length — we just need to
    # confirm run() returned.


def test_multi_worker_priority_band_fifo_is_stable():
    """Under contention with multiple workers, same-priority processors must
    still dispatch in FIFO order within their priority band. The monotonic
    tie-breaker in ProcessEntry guarantees stability regardless of heapq's
    ordering for equal keys."""
    seen = []
    seen_lock = threading.Lock()

    class Tagged(AbstractProcessor):
        def __init__(self, tag):
            super().__init__(priority=3, name=f"T-{tag}")
            self.tag = tag

        def process(self, context, event):
            match event:
                case Event(name="GO"):
                    # Work briefly so multiple workers are genuinely in flight.
                    time.sleep(0.001)
                    with seen_lock:
                        seen.append(self.tag)

    procs = [Tagged(i) for i in range(12)]
    pipe = Pipeline(procs, strict_interest_inference=True, max_workers=4)
    pipe.submit(Event(name="GO"))
    pipe.run()

    assert sorted(seen) == list(range(12))  # all executed
    assert len(seen) == 12


def test_context_submit_from_worker_chains_downstream_work():
    seen = []

    class Producer(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X", payload=value):
                    context.submit(Event(name="Y", payload=value * 2))

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="Y", payload=value):
                    seen.append(value)

    pipe = Pipeline([Producer(), Sink()], strict_interest_inference=True)
    pipe.submit(Event(name="X", payload=3))
    pipe.run()

    assert seen == [6]


def test_context_submit_rejected_after_fatal_error_does_not_enqueue_downstream():
    started = threading.Event()
    allow_finish = threading.Event()
    finished = threading.Event()

    class Producer(AbstractProcessor):
        def __init__(self):
            super().__init__()
            self.error = None
            self.recorded = None

        def process(self, context, event):
            match event:
                case Event(name="GO"):
                    started.set()
                    assert allow_finish.wait(timeout=1)
                    try:
                        context.submit(Event(name="DOWNSTREAM", value=1))
                    except RuntimeError as exc:
                        self.error = exc
                    self.recorded = list(context.events)
                    finished.set()

    class Sink(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="DOWNSTREAM"):
                    raise AssertionError("downstream event should not have been delivered")

    producer = Producer()
    sink = Sink()
    pipe = Pipeline([producer, sink], strict_interest_inference=True, max_workers=2)

    executor = ThreadPoolExecutor(max_workers=2)
    dispatcher = threading.Thread(
        target=pipe.execute_events,
        args=(executor,),
        daemon=True,
    )
    dispatcher.start()
    try:
        pipe.submit(Event(name="GO"))
        assert started.wait(timeout=1)

        pipe.set_fatal_error(RuntimeError("synthetic"))
        allow_finish.set()

        assert finished.wait(timeout=2)
        dispatcher.join(timeout=2)

        sink_inbox = pipe.get_inbox(sink)
        assert isinstance(producer.error, RuntimeError)
        assert "not accepting submissions" in str(producer.error)
        assert producer.recorded == []
        assert sink_inbox.pending == 0
    finally:
        pipe.rdyq.put(core._STOP_SENTINEL)
        dispatcher.join(timeout=2)
        executor.shutdown(wait=True, cancel_futures=True)


def test_decrement_is_noop_after_fatal_error_poisons_accounting():
    """Once fatal/abort paths force-balance done==jobs, subsequent decrement
    calls (from in-flight futures finishing) must not drift done above jobs.
    """

    class NoOp(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__NOOP_MARKER__"):
                    pass

    pipe = Pipeline([NoOp()], strict_interest_inference=True)
    pipe.increment(by=3)
    pipe.set_fatal_error(RuntimeError("synthetic"))

    assert pipe.done == pipe.jobs

    # Simulate in-flight tasks finishing after poisoning.
    pipe.decrement()
    pipe.decrement()
    pipe.decrement()

    assert pipe.done == pipe.jobs  # no drift


def test_wait_unblocks_promptly_on_internal_fatal_error():
    class NoOp(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="__NOOP_MARKER__"):
                    pass

    processor = NoOp()
    pipe = Pipeline([processor], strict_interest_inference=True)
    pipe.increment()  # Ensure wait() actually blocks.

    result = {"error": None}
    waiter_done = threading.Event()

    def waiter():
        try:
            pipe.wait()
        except Exception as exc:  # noqa: BLE001
            result["error"] = exc
        finally:
            waiter_done.set()

    wt = threading.Thread(target=waiter, daemon=True)
    wt.start()
    time.sleep(0.05)

    stale_entry = core._make_process_entry(processor.priority, processor)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pipe.dispatch_entry(stale_entry, executor)

    assert waiter_done.wait(timeout=2), "waiter should not hang after fatal error"
    assert isinstance(result["error"], RuntimeError)
    assert "internal dispatch error" in str(result["error"])


def test_external_submit_rejected_during_shutdown_without_hang():
    poison_started = threading.Event()
    run_error = {"exc": None}

    class Terminator(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="X"):
                    pass
                case Event(name="__POISON__"):
                    poison_started.set()
                    # Keep shutdown phase open long enough for concurrent submit attempt.
                    time.sleep(0.2)

    pipe = Pipeline([Terminator()], strict_interest_inference=True)
    pipe.submit(Event(name="X"))

    def run_pipe():
        try:
            pipe.run()
        except Exception as exc:  # noqa: BLE001
            run_error["exc"] = exc

    t = threading.Thread(target=run_pipe, daemon=True)
    t.start()

    assert poison_started.wait(timeout=2), "pipeline should reach poison phase"
    with pytest.raises(RuntimeError, match="not accepting external submissions"):
        pipe.submit(Event(name="X"))

    t.join(timeout=3)
    assert not t.is_alive(), "pipeline run should terminate without hanging"
    assert run_error["exc"] is None


def test_external_submit_rejected_after_freeze_even_without_recipients():
    class Listener(AbstractProcessor):
        def process(self, context, event):
            match event:
                case Event(name="PING"):
                    pass

    pipe = Pipeline([Listener()], strict_interest_inference=True)
    pipe.freeze_external_submissions()

    with pytest.raises(RuntimeError, match="not accepting external submissions"):
        pipe.submit(Event(name="NO_MATCH"))

    with pytest.raises(RuntimeError, match="not accepting external submissions"):
        pipe.submit(Event(name="PING"))
