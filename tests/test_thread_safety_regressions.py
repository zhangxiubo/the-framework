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


def test_wait_unblocks_promptly_on_internal_fatal_error():
    class NoOp(AbstractProcessor):
        def process(self, context, event):
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

    stale_entry = core.ProcessEntry(priority=processor.priority, processor=processor)
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
