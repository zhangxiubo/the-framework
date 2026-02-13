import importlib
import sys
import threading
import time
import types
from pathlib import Path


def install_optional_dependency_stubs():
    try:
        import deepdiff  # noqa: F401
    except ModuleNotFoundError:
        deepdiff = types.ModuleType("deepdiff")

        class DeepHash(dict):
            def __init__(self, obj):
                super().__init__()
                self[obj] = hash(repr(obj))

        deepdiff.DeepHash = DeepHash
        sys.modules["deepdiff"] = deepdiff

    try:
        import dill  # noqa: F401
    except ModuleNotFoundError:
        dill = types.ModuleType("dill")
        dill.dumps = lambda x: x
        dill.loads = lambda x: x
        sys.modules["dill"] = dill

    try:
        import rocksdict  # noqa: F401
        import rocksdict.rocksdict  # noqa: F401
    except ModuleNotFoundError:
        rocksdict = types.ModuleType("rocksdict")

        class Rdict(dict):
            def set_dumps(self, *args, **kwargs):
                pass

            def set_loads(self, *args, **kwargs):
                pass

            def set_write_options(self, *args, **kwargs):
                pass

            def close(self):
                pass

        class WriteOptions:
            def __init__(self):
                self.sync = False

        rocksdict.Rdict = Rdict
        rocksdict.WriteOptions = WriteOptions
        sys.modules["rocksdict"] = rocksdict

        rocksdict_sub = types.ModuleType("rocksdict.rocksdict")

        class AccessType:
            @staticmethod
            def read_write():
                return "rw"

            @staticmethod
            def read_only():
                return "ro"

        rocksdict_sub.AccessType = AccessType
        sys.modules["rocksdict.rocksdict"] = rocksdict_sub


install_optional_dependency_stubs()
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
core = importlib.import_module("framework.core")

AbstractProcessor = core.AbstractProcessor
Event = core.Event
Pipeline = core.Pipeline


def run_dispatcher_once(pipeline: Pipeline):
    q = pipeline.q

    def runner():
        pipeline.execute_events(q)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    try:
        q.put(True)
        pipeline.wait()
    finally:
        q.put(False)
        t.join(timeout=5)


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


def test_context_submit_from_worker_keeps_progress_without_pulse_loop():
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
