"""Shared test helpers for dispatcher driving and optional dependency stubs."""

import sys
import threading
import types


def install_optional_dependency_stubs() -> None:
    """Provide minimal module stubs when optional dependencies are unavailable."""
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


def run_dispatcher_once(pipeline, pulses: int = 1) -> None:
    """Run dispatcher thread for one or more pulse/wait cycles."""
    q = pipeline.q

    def runner():
        pipeline.execute_events()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    try:
        for _ in range(pulses):
            q.put(True)
            pipeline.wait()
    finally:
        q.put(False)
        t.join(timeout=5)
