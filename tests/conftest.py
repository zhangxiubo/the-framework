"""Shared pytest fixtures for framework tests."""

import threading

import pytest


@pytest.fixture
def run_pipeline():
    """Factory fixture that returns a helper to run pipelines for testing."""
    from framework.core import Pipeline

    def run_dispatcher_once_impl(pipeline: Pipeline, pulses: int = 1):
        """Run the pipeline dispatcher for a given number of pulses."""
        q = pipeline.q

        def runner():
            pipeline.execute_events(q)

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        try:
            for _ in range(pulses):
                q.put(True)
                pipeline.wait()
        finally:
            q.put(False)
            t.join(timeout=5)

    return run_dispatcher_once_impl


def run_dispatcher_once(pipeline, pulses: int = 1):
    """Standalone helper (for backward compatibility)."""
    q = pipeline.q

    def runner():
        pipeline.execute_events(q)

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    try:
        for _ in range(pulses):
            q.put(True)
            pipeline.wait()
    finally:
        q.put(False)
        t.join(timeout=5)
