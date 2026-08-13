"""Tests for the incomplete-build contract.

A build that raises ``IncompleteBuild`` declines the archive write for the
current input: rows already yielded still flowed downstream this run, but
the input's key is absent from the archive, so a later run re-attempts it.
Successful inputs archive exactly as before and are never re-built.
"""

from framework.core import AbstractProcessor, Pipeline
from framework.reactive import (
    IncompleteBuild,
    ReactiveBuilder,
    ReactiveEvent,
    parallelize,
)
from tests.helpers import run_dispatcher_once


class Sink(AbstractProcessor):
    """Collect artifacts built for one target."""

    def __init__(self, target: str, results: list):
        super().__init__()
        self.target = target
        self.results = results

    def process(self, context, event):
        match event:
            case ReactiveEvent(name="built", target=target, artifact=value) if (
                target == self.target
            ):
                self.results.append(value)


class FlakyWorker(ReactiveBuilder):
    """Fails inputs listed in ``failing`` by raising IncompleteBuild.

    ``failing`` is shared and mutable so a test can heal the failure
    between runs; ``build_log`` records every attempted input.
    """

    def __init__(self, *args, failing: set, build_log: list, **kwargs):
        super().__init__(*args, **kwargs)
        self.failing = failing
        self.build_log = build_log

    def build(self, context, item):
        self.build_log.append(item)
        if item in self.failing:
            raise IncompleteBuild(f"item {item!r} could not be completed")
        yield item * 10


class PartialEmitter(ReactiveBuilder):
    """Yields one row, then declares the build incomplete."""

    def __init__(self, *args, build_log: list, **kwargs):
        super().__init__(*args, **kwargs)
        self.build_log = build_log

    def build(self, context, item):
        self.build_log.append(item)
        yield ("partial", item)
        raise IncompleteBuild("stopped after one row")


def run_worker_pipeline(tmp_path, worker, inputs, results):
    pipe = Pipeline([worker, Sink("output", results)], workspace=tmp_path)
    for item in inputs:
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=item))
    pipe.submit(ReactiveEvent(name="resolve", target="output"))
    run_dispatcher_once(pipe, pulses=3)
    pipe.close_archives()


class TestIncompleteBuildDefaultPublish:
    def test_incomplete_input_is_not_archived_and_retries_on_resume(self, tmp_path):
        failing, build_log = {2}, []
        results: list = []
        run_worker_pipeline(
            tmp_path,
            FlakyWorker("output", ["input"], persist=True, failing=failing, build_log=build_log),
            [1, 2],
            results,
        )
        assert results == [10]
        assert build_log == [1, 2]

        failing.clear()
        healed_results: list = []
        run_worker_pipeline(
            tmp_path,
            FlakyWorker("output", ["input"], persist=True, failing=failing, build_log=build_log),
            [1, 2],
            healed_results,
        )
        # The good input replays without rebuilding; only the healed one runs.
        assert build_log == [1, 2, 2]
        assert sorted(healed_results) == [10, 20]

    def test_successful_inputs_archive_exactly_as_before(self, tmp_path):
        build_log: list = []
        results: list = []
        run_worker_pipeline(
            tmp_path,
            FlakyWorker("output", ["input"], persist=True, failing=set(), build_log=build_log),
            [1],
            results,
        )
        replay_results: list = []
        run_worker_pipeline(
            tmp_path,
            FlakyWorker("output", ["input"], persist=True, failing=set(), build_log=build_log),
            [1],
            replay_results,
        )
        assert build_log == [1]
        assert results == replay_results == [10]

    def test_rows_yielded_before_the_raise_still_flow_this_run(self, tmp_path):
        build_log: list = []
        results: list = []
        run_worker_pipeline(
            tmp_path,
            PartialEmitter("output", ["input"], persist=True, build_log=build_log),
            [7],
            results,
        )
        assert results == [("partial", 7)]

        # The input was not archived, so a later run attempts it again.
        rerun_results: list = []
        run_worker_pipeline(
            tmp_path,
            PartialEmitter("output", ["input"], persist=True, build_log=build_log),
            [7],
            rerun_results,
        )
        assert build_log == [7, 7]

    def test_pipeline_continues_past_an_incomplete_input(self, tmp_path):
        build_log: list = []
        results: list = []
        run_worker_pipeline(
            tmp_path,
            FlakyWorker("output", ["input"], persist=True, failing={1}, build_log=build_log),
            [1, 2, 3],
            results,
        )
        assert sorted(results) == [20, 30]


def run_parallel_pipeline(tmp_path, inputs, results, *, failing, build_log, num_workers=2):
    components = parallelize(
        provides="output",
        requires=["input"],
        worker_builder=FlakyWorker,
        num_workers=num_workers,
        persist=True,
        failing=failing,
        build_log=build_log,
    )
    pipe = Pipeline([*components, Sink("output", results)], workspace=tmp_path)
    for item in inputs:
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=item))
    pipe.submit(ReactiveEvent(name="resolve", target="output"))
    run_dispatcher_once(pipe, pulses=4)
    pipe.close_archives()


class TestIncompleteBuildParallelize:
    def test_incomplete_item_retries_on_resume_while_good_items_replay(self, tmp_path):
        failing, build_log = {2}, []
        results: list = []
        run_parallel_pipeline(
            tmp_path, [1, 2, 3], results, failing=failing, build_log=build_log
        )
        assert sorted(results) == [10, 30]

        failing.clear()
        healed_results: list = []
        run_parallel_pipeline(
            tmp_path, [1, 2, 3], healed_results, failing=failing, build_log=build_log
        )
        # Only the healed item is rebuilt; the archive replays the rest.
        # Workers run concurrently, so within-run build order is free.
        assert sorted(build_log[:3]) == [1, 2, 3]
        assert build_log[3:] == [2]
        assert sorted(healed_results) == [10, 20, 30]


class TestSequenceCursorOnResume:
    def test_new_input_on_a_warm_archive_gets_a_fresh_sequence(self, tmp_path):
        """Regression: the cursor must resume past archived sequences, or a
        new input reuses seq 0 and collides with the archived worker/emitter
        entries, silently producing nothing."""
        build_log: list = []
        results: list = []
        run_parallel_pipeline(
            tmp_path, [1, 2], results, failing=set(), build_log=build_log
        )
        assert sorted(results) == [10, 20]

        extended_results: list = []
        run_parallel_pipeline(
            tmp_path, [1, 2, 3], extended_results, failing=set(), build_log=build_log
        )
        assert build_log[2:] == [3]
        assert sorted(extended_results) == [10, 20, 30]
