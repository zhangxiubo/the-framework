"""Unit tests for parallelize() and its supporting routing primitives."""

import random
import time

import pytest
from framework.core import AbstractProcessor, Event, Pipeline
from framework.reactive import (
    LoadBalancerCollector,
    LoadBalancerRouter,
    LoadBalancerSequencer,
    ReactiveBuilder,
    ReactiveEvent,
    SkipAheadOnBacklog,
    parallelize,
)
from tests.helpers import run_dispatcher_once


class SimpleDoubler(ReactiveBuilder):
    """Test worker that doubles its input."""

    def __init__(self, **kwargs):
        super().__init__(provides="unused", requires=[], persist=False, **kwargs)

    def build(self, context, item):
        yield item * 2


class MultiOutputWorker(ReactiveBuilder):
    """Test worker that produces multiple outputs per input."""

    def __init__(self, **kwargs):
        super().__init__(provides="unused", requires=[], persist=False, **kwargs)

    def build(self, context, item):
        yield item
        yield item**2


class StatefulCounter(ReactiveBuilder):
    """Test worker that maintains state (counts calls)."""

    def __init__(self, **kwargs):
        super().__init__(provides="unused", requires=[], persist=False, **kwargs)
        self.call_count = 0

    def build(self, context, item):
        self.call_count += 1
        yield (item, self.call_count)


class TestParallelizeBasic:
    """Basic structure and configuration tests for parallelize()."""

    def test_parallelize_returns_correct_components(self):
        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=3,
        )

        assert len(components) == 8
        assert isinstance(components[0], LoadBalancerSequencer)
        assert isinstance(components[-1], LoadBalancerCollector)

        for i in range(3):
            assert isinstance(components[1 + i * 2], LoadBalancerRouter)
            assert isinstance(components[2 + i * 2], SimpleDoubler)

    def test_parallelize_single_worker(self):
        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=1,
        )

        assert len(components) == 4

    def test_parallelize_validates_num_workers(self):
        with pytest.raises(ValueError, match="num_workers must be >= 1"):
            parallelize(
                provides="output",
                requires=["input"],
                worker_builder=SimpleDoubler,
                num_workers=0,
            )

    def test_parallelize_workers_are_independent_instances(self):
        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=StatefulCounter,
            num_workers=3,
        )

        workers = [c for c in components if isinstance(c, StatefulCounter)]
        assert len(workers) == 3
        assert workers[0] is not workers[1]
        assert workers[1] is not workers[2]

    def test_parallelize_workers_have_distinct_names(self):
        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=StatefulCounter,
            num_workers=3,
        )

        names = [worker.name for worker in components if isinstance(worker, StatefulCounter)]
        assert len(names) == len(set(names))
        assert "_worker_0" in names[0]
        assert "_worker_1" in names[1]
        assert "_worker_2" in names[2]

    def test_parallelize_worker_names_are_deterministic(self):
        components1 = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=StatefulCounter,
            num_workers=2,
        )
        components2 = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=StatefulCounter,
            num_workers=2,
        )

        names1 = [worker.name for worker in components1 if isinstance(worker, StatefulCounter)]
        names2 = [worker.name for worker in components2 if isinstance(worker, StatefulCounter)]
        assert names1 == names2
        assert "output" in names1[0]
        assert "StatefulCounter" in names1[0]

    def test_parallelize_different_provides_different_names(self):
        components1 = parallelize(
            provides="output_a",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=2,
        )
        components2 = parallelize(
            provides="output_b",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=2,
        )

        names1 = {worker.name for worker in components1 if isinstance(worker, SimpleDoubler)}
        names2 = {worker.name for worker in components2 if isinstance(worker, SimpleDoubler)}
        assert names1.isdisjoint(names2)

    def test_parallelize_passes_init_kwargs(self):
        class ConfigurableWorker(ReactiveBuilder):
            def __init__(self, multiplier=1, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )
                self.multiplier = multiplier

            def build(self, context, item):
                yield item * self.multiplier

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=ConfigurableWorker,
            num_workers=2,
            multiplier=10,
        )

        workers = [worker for worker in components if isinstance(worker, ConfigurableWorker)]
        assert workers[0].multiplier == 10
        assert workers[1].multiplier == 10


class TestLoadBalancerSequencerBehavior:
    """Tests for LoadBalancerSequencer."""

    def test_sequencer_assigns_sequential_numbers(self):
        from framework.core import Context

        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input"],
        )

        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)

        results = []
        for i in range(5):
            results.extend(list(sequencer.build(ctx, f"item_{i}")))

        seqs = [result[0] for result in results]
        assert seqs == [0, 1, 2, 3, 4]

    def test_sequencer_preserves_args(self):
        from framework.core import Context

        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input"],
        )

        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)

        results = list(sequencer.build(ctx, "value1", "value2"))
        seq, args = results[0]
        assert args == ("value1", "value2")


class TestLoadBalancerRouterBehavior:
    """Tests for LoadBalancerRouter."""

    def test_router_filters_by_worker_id(self):
        from framework.core import Context

        router = LoadBalancerRouter(
            provides="routed",
            requires=["sequenced"],
            worker_id=1,
            num_workers=3,
        )

        pipeline = Pipeline([router])
        ctx = Context(pipeline)

        results = []
        for seq in range(10):
            results.extend(list(router.build(ctx, (seq, ("item",)))))

        forwarded_seqs = [result[0] for result in results]
        assert forwarded_seqs == [1, 4, 7]

    def test_router_round_robin_distribution(self):
        from framework.core import Context

        routers = [
            LoadBalancerRouter(
                provides=f"routed_{i}",
                requires=["sequenced"],
                worker_id=i,
                num_workers=3,
            )
            for i in range(3)
        ]

        pipeline = Pipeline(routers)
        ctx = Context(pipeline)

        counts = [0, 0, 0]
        for seq in range(9):
            item = (seq, ("data",))
            for i, router in enumerate(routers):
                counts[i] += len(list(router.build(ctx, item)))

        assert counts == [3, 3, 3]


class TestLoadBalancerCollectorBehavior:
    """Tests for LoadBalancerCollector."""

    def test_collector_emits_in_sequence_order(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        results = []
        results.extend(list(collector.build(ctx, (2, ["r2"]))))
        results.extend(list(collector.build(ctx, (0, ["r0"]))))
        results.extend(list(collector.build(ctx, (1, ["r1"]))))

        assert results == ["r0", "r1", "r2"]

    def test_collector_buffers_until_ready(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        r1 = list(collector.build(ctx, (5, ["r5"])))
        r2 = list(collector.build(ctx, (3, ["r3"])))
        r3 = list(collector.build(ctx, (4, ["r4"])))

        assert r1 == []
        assert r2 == []
        assert r3 == []
        assert collector.get_pending_count() == 3
        assert collector.get_next_expected_seq() == 0

    def test_collector_flushes_consecutive_sequences(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        list(collector.build(ctx, (1, ["r1"])))
        list(collector.build(ctx, (2, ["r2"])))
        list(collector.build(ctx, (3, ["r3"])))

        results = list(collector.build(ctx, (0, ["r0"])))

        assert results == ["r0", "r1", "r2", "r3"]
        assert collector.get_pending_count() == 0
        assert collector.get_next_expected_seq() == 4

    def test_collector_handles_multiple_results_per_sequence(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        results = list(collector.build(ctx, (0, ["a", "b", "c"])))
        assert results == ["a", "b", "c"]

    def test_collector_gap_policy_can_skip_missing_sequences(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
            gap_policy=SkipAheadOnBacklog(max_buffered=3),
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        assert list(collector.build(ctx, (5, ["r5"]))) == []
        assert list(collector.build(ctx, (6, ["r6"]))) == []
        assert list(collector.build(ctx, (7, ["r7"]))) == ["r5", "r6", "r7"]
        assert collector.get_next_expected_seq() == 8
        assert collector.skipped_sequences == 5


class TestParallelizeIntegration:
    """Integration tests for the full parallelized graph."""

    def test_fifo_order_preserved_with_two_workers(self):
        results = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)

            def build(self, context, *args):
                for i in range(10):
                    yield i

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=2,
        )

        pipe = Pipeline([Source(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=3)

        assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    def test_fifo_order_with_multiple_outputs_per_item(self):
        results = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)

            def build(self, context, *args):
                yield 2
                yield 3
                yield 4

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=MultiOutputWorker,
            num_workers=3,
        )

        pipe = Pipeline([Source(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=3)

        assert results == [2, 4, 3, 9, 4, 16]

    def test_parallelize_with_empty_worker_output(self):
        results = []

        class FilterWorker(ReactiveBuilder):
            def __init__(self, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )

            def build(self, context, item):
                if item % 2 == 0:
                    yield item

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)

            def build(self, context, *args):
                for i in range(6):
                    yield i

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=FilterWorker,
            num_workers=2,
        )

        pipe = Pipeline([Source(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=3)

        assert results == [0, 2, 4]

    def test_multiple_requires_topics_joins_inputs(self):
        results = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class SourceA(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input_a", requires=[], persist=False)

            def build(self, context, *args):
                yield "a1"
                yield "a2"

        class SourceB(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input_b", requires=[], persist=False)

            def build(self, context, *args):
                yield "b1"
                yield "b2"

        class JoiningWorker(ReactiveBuilder):
            def __init__(self, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )

            def build(self, context, item_a, item_b):
                yield f"{item_a}+{item_b}"

        components = parallelize(
            provides="output",
            requires=["input_a", "input_b"],
            worker_builder=JoiningWorker,
            num_workers=2,
        )

        pipe = Pipeline([SourceA(), SourceB(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input_a"))
        pipe.submit(ReactiveEvent(name="resolve", target="input_b"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=4)

        assert results == ["a1+b1", "a2+b2"]


class TestParallelizeEdgeCases:
    """Edge case tests for parallelize() and shared internals."""

    def test_worker_with_init_args(self):
        class WorkerWithArgs(ReactiveBuilder):
            def __init__(self, prefix, suffix, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )
                self.prefix = prefix
                self.suffix = suffix

            def build(self, context, item):
                yield f"{self.prefix}{item}{self.suffix}"

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=WorkerWithArgs,
            num_workers=2,
            worker_init_args=("<<", ">>"),
        )

        workers = [worker for worker in components if isinstance(worker, WorkerWithArgs)]
        assert workers[0].prefix == "<<"
        assert workers[0].suffix == ">>"

    def test_large_sequence_gap(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        r1 = list(collector.build(ctx, (0, ["r0"])))
        r2 = list(collector.build(ctx, (1000, ["r1000"])))

        assert r1 == ["r0"]
        assert r2 == []
        assert collector.get_next_expected_seq() == 1
        assert 1000 in collector.buffer

    def test_sequencer_state_persists_across_calls(self):
        from framework.core import Context

        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input"],
        )

        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)

        list(sequencer.build(ctx, "a"))
        list(sequencer.build(ctx, "b"))

        r3 = list(sequencer.build(ctx, "c"))
        r4 = list(sequencer.build(ctx, "d"))

        assert r3[0][0] == 2
        assert r4[0][0] == 3

    def test_sequencer_with_multiple_requires_preserves_all_args(self):
        from framework.core import Context

        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input_a", "input_b"],
        )

        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)

        results = list(sequencer.build(ctx, "val_a", "val_b"))
        seq, args = results[0]
        assert seq == 0
        assert args == ("val_a", "val_b")


class TestParallelizeWorkerIsolation:
    """Tests for worker instance isolation."""

    def test_worker_state_is_isolated(self):
        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=StatefulCounter,
            num_workers=2,
        )

        workers = [worker for worker in components if isinstance(worker, StatefulCounter)]
        w1, w2 = workers

        w1.build(None, "a")
        list(w1.build(None, "a"))
        list(w1.build(None, "b"))
        list(w2.build(None, "c"))

        assert w1.call_count == 2
        assert w2.call_count == 1


class TestParallelizeExceptionHandling:
    """Tests for worker exception behavior."""

    def test_worker_exception_propagates(self):
        class FailingWorker(ReactiveBuilder):
            def __init__(self, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )

            def build(self, context, item):
                if item == "fail":
                    raise ValueError("Intentional failure")
                yield item

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=FailingWorker,
            num_workers=2,
        )

        worker = [item for item in components if isinstance(item, FailingWorker)][0]
        assert list(worker.build(None, "ok")) == ["ok"]

        with pytest.raises(ValueError, match="Intentional failure"):
            list(worker.build(None, "fail"))

    def test_worker_exception_does_not_affect_other_workers(self):
        class SelectiveFailer(ReactiveBuilder):
            def __init__(self, fail_on=None, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )
                self.fail_on = fail_on

            def build(self, context, item):
                if self.fail_on is not None and item == self.fail_on:
                    raise RuntimeError(f"Failed on {item}")
                yield item * 2

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SelectiveFailer,
            num_workers=2,
        )

        workers = [item for item in components if isinstance(item, SelectiveFailer)]
        workers[0].fail_on = 5

        assert list(workers[1].build(None, 5)) == [10]
        with pytest.raises(RuntimeError, match="Failed on 5"):
            list(workers[0].build(None, 5))


class TestParallelizeStress:
    """Stress tests for parallelize() and shared primitives."""

    def test_many_items_single_worker(self):
        results = []
        num_items = 1000

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)

            def build(self, context, *args):
                for i in range(num_items):
                    yield i

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=1,
        )

        pipe = Pipeline([Source(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=5)

        expected = [i * 2 for i in range(num_items)]
        assert results == expected

    def test_many_items_many_workers(self):
        results = []
        num_items = 500
        num_workers = 10

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        results.append(value)

        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)

            def build(self, context, *args):
                for i in range(num_items):
                    yield i

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SimpleDoubler,
            num_workers=num_workers,
        )

        pipe = Pipeline([Source(), *components, Sink()])
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=10)

        expected = [i * 2 for i in range(num_items)]
        assert results == expected

    def test_many_workers_component_count(self):
        for num_workers in [1, 5, 10, 50]:
            components = parallelize(
                provides="output",
                requires=["input"],
                worker_builder=SimpleDoubler,
                num_workers=num_workers,
            )

            assert len(components) == 1 + num_workers * 2 + 1

    def test_stress_collector_reordering(self):
        from framework.core import Context

        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )

        pipeline = Pipeline([collector])
        ctx = Context(pipeline)

        num_items = 1000
        seqs = list(range(1, num_items))
        random.shuffle(seqs)

        results = []
        for seq in seqs:
            results.extend(list(collector.build(ctx, (seq, [f"r{seq}"]))))

        assert results == []
        assert collector.get_pending_count() == num_items - 1

        results.extend(list(collector.build(ctx, (0, ["r0"]))))

        expected = [f"r{i}" for i in range(num_items)]
        assert results == expected
        assert collector.get_pending_count() == 0

    def test_high_worker_count_distribution(self):
        from framework.core import Context

        num_workers = 20
        num_items = 1000

        routers = [
            LoadBalancerRouter(
                provides=f"routed_{i}",
                requires=["sequenced"],
                worker_id=i,
                num_workers=num_workers,
            )
            for i in range(num_workers)
        ]

        pipeline = Pipeline(routers)
        ctx = Context(pipeline)

        counts = [0] * num_workers
        for seq in range(num_items):
            item = (seq, ("data",))
            for i, router in enumerate(routers):
                counts[i] += len(list(router.build(ctx, item)))

        expected_per_worker = num_items // num_workers
        for i, count in enumerate(counts):
            assert count == expected_per_worker


class TestParallelizeConstructorSupport:
    """Constructor compatibility tests for worker builders."""

    def test_parallelize_accepts_worker_using_inherited_reactivebuilder_init(self):
        class InheritedInitWorker(ReactiveBuilder):
            def build(self, context, item):
                yield item.upper()

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=InheritedInitWorker,
            num_workers=2,
            persist=True,
        )

        workers = [worker for worker in components if isinstance(worker, InheritedInitWorker)]
        assert len(workers) == 2
        assert all(worker.persist is True for worker in workers)

    def test_parallelize_accepts_positional_args(self):
        class WorkerWithRequiredArgs(ReactiveBuilder):
            def __init__(self, prefix, suffix, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )
                self.prefix = prefix
                self.suffix = suffix

            def build(self, context, item):
                yield f"{self.prefix}{item}{self.suffix}"

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=WorkerWithRequiredArgs,
            num_workers=2,
            worker_init_args=("<<", ">>"),
        )

        workers = [worker for worker in components if isinstance(worker, WorkerWithRequiredArgs)]
        assert workers[0].prefix == "<<"
        assert workers[0].suffix == ">>"

    def test_parallelize_mixed_positional_and_keyword_args(self):
        class MixedArgsWorker(ReactiveBuilder):
            def __init__(self, required_arg, optional=10, **kwargs):
                super().__init__(
                    provides="unused", requires=[], persist=False, **kwargs
                )
                self.required = required_arg
                self.optional = optional

            def build(self, context, item):
                yield item * self.optional

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=MixedArgsWorker,
            num_workers=2,
            worker_init_args=("my_value",),
            optional=20,
        )

        workers = [worker for worker in components if isinstance(worker, MixedArgsWorker)]
        assert workers[0].required == "my_value"
        assert workers[0].optional == 20


class TestParallelizeBehavior:
    """Behavioral tests specific to the public parallelize() API."""

    def test_parallelize_preserves_fifo_order(self):
        seen = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        seen.append(value)

        class SlowWorker(ReactiveBuilder):
            def build(self, context, item):
                if item == 0:
                    time.sleep(0.05)
                yield item

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SlowWorker,
            num_workers=2,
        )

        pipe = Pipeline([*components, Sink()])
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=0))
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=1))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=4)

        assert seen == [0, 1]

    def test_parallelize_can_disable_fifo_reordering(self):
        seen = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        seen.append(value)

        class SlowWorker(ReactiveBuilder):
            def build(self, context, item):
                if item == 0:
                    time.sleep(0.05)
                yield item

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=SlowWorker,
            num_workers=2,
            preserve_fifo=False,
        )

        pipe = Pipeline([*components, Sink()])
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=0))
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=1))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=4)

        assert seen == [1, 0]

    def test_parallelize_deduplicates_like_a_single_reactive_builder(self):
        seen = []

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=value):
                        seen.append(value)

        class Worker(ReactiveBuilder):
            def build(self, context, item):
                yield item * 10

        components = parallelize(
            provides="output",
            requires=["input"],
            worker_builder=Worker,
            num_workers=2,
        )

        pipe = Pipeline([*components, Sink()])
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=2))
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=2))
        pipe.submit(ReactiveEvent(name="built", target="input", artifact=3))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        run_dispatcher_once(pipe, pulses=4)

        assert seen == [20, 30]
