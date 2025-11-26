"""Unit tests for make_load_balancer and related classes."""

import threading
import time
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parents[1] / "src"))

import pytest
from framework.core import AbstractProcessor, Event, Pipeline
from framework.reactive import (
    ReactiveBuilder,
    ReactiveEvent,
    LoadBalancerSequencer,
    LoadBalancerRouter,
    LoadBalancerWorkerWrapper,
    LoadBalancerCollector,
    make_load_balancer,
)


def run_dispatcher_once(pipeline: Pipeline, pulses: int = 1):
    """Helper to run the pipeline dispatcher for a given number of pulses."""
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
        # Yield the item and its square
        yield item
        yield item ** 2


class StatefulCounter(ReactiveBuilder):
    """Test worker that maintains state (counts calls)."""
    
    def __init__(self, **kwargs):
        super().__init__(provides="unused", requires=[], persist=False, **kwargs)
        self.call_count = 0
    
    def build(self, context, item):
        self.call_count += 1
        yield (item, self.call_count)


class TestMakeLoadBalancerBasic:
    """Basic functionality tests for make_load_balancer."""

    def test_make_load_balancer_returns_correct_components(self):
        """make_load_balancer should return sequencer + N*(router+worker) + collector."""
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=3,
        )
        
        # sequencer + 3*(router+worker) + collector = 1 + 6 + 1 = 8 components
        assert len(components) == 8
        
        # Check types
        assert isinstance(components[0], LoadBalancerSequencer)
        assert isinstance(components[-1], LoadBalancerCollector)
        
        # Check routers and wrappers alternate
        for i in range(3):
            assert isinstance(components[1 + i*2], LoadBalancerRouter)
            assert isinstance(components[2 + i*2], LoadBalancerWorkerWrapper)

    def test_make_load_balancer_single_worker(self):
        """Load balancer with 1 worker should still work correctly."""
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=1,
        )
        
        # sequencer + 1*(router+worker) + collector = 4 components
        assert len(components) == 4

    def test_make_load_balancer_validates_num_workers(self):
        """make_load_balancer should reject num_workers < 1."""
        with pytest.raises(ValueError, match="num_workers must be >= 1"):
            make_load_balancer(
                provides="output",
                requires=["input"],
                worker_class=SimpleDoubler,
                num_workers=0,
            )

    def test_make_load_balancer_workers_are_independent_instances(self):
        """Each worker wrapper should contain a separate worker instance."""
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=StatefulCounter,
            num_workers=3,
        )
        
        # Get the worker wrappers (every other component after sequencer, before collector)
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        
        assert len(wrappers) == 3
        assert wrappers[0].worker is not wrappers[1].worker
        assert wrappers[1].worker is not wrappers[2].worker

    def test_make_load_balancer_workers_have_distinct_names(self):
        """Each worker instance should have a unique name to avoid archive collisions."""
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=StatefulCounter,
            num_workers=3,
        )
        
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        
        # Get the names of all workers
        names = [w.worker.name for w in wrappers]
        
        # All names should be unique
        assert len(names) == len(set(names)), f"Worker names are not unique: {names}"
        
        # Each name should contain the worker index
        assert "_worker_0" in names[0]
        assert "_worker_1" in names[1]
        assert "_worker_2" in names[2]

    def test_make_load_balancer_worker_names_are_deterministic(self):
        """Worker names should be deterministic across multiple calls."""
        components1 = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=StatefulCounter,
            num_workers=2,
        )
        components2 = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=StatefulCounter,
            num_workers=2,
        )
        
        wrappers1 = [c for c in components1 if isinstance(c, LoadBalancerWorkerWrapper)]
        wrappers2 = [c for c in components2 if isinstance(c, LoadBalancerWorkerWrapper)]
        
        # Names should be identical across calls
        names1 = [w.worker.name for w in wrappers1]
        names2 = [w.worker.name for w in wrappers2]
        assert names1 == names2
        
        # Names should include the provides topic and worker class name
        assert "output" in names1[0]
        assert "StatefulCounter" in names1[0]

    def test_make_load_balancer_different_provides_different_names(self):
        """Different load balancers should have different worker names."""
        components1 = make_load_balancer(
            provides="output_a",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=2,
        )
        components2 = make_load_balancer(
            provides="output_b",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=2,
        )
        
        wrappers1 = [c for c in components1 if isinstance(c, LoadBalancerWorkerWrapper)]
        wrappers2 = [c for c in components2 if isinstance(c, LoadBalancerWorkerWrapper)]
        
        names1 = set(w.worker.name for w in wrappers1)
        names2 = set(w.worker.name for w in wrappers2)
        
        # Names should not overlap between different load balancers
        assert names1.isdisjoint(names2)

    def test_make_load_balancer_passes_init_kwargs(self):
        """Worker init kwargs should be passed to each worker instance."""
        
        class ConfigurableWorker(ReactiveBuilder):
            def __init__(self, multiplier=1, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
                self.multiplier = multiplier
            
            def build(self, context, item):
                yield item * self.multiplier
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=ConfigurableWorker,
            num_workers=2,
            worker_init_kwargs={"multiplier": 10},
        )
        
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        assert wrappers[0].worker.multiplier == 10
        assert wrappers[1].worker.multiplier == 10


class TestLoadBalancerSequencerBehavior:
    """Tests for LoadBalancerSequencer."""

    def test_sequencer_assigns_sequential_numbers(self):
        """Each item should get a unique, sequential sequence number."""
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
        
        # Check sequence numbers are 0, 1, 2, 3, 4
        seqs = [r[0] for r in results]
        assert seqs == [0, 1, 2, 3, 4]

    def test_sequencer_preserves_args(self):
        """Sequencer should preserve the original args."""
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
        """Router should only forward items where seq % num_workers == worker_id."""
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
        
        # Worker 1 should get seq 1, 4, 7
        forwarded_seqs = [r[0] for r in results]
        assert forwarded_seqs == [1, 4, 7]

    def test_router_round_robin_distribution(self):
        """Items should be distributed evenly across workers."""
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
        
        # Count how many items each router forwards
        counts = [0, 0, 0]
        for seq in range(9):
            item = (seq, ("data",))
            for i, router in enumerate(routers):
                counts[i] += len(list(router.build(ctx, item)))
        
        # Should be evenly distributed
        assert counts == [3, 3, 3]


class TestLoadBalancerWorkerWrapperBehavior:
    """Tests for LoadBalancerWorkerWrapper."""

    def test_wrapper_passes_through_sequence_number(self):
        """Wrapper should preserve sequence number in output."""
        from framework.core import Context
        
        worker_instance = SimpleDoubler()
        wrapper = LoadBalancerWorkerWrapper(
            provides="results",
            requires=["routed"],
            worker=worker_instance,
        )
        
        pipeline = Pipeline([wrapper])
        ctx = Context(pipeline)
        
        # Input: (seq=42, args=(5,))
        results = list(wrapper.build(ctx, (42, (5,))))
        
        assert len(results) == 1
        seq, result_list = results[0]
        assert seq == 42
        assert result_list == [10]  # 5 * 2

    def test_wrapper_collects_multiple_outputs(self):
        """Wrapper should collect all outputs from wrapped worker's build."""
        from framework.core import Context
        
        worker_instance = MultiOutputWorker()
        wrapper = LoadBalancerWorkerWrapper(
            provides="results",
            requires=["routed"],
            worker=worker_instance,
        )
        
        pipeline = Pipeline([wrapper])
        ctx = Context(pipeline)
        
        results = list(wrapper.build(ctx, (7, (3,))))
        
        assert len(results) == 1
        seq, result_list = results[0]
        assert seq == 7
        assert result_list == [3, 9]  # item and item^2


class TestLoadBalancerCollectorBehavior:
    """Tests for LoadBalancerCollector."""

    def test_collector_emits_in_sequence_order(self):
        """Collector should emit results in sequence order regardless of arrival order."""
        from framework.core import Context
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        # Send results out of order: seq 2, seq 0, seq 1
        results = []
        results.extend(list(collector.build(ctx, (2, ["r2"]))))  # Buffered
        results.extend(list(collector.build(ctx, (0, ["r0"]))))  # Emits r0
        results.extend(list(collector.build(ctx, (1, ["r1"]))))  # Emits r1, r2
        
        assert results == ["r0", "r1", "r2"]

    def test_collector_buffers_until_ready(self):
        """Collector should buffer results until the expected sequence arrives."""
        from framework.core import Context
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        # Send seq 5, 3, 4 (none should emit yet because we're waiting for seq 0)
        r1 = list(collector.build(ctx, (5, ["r5"])))
        r2 = list(collector.build(ctx, (3, ["r3"])))
        r3 = list(collector.build(ctx, (4, ["r4"])))
        
        assert r1 == []
        assert r2 == []
        assert r3 == []
        assert collector.get_pending_count() == 3
        assert collector.get_next_expected_seq() == 0

    def test_collector_flushes_consecutive_sequences(self):
        """When the expected seq arrives, collector should flush all consecutive ready seqs."""
        from framework.core import Context
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        # Buffer seqs 1, 2, 3
        list(collector.build(ctx, (1, ["r1"])))
        list(collector.build(ctx, (2, ["r2"])))
        list(collector.build(ctx, (3, ["r3"])))
        
        # Now send seq 0 - should flush 0, 1, 2, 3
        results = list(collector.build(ctx, (0, ["r0"])))
        
        assert results == ["r0", "r1", "r2", "r3"]
        assert collector.get_pending_count() == 0
        assert collector.get_next_expected_seq() == 4

    def test_collector_handles_multiple_results_per_sequence(self):
        """Collector should emit all results for a sequence before moving on."""
        from framework.core import Context
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        # Seq 0 has multiple results
        results = list(collector.build(ctx, (0, ["a", "b", "c"])))
        
        assert results == ["a", "b", "c"]


class TestLoadBalancerFIFOOrdering:
    """Tests verifying deterministic FIFO ordering."""

    def test_fifo_order_preserved_with_two_workers(self):
        """Items should be emitted in input order regardless of which worker processes them."""
        results = []
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)
            
            def build(self, context, *args):
                for i in range(10):
                    yield i
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=2,
        )
        
        pipe = Pipeline([Source(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=3)
        
        # Results should be in order: 0*2, 1*2, 2*2, ..., 9*2
        assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    def test_fifo_order_with_multiple_outputs_per_item(self):
        """FIFO should be maintained even when workers produce multiple outputs."""
        results = []
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)
            
            def build(self, context, *args):
                yield 2
                yield 3
                yield 4
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=MultiOutputWorker,  # Yields item and item^2
            num_workers=3,
        )
        
        pipe = Pipeline([Source(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=3)
        
        # Each input produces (item, item^2) in order
        # Input order: 2, 3, 4
        # Output order: (2, 4), (3, 9), (4, 16) flattened
        assert results == [2, 4, 3, 9, 4, 16]


class TestLoadBalancerIntegration:
    """Integration tests for the full load balancer in a pipeline."""

    def test_load_balancer_with_empty_worker_output(self):
        """Load balancer should handle workers that produce no output."""
        results = []
        
        class FilterWorker(ReactiveBuilder):
            """Only yields even numbers."""
            def __init__(self, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
            
            def build(self, context, item):
                if item % 2 == 0:
                    yield item
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)
            
            def build(self, context, *args):
                for i in range(6):
                    yield i
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=FilterWorker,
            num_workers=2,
        )
        
        pipe = Pipeline([Source(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=3)
        
        # Only even numbers, in order
        assert results == [0, 2, 4]


class TestLoadBalancerEdgeCases:
    """Edge case tests for load balancer."""

    def test_worker_with_init_args(self):
        """Load balancer should support workers with positional init args."""
        
        class WorkerWithArgs(ReactiveBuilder):
            def __init__(self, prefix, suffix, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
                self.prefix = prefix
                self.suffix = suffix
            
            def build(self, context, item):
                yield f"{self.prefix}{item}{self.suffix}"
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=WorkerWithArgs,
            num_workers=2,
            worker_init_args=("<<", ">>"),
        )
        
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        assert wrappers[0].worker.prefix == "<<"
        assert wrappers[0].worker.suffix == ">>"

    def test_large_sequence_gap(self):
        """Collector should handle large gaps in sequence numbers."""
        from framework.core import Context
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        # Send seq 0, then seq 1000
        r1 = list(collector.build(ctx, (0, ["r0"])))
        r2 = list(collector.build(ctx, (1000, ["r1000"])))
        
        assert r1 == ["r0"]
        assert r2 == []  # Waiting for seq 1
        assert collector.get_next_expected_seq() == 1
        assert 1000 in collector.buffer

    def test_sequencer_state_persists_across_calls(self):
        """Sequencer counter should persist across builds."""
        from framework.core import Context
        
        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input"],
        )
        
        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)
        
        # First batch
        list(sequencer.build(ctx, "a"))
        list(sequencer.build(ctx, "b"))
        
        # Second batch (should continue from where we left off)
        r3 = list(sequencer.build(ctx, "c"))
        r4 = list(sequencer.build(ctx, "d"))
        
        assert r3[0][0] == 2
        assert r4[0][0] == 3


class TestLoadBalancerWorkerIsolation:
    """Tests for worker instance isolation."""

    def test_worker_state_is_isolated(self):
        """Each worker instance should have independent state."""
        from framework.core import Context
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=StatefulCounter,
            num_workers=2,
        )
        
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        w1, w2 = wrappers
        
        pipeline = Pipeline(list(components))
        ctx = Context(pipeline)
        
        # Process items through each wrapper directly
        list(w1.build(ctx, (0, ("a",))))
        list(w1.build(ctx, (2, ("b",))))
        list(w2.build(ctx, (1, ("c",))))
        
        # w1's worker was called twice, w2's worker once
        assert w1.worker.call_count == 2
        assert w2.worker.call_count == 1


class TestLoadBalancerMultipleInputTopics:
    """Tests for load balancer with multiple input topics."""

    def test_multiple_requires_topics_joins_inputs(self):
        """Load balancer with multiple requires joins items from all topics.
        
        When a load balancer has multiple requires, it waits for items from
        ALL topics and passes them together to the worker (cross-product join).
        """
        results = []
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
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
            """Worker that expects two args (one from each input topic)."""
            def __init__(self, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
            
            def build(self, context, item_a, item_b):
                yield f"{item_a}+{item_b}"
        
        components = make_load_balancer(
            provides="output",
            requires=["input_a", "input_b"],
            worker_class=JoiningWorker,
            num_workers=2,
        )
        
        pipe = Pipeline([SourceA(), SourceB(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input_a"))
        pipe.submit(ReactiveEvent(name="resolve", target="input_b"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=4)
        
        # With cross-product join: (a1,b1), (a2,b2)
        # Results should be joined pairs in FIFO order
        assert len(results) == 2
        assert results[0] == "a1+b1"
        assert results[1] == "a2+b2"

    def test_sequencer_with_multiple_requires_preserves_all_args(self):
        """Sequencer should preserve all args when multiple requires are used."""
        from framework.core import Context
        
        sequencer = LoadBalancerSequencer(
            provides="sequenced",
            requires=["input_a", "input_b"],
        )
        
        pipeline = Pipeline([sequencer])
        ctx = Context(pipeline)
        
        # When called with multiple args (from multiple requires)
        results = list(sequencer.build(ctx, "val_a", "val_b"))
        
        seq, args = results[0]
        assert seq == 0
        assert args == ("val_a", "val_b")


class TestLoadBalancerExceptionHandling:
    """Tests for worker exception behavior."""

    def test_worker_exception_propagates(self):
        """Exceptions in worker.build() should propagate up."""
        from framework.core import Context
        
        class FailingWorker(ReactiveBuilder):
            def __init__(self, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
            
            def build(self, context, item):
                if item == "fail":
                    raise ValueError("Intentional failure")
                yield item
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=FailingWorker,
            num_workers=2,
        )
        
        # Find a wrapper to test directly
        wrapper = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)][0]
        
        pipeline = Pipeline(list(components))
        ctx = Context(pipeline)
        
        # Normal item should work
        results = list(wrapper.build(ctx, (0, ("ok",))))
        assert results == [(0, ["ok"])]
        
        # Failing item should raise
        with pytest.raises(ValueError, match="Intentional failure"):
            list(wrapper.build(ctx, (1, ("fail",))))

    def test_worker_exception_does_not_affect_other_workers(self):
        """An exception in one worker should not affect other workers."""
        from framework.core import Context
        
        class SelectiveFailer(ReactiveBuilder):
            def __init__(self, fail_on=None, **kwargs):
                super().__init__(provides="unused", requires=[], persist=False, **kwargs)
                self.fail_on = fail_on
            
            def build(self, context, item):
                if self.fail_on is not None and item == self.fail_on:
                    raise RuntimeError(f"Failed on {item}")
                yield item * 2
        
        # Create load balancer with 2 workers
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SelectiveFailer,
            num_workers=2,
        )
        
        wrappers = [c for c in components if isinstance(c, LoadBalancerWorkerWrapper)]
        w1, w2 = wrappers
        
        # Configure w1 to fail on specific input
        w1.worker.fail_on = 5
        
        pipeline = Pipeline(list(components))
        ctx = Context(pipeline)
        
        # w2 should still work fine
        results = list(w2.build(ctx, (1, (5,))))
        assert results == [(1, [10])]
        
        # w1 fails on 5
        with pytest.raises(RuntimeError, match="Failed on 5"):
            list(w1.build(ctx, (0, (5,))))


class TestLoadBalancerStress:
    """Stress tests for load balancer with many items and workers."""

    def test_many_items_single_worker(self):
        """Single worker should handle many items correctly."""
        results = []
        num_items = 1000
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)
            
            def build(self, context, *args):
                for i in range(num_items):
                    yield i
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=1,
        )
        
        pipe = Pipeline([Source(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=5)
        
        # All items should be processed in order
        assert len(results) == num_items
        expected = [i * 2 for i in range(num_items)]
        assert results == expected

    def test_many_items_many_workers(self):
        """Many workers should handle many items with correct FIFO ordering."""
        results = []
        num_items = 500
        num_workers = 10
        
        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="output", artifact=a):
                        results.append(a)
        
        class Source(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="input", requires=[], persist=False)
            
            def build(self, context, *args):
                for i in range(num_items):
                    yield i
        
        components = make_load_balancer(
            provides="output",
            requires=["input"],
            worker_class=SimpleDoubler,
            num_workers=num_workers,
        )
        
        pipe = Pipeline([Source(), *components, Sink()])
        
        pipe.submit(ReactiveEvent(name="resolve", target="input"))
        pipe.submit(ReactiveEvent(name="resolve", target="output"))
        
        run_dispatcher_once(pipe, pulses=10)
        
        # All items should be processed in FIFO order
        assert len(results) == num_items
        expected = [i * 2 for i in range(num_items)]
        assert results == expected

    def test_many_workers_component_count(self):
        """Verify component count scales correctly with worker count."""
        for num_workers in [1, 5, 10, 50]:
            components = make_load_balancer(
                provides="output",
                requires=["input"],
                worker_class=SimpleDoubler,
                num_workers=num_workers,
            )
            
            # Expected: 1 sequencer + N*(router + wrapper) + 1 collector
            expected_count = 1 + num_workers * 2 + 1
            assert len(components) == expected_count, f"Failed for num_workers={num_workers}"

    def test_stress_collector_reordering(self):
        """Collector should handle many out-of-order arrivals efficiently."""
        from framework.core import Context
        import random
        
        collector = LoadBalancerCollector(
            provides="output",
            requires=["results"],
        )
        
        pipeline = Pipeline([collector])
        ctx = Context(pipeline)
        
        num_items = 1000
        
        # Generate sequence numbers in random order (except 0 which we send last)
        seqs = list(range(1, num_items))
        random.shuffle(seqs)
        
        results = []
        
        # Send all items except seq 0
        for seq in seqs:
            results.extend(list(collector.build(ctx, (seq, [f"r{seq}"]))))
        
        # Nothing should have been emitted yet
        assert results == []
        assert collector.get_pending_count() == num_items - 1
        
        # Now send seq 0 - should flush everything
        results.extend(list(collector.build(ctx, (0, ["r0"]))))
        
        # All items should now be emitted in order
        assert len(results) == num_items
        expected = [f"r{i}" for i in range(num_items)]
        assert results == expected
        assert collector.get_pending_count() == 0

    def test_high_worker_count_distribution(self):
        """Work should be evenly distributed across many workers."""
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
        
        # Count items per worker
        counts = [0] * num_workers
        for seq in range(num_items):
            item = (seq, ("data",))
            for i, router in enumerate(routers):
                counts[i] += len(list(router.build(ctx, item)))
        
        # Distribution should be even (50 items per worker)
        expected_per_worker = num_items // num_workers
        for i, count in enumerate(counts):
            assert count == expected_per_worker, f"Worker {i} got {count}, expected {expected_per_worker}"
