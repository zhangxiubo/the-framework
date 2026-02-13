"""Unit tests for make_rendezvous and related classes."""

import threading

import pytest
from framework.core import AbstractProcessor, Event, Pipeline
from framework.reactive import (
    ReactiveBuilder,
    ReactiveEvent,
    MaxPendingRendezvousKeys,
    RendezvousPublisher,
    RendezvousReceiver,
    make_rendezvous,
)


def run_dispatcher_once(pipeline: Pipeline, pulses: int = 1):
    """Helper to run the pipeline dispatcher for a given number of pulses."""
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


class TestMakeRendezvousBasic:
    """Basic functionality tests for make_rendezvous."""

    def test_make_rendezvous_returns_correct_number_of_components(self):
        """make_rendezvous should return publisher + N receivers for N requirements."""
        publisher, *receivers = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[lambda x: x["id"], lambda x: x["id"]],
        )

        assert isinstance(publisher, RendezvousPublisher)
        assert len(receivers) == 2
        assert all(isinstance(r, RendezvousReceiver) for r in receivers)

    def test_make_rendezvous_receivers_have_correct_indices(self):
        """Each receiver should have the correct index corresponding to its position."""
        publisher, recv_a, recv_b, recv_c = make_rendezvous(
            provides="joined",
            requires=["a", "b", "c"],
            keys=[lambda x: x, lambda x: x, lambda x: x],
        )

        assert recv_a.index == 0
        assert recv_b.index == 1
        assert recv_c.index == 2

    def test_make_rendezvous_receivers_require_correct_topics(self):
        """Each receiver should require its corresponding input topic."""
        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[lambda x: x, lambda x: x],
        )

        assert recv_a.requires == ["stream_a"]
        assert recv_b.requires == ["stream_b"]

    def test_make_rendezvous_all_components_share_same_internal_topic(self):
        """Publisher and all receivers should use the same internal topic UUID."""
        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[lambda x: x, lambda x: x],
        )

        # Publisher requires the internal topic
        publisher_requires = publisher.requires
        assert len(publisher_requires) == 1
        internal_topic = publisher_requires[0]

        # All receivers provide to the same internal topic
        assert recv_a.provides == internal_topic
        assert recv_b.provides == internal_topic


class TestRendezvousReceiverBehavior:
    """Tests for RendezvousReceiver build behavior."""

    def test_receiver_tags_artifact_with_correct_index(self):
        """RendezvousReceiver.build should yield (index, args) tuple."""
        from framework.core import Context

        receiver = RendezvousReceiver(
            provides="internal_topic",
            requires=["input_stream"],
            index=42,
        )

        # Simulate build call
        pipeline = Pipeline([receiver])
        ctx = Context(pipeline)

        results = list(receiver.build(ctx, "test_value", "extra_arg"))

        assert len(results) == 1
        index, args = results[0]
        assert index == 42
        assert args == ("test_value", "extra_arg")

    def test_receiver_preserves_complex_artifacts(self):
        """Receiver should preserve complex artifact structures."""
        from framework.core import Context

        receiver = RendezvousReceiver(
            provides="topic",
            requires=["input"],
            index=0,
        )

        pipeline = Pipeline([receiver])
        ctx = Context(pipeline)

        complex_artifact = {"id": 123, "data": [1, 2, 3], "nested": {"key": "value"}}
        results = list(receiver.build(ctx, complex_artifact))

        index, args = results[0]
        assert args == (complex_artifact,)


class TestRendezvousPublisherBehavior:
    """Tests for RendezvousPublisher build behavior."""

    def test_publisher_joins_matching_keys_from_two_streams(self):
        """Publisher should emit joined tuple when all streams contribute same key."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["id"], lambda x: x["id"]],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Simulate receiving tagged items from two receivers
        item_from_stream_0 = (0, ({"id": "key1", "value": "a"},))
        item_from_stream_1 = (1, ({"id": "key1", "value": "b"},))

        # First item - should not emit yet (waiting for match)
        results1 = list(publisher.build(ctx, item_from_stream_0))
        assert results1 == []

        # Second item with matching key - should emit joined result
        results2 = list(publisher.build(ctx, item_from_stream_1))
        assert len(results2) == 1

        # Result should be ordered by stream index
        joined = results2[0]
        assert joined[0]["value"] == "a"  # from stream 0
        assert joined[1]["value"] == "b"  # from stream 1

    def test_publisher_uses_different_key_functions_per_stream(self):
        """Publisher should apply the correct key function for each stream."""
        from framework.core import Context

        # Stream 0 uses 'id' field, Stream 1 uses 'ref' field
        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["id"], lambda x: x["ref"]],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        item_from_stream_0 = (0, ({"id": "shared_key", "data": "from_0"},))
        item_from_stream_1 = (1, ({"ref": "shared_key", "data": "from_1"},))

        list(publisher.build(ctx, item_from_stream_0))
        results = list(publisher.build(ctx, item_from_stream_1))

        assert len(results) == 1
        assert results[0][0]["data"] == "from_0"
        assert results[0][1]["data"] == "from_1"

    def test_publisher_handles_three_way_rendezvous(self):
        """Publisher should handle joining three streams."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x, lambda x: x, lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send items from all three streams with the same key
        assert list(publisher.build(ctx, (0, ("key_a",)))) == []
        assert list(publisher.build(ctx, (1, ("key_a",)))) == []
        results = list(publisher.build(ctx, (2, ("key_a",))))

        assert len(results) == 1
        assert results[0] == ("key_a", "key_a", "key_a")

    def test_publisher_handles_multiple_different_keys(self):
        """Publisher should track multiple pending keys independently."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x, lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send key_a from stream 0
        list(publisher.build(ctx, (0, ("key_a",))))
        # Send key_b from stream 0
        list(publisher.build(ctx, (0, ("key_b",))))
        # Send key_b from stream 1 (should complete key_b)
        results_b = list(publisher.build(ctx, (1, ("key_b",))))
        # Send key_a from stream 1 (should complete key_a)
        results_a = list(publisher.build(ctx, (1, ("key_a",))))

        assert len(results_b) == 1
        assert results_b[0] == ("key_b", "key_b")

        assert len(results_a) == 1
        assert results_a[0] == ("key_a", "key_a")


class TestRendezvousBugExposure:
    """Tests that expose potential bugs in the rendezvous mechanism."""

    def test_fixed_duplicate_key_from_same_stream_before_match(self):
        """
        FIXED: When the same key arrives multiple times from one stream before
        the other stream contributes, the newer value overwrites the older one.
        The rendezvous still completes correctly when all streams contribute.
        """
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["key"], lambda x: x["key"]],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send "key_a" twice from stream 0 - second overwrites first
        list(publisher.build(ctx, (0, ({"key": "a", "seq": 1},))))
        list(publisher.build(ctx, (0, ({"key": "a", "seq": 2},))))

        # Only one entry should be in the index (the second one)
        assert len(publisher.index["a"]) == 1
        assert publisher.index["a"][0]["seq"] == 2

        # Send "key_a" from stream 1 - should complete successfully
        results = list(publisher.build(ctx, (1, ({"key": "a", "seq": 3},))))

        # Should successfully join with the second value from stream 0
        assert len(results) == 1
        assert results[0][0]["seq"] == 2  # from stream 0 (overwritten)
        assert results[0][1]["seq"] == 3  # from stream 1

    def test_orphaned_keys_can_be_cleaned(self):
        """
        Test that orphaned keys (partial matches that will never complete)
        can be cleaned up using the clear_orphaned_keys method.
        """
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x, lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send many unique keys from only stream 0
        for i in range(1000):
            list(publisher.build(ctx, (0, (f"orphan_key_{i}",))))

        # All these keys are stuck in the index, waiting for stream 1
        assert len(publisher.index) == 1000, "Keys should be accumulating"

        # Use the cleanup method to clear orphaned keys
        cleared = publisher.clear_orphaned_keys()
        assert cleared == 1000
        assert len(publisher.index) == 0, "Index should be empty after cleanup"

    def test_fixed_duplicate_key_overwrites_gracefully(self):
        """
        FIXED: Duplicate key from same stream now overwrites the previous value
        gracefully instead of crashing with an assertion.
        """
        from framework.core import Context
        import logging

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["key"], lambda x: x["key"]],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send same key twice from stream 0 - second should overwrite first
        list(publisher.build(ctx, (0, ({"key": "k", "value": "first"},))))
        list(publisher.build(ctx, (0, ({"key": "k", "value": "second"},))))

        # Now send from stream 1 - should complete successfully
        results = list(publisher.build(ctx, (1, ({"key": "k", "value": "from_1"},))))

        # Should complete with the second (overwritten) value from stream 0
        assert len(results) == 1
        assert results[0][0]["value"] == "second"
        assert results[0][1]["value"] == "from_1"

    def test_result_ordering_by_stream_index(self):
        """
        Test that results are correctly ordered by stream index,
        regardless of the order in which items arrive.
        """
        from framework.core import Context

        # All key functions extract the "key" field so items can match
        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[
                lambda x: x["key"],
                lambda x: x["key"],
                lambda x: x["key"],
            ],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Send items out of order: stream 2, then 0, then 1
        # All have the same key so they should match
        list(publisher.build(ctx, (2, ({"key": "shared", "src": "stream_2"},))))
        list(publisher.build(ctx, (0, ({"key": "shared", "src": "stream_0"},))))
        results = list(
            publisher.build(ctx, (1, ({"key": "shared", "src": "stream_1"},)))
        )

        # Should be sorted by stream index (0, 1, 2)
        assert len(results) == 1
        assert results[0][0]["src"] == "stream_0"
        assert results[0][1]["src"] == "stream_1"
        assert results[0][2]["src"] == "stream_2"

    def test_bug_key_extraction_failure_handling(self):
        """
        BUG: If a key function raises an exception, there's no error handling.
        This will crash the pipeline.
        """
        from framework.core import Context

        def failing_key_func(x):
            raise ValueError("Cannot extract key")

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["id"], failing_key_func],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Stream 0 works fine
        list(publisher.build(ctx, (0, ({"id": "key1"},))))

        # Stream 1 will fail when extracting key
        with pytest.raises(ValueError, match="Cannot extract key"):
            list(publisher.build(ctx, (1, ("some_value",))))


class TestRendezvousIntegration:
    """Integration tests for the full rendezvous mechanism in a pipeline."""

    def test_full_rendezvous_pipeline_two_streams(self):
        """Test complete rendezvous flow through a pipeline with two streams."""
        joined_results = []

        class Source1(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="stream_a", requires=[], persist=False)

            def build(self, context, *args):
                yield {"id": 1, "source": "a", "data": "alpha"}
                yield {"id": 2, "source": "a", "data": "beta"}

        class Source2(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="stream_b", requires=[], persist=False)

            def build(self, context, *args):
                yield {"id": 2, "source": "b", "data": "two"}
                yield {"id": 1, "source": "b", "data": "one"}

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="joined", artifact=a):
                        joined_results.append(a)

        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[lambda x: x["id"], lambda x: x["id"]],
        )

        pipe = Pipeline([Source1(), Source2(), publisher, recv_a, recv_b, Sink()])

        # Trigger both sources
        pipe.submit(ReactiveEvent(name="resolve", target="stream_a"))
        pipe.submit(ReactiveEvent(name="resolve", target="stream_b"))
        pipe.submit(ReactiveEvent(name="resolve", target="joined"))

        run_dispatcher_once(pipe, pulses=3)

        # Should have two joined results (id=1 and id=2)
        assert len(joined_results) == 2

        # Verify the join is correct
        for result in joined_results:
            assert result[0]["id"] == result[1]["id"]
            assert result[0]["source"] == "a"
            assert result[1]["source"] == "b"

    def test_rendezvous_with_unmatched_items(self):
        """Test that unmatched items don't produce spurious output."""
        joined_results = []

        class Source1(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="stream_a", requires=[], persist=False)

            def build(self, context, *args):
                yield {"id": 1, "source": "a"}
                yield {"id": 3, "source": "a"}  # No match in stream_b

        class Source2(ReactiveBuilder):
            def __init__(self):
                super().__init__(provides="stream_b", requires=[], persist=False)

            def build(self, context, *args):
                yield {"id": 1, "source": "b"}
                yield {"id": 2, "source": "b"}  # No match in stream_a

        class Sink(AbstractProcessor):
            def process(self, context, event):
                match event:
                    case ReactiveEvent(name="built", target="joined", artifact=a):
                        joined_results.append(a)

        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[lambda x: x["id"], lambda x: x["id"]],
        )

        pipe = Pipeline([Source1(), Source2(), publisher, recv_a, recv_b, Sink()])

        pipe.submit(ReactiveEvent(name="resolve", target="stream_a"))
        pipe.submit(ReactiveEvent(name="resolve", target="stream_b"))
        pipe.submit(ReactiveEvent(name="resolve", target="joined"))

        run_dispatcher_once(pipe, pulses=3)

        # Only id=1 should match
        assert len(joined_results) == 1
        assert joined_results[0][0]["id"] == 1
        assert joined_results[0][1]["id"] == 1


class TestRendezvousEdgeCases:
    """Edge case tests for rendezvous mechanism."""

    def test_single_stream_rendezvous(self):
        """Test rendezvous with only one stream (degenerate case)."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # With only one stream, every item should immediately "complete"
        results = list(publisher.build(ctx, (0, ("value",))))

        assert len(results) == 1
        assert results[0] == ("value",)

    def test_rendezvous_with_none_key(self):
        """Test that None can be used as a valid key."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x.get("id"), lambda x: x.get("id")],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Both items have None as the key
        list(publisher.build(ctx, (0, ({"value": "a"},))))  # id is None
        results = list(publisher.build(ctx, (1, ({"value": "b"},))))

        assert len(results) == 1

    def test_rendezvous_with_tuple_key(self):
        """Test that tuple can be used as a key (must be hashable)."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[
                lambda x: (x["a"], x["b"]),
                lambda x: (x["x"], x["y"]),
            ],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        list(publisher.build(ctx, (0, ({"a": 1, "b": 2, "data": "first"},))))
        results = list(publisher.build(ctx, (1, ({"x": 1, "y": 2, "data": "second"},))))

        assert len(results) == 1
        assert results[0][0]["data"] == "first"
        assert results[0][1]["data"] == "second"

    def test_rendezvous_key_cleaned_up_after_match(self):
        """Test that matched keys are properly removed from the index."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x, lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Complete a rendezvous
        list(publisher.build(ctx, (0, ("key_a",))))
        list(publisher.build(ctx, (1, ("key_a",))))

        # The key should be removed from the index
        assert "key_a" not in publisher.index

    def test_rendezvous_empty_value_handling(self):
        """Test rendezvous with empty/falsy values."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: "same_key", lambda x: "same_key"],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Use empty string, 0, and empty dict as values
        list(publisher.build(ctx, (0, ("",))))
        results = list(publisher.build(ctx, (1, (0,))))

        assert len(results) == 1
        assert results[0] == ("", 0)

    def test_rendezvous_orphan_policy_evicts_oldest_pending_keys(self):
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["id"], lambda x: x["id"]],
            orphan_policy=MaxPendingRendezvousKeys(max_pending=2),
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Fill pending with three unmatched keys from stream 0.
        list(publisher.build(ctx, (0, ({"id": "k1", "v": "a1"},))))
        list(publisher.build(ctx, (0, ({"id": "k2", "v": "a2"},))))
        list(publisher.build(ctx, (0, ({"id": "k3", "v": "a3"},))))

        # Policy should have evicted the oldest key (k1).
        assert set(publisher.index.keys()) == {"k2", "k3"}

        # k2 is still pending and should complete.
        joined = list(publisher.build(ctx, (1, ({"id": "k2", "v": "b2"},))))
        assert len(joined) == 1
        assert joined[0][0]["v"] == "a2"
        assert joined[0][1]["v"] == "b2"

        # k1 was already evicted, so a late partner from stream 1 cannot complete it.
        assert list(publisher.build(ctx, (1, ({"id": "k1", "v": "b1"},)))) == []


class TestRendezvousPublisherIndexUnpacking:
    """Tests specifically for the index unpacking logic in RendezvousPublisher.build."""

    def test_build_unpacks_item_correctly(self):
        """Test that the item unpacking i, (v, *_) = item works correctly."""
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            keys=[lambda x: x["id"]],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # The receiver yields (index, args) where args is a tuple
        # So item = (0, (actual_value,))
        item = (0, ({"id": "test", "data": "value"},))

        results = list(publisher.build(ctx, item))

        # With single stream, should complete immediately
        assert len(results) == 1
        assert results[0][0]["data"] == "value"

    def test_build_handles_multi_arg_receiver_output(self):
        """
        Test handling when receiver captures multiple args.
        RendezvousReceiver.build yields (index, args) where args can have multiple values.
        """
        from framework.core import Context

        publisher = RendezvousPublisher(
            provides="joined",
            requires=["internal"],
            # Key function receives just the first element (v) from unpacking (v, *_)
            keys=[lambda x: x],
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        # Simulate receiver output with multiple args
        # (index, (first_arg, second_arg, ...))
        item = (0, ("key_value", "extra1", "extra2"))

        results = list(publisher.build(ctx, item))

        # The key is extracted from just "key_value" (the first element)
        # The stored value v is also just "key_value"
        assert len(results) == 1
        assert results[0] == ("key_value",)


class TestMakeRendezvousFactoryFunction:
    """Tests for the make_rendezvous factory function itself."""

    def test_make_rendezvous_unique_topic_per_call(self):
        """Each call to make_rendezvous should create a unique internal topic."""
        pub1, recv1_a, recv1_b = make_rendezvous(
            provides="join1",
            requires=["a", "b"],
            keys=[lambda x: x, lambda x: x],
        )

        pub2, recv2_a, recv2_b = make_rendezvous(
            provides="join2",
            requires=["a", "b"],
            keys=[lambda x: x, lambda x: x],
        )

        # Internal topics should be different UUIDs
        topic1 = pub1.requires[0]
        topic2 = pub2.requires[0]
        assert topic1 != topic2

    def test_make_rendezvous_preserves_key_functions(self):
        """Key functions should be preserved and accessible."""
        key_a = lambda x: x["a"]
        key_b = lambda x: x["b"]

        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=[key_a, key_b],
        )

        assert publisher.keys[0] is key_a
        assert publisher.keys[1] is key_b

    def test_make_rendezvous_validates_keys_count_matches_requires(self):
        """make_rendezvous should raise ValueError if keys count doesn't match requires."""
        with pytest.raises(ValueError, match="Number of key functions"):
            make_rendezvous(
                provides="joined",
                requires=["a", "b", "c"],
                keys=[lambda x: x, lambda x: x],  # Only 2 keys for 3 requires
            )

    def test_make_rendezvous_handles_generator_keys(self):
        """Generator keys should be materialized to list (bug fix regression test)."""
        from framework.core import Context

        def key_funcs():
            yield lambda x: x["a"]
            yield lambda x: x["b"]

        publisher, recv_a, recv_b = make_rendezvous(
            provides="joined",
            requires=["stream_a", "stream_b"],
            keys=key_funcs(),
        )

        pipeline = Pipeline([publisher])
        ctx = Context(pipeline)

        list(publisher.build(ctx, (0, ({"a": "k1"},))))
        results = list(publisher.build(ctx, (1, ({"b": "k1"},))))

        assert len(results) == 1
