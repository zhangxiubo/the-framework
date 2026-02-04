"""Tests for ReservoirSampler sampling behavior.

These tests verify the deterministic content-based sampling which ensures
replay consistency when items arrive in different orders.
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.append(str(Path(__file__).parents[1] / "src"))

from framework.core import Context, Pipeline
from framework.reactive import ReservoirSampler


class TestReservoirSamplerDeterminism:
    """Tests for ReservoirSampler's content-based deterministic sampling."""

    def test_same_content_produces_same_sample_regardless_of_order(self):
        """Same content should produce same sample (deterministic by content hash).

        The seed is derived from content hash, so identical content always gets
        the same random priority. Items fed in different orders produce same sample.
        """
        sample_size = 5
        items = [
            "apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
            "honeydew",
            "kiwi",
            "lemon",
        ]

        # Sample 1: items in original order
        sampler1 = ReservoirSampler(
            provides="sampled", requires=["input"], size=sample_size
        )
        pipeline1 = Pipeline([sampler1])
        ctx1 = Context(pipeline1)

        for item in items:
            list(sampler1.build(ctx1, item))
        sample1 = set(sampler1.phase(ctx1, 1))

        # Sample 2: items in reversed order
        sampler2 = ReservoirSampler(
            provides="sampled", requires=["input"], size=sample_size
        )
        pipeline2 = Pipeline([sampler2])
        ctx2 = Context(pipeline2)

        for item in reversed(items):
            list(sampler2.build(ctx2, item))
        sample2 = set(sampler2.phase(ctx2, 1))

        # Same items should be selected regardless of insertion order
        assert sample1 == sample2, (
            f"Deterministic sampling should produce same results. "
            f"Got {sample1} vs {sample2}"
        )

    def test_sample_size_respected(self):
        """Sampler should never return more than requested size."""
        sample_size = 5
        sampler = ReservoirSampler(
            provides="sampled", requires=["input"], size=sample_size
        )
        pipeline = Pipeline([sampler])
        ctx = Context(pipeline)

        for i in range(100):
            list(sampler.build(ctx, i))

        sample = list(sampler.phase(ctx, 1))
        assert len(sample) == sample_size

    def test_small_input_returns_all_items(self):
        """When input size < sample size, all items should be returned."""
        sample_size = 10
        sampler = ReservoirSampler(
            provides="sampled", requires=["input"], size=sample_size
        )
        pipeline = Pipeline([sampler])
        ctx = Context(pipeline)

        items = ["a", "b", "c"]
        for item in items:
            list(sampler.build(ctx, item))

        sample = list(sampler.phase(ctx, 1))
        # Items are stored as arg tuples
        assert len(sample) == len(items)
