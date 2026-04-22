"""Stream-shaped utility builders: batching, grouping, sampling."""

from __future__ import annotations

import heapq
import itertools
import random
from collections import defaultdict
from collections.abc import Callable
from typing import Any

import deepdiff

from ..core import Context
from .builder import ReactiveBuilder


class Collector(ReactiveBuilder):
    """Batch inputs during a phase and forward them once per phase if changed."""

    def __init__(self, forwards_to, topics, limit=None):
        super().__init__(forwards_to, topics, persist=False)
        self.current_batch = []
        self.past_batches = []
        self.limit = float("inf") if limit is None else limit

    def build(self, context, *args, **kwargs):
        """Accumulate incoming artifacts up to the configured limit."""
        if len(self.current_batch) < self.limit:
            self.current_batch.append(args)
        yield from []

    def phase(self, context, phase):
        """Emit the batch when advancing phases and remember historical values."""
        batch, self.current_batch = self.current_batch, []
        if batch:
            self.past_batches.append(batch)
            yield batch

    def values(self):
        """Iterate over all values ever seen."""
        return itertools.chain.from_iterable((*self.past_batches, self.current_batch))


class Grouper(ReactiveBuilder):
    """Group artifacts by a derived key per phase."""

    def __init__(self, forwards_to, topics, keyfunc: Callable[..., Any]):
        super().__init__(forwards_to, topics, persist=False)
        self.keyfunc = keyfunc
        self.store = defaultdict(list)

    def build(self, context, *args):
        """Collect arguments under the derived key until the phase boundary."""
        key = self.keyfunc(*args)
        self.store[key].append(args)
        yield from []

    def phase(self, context, phase):
        """On each phase, emit all groups and reset the working store."""
        groups, self.store = self.store, defaultdict(list)
        for key, group in groups.items():
            yield key, group


class StreamGrouper(ReactiveBuilder):
    """Streaming grouper that emits whenever the key changes."""

    UNSET_SENTINEL = object()  # Sentinel that can't collide with any user key

    def __init__(
        self,
        provides,
        requires,
        keyfunc: Callable[..., Any],
        **kwargs,
    ):
        super().__init__(provides, requires, persist=False, **kwargs)
        self.lastkey = self.UNSET_SENTINEL
        self.lastset = []
        self.keyfunc = keyfunc

    def build(self, context, *args):
        """Emit the previous key's group when a new key is observed."""
        thiskey = self.keyfunc(*args)
        if thiskey == self.lastkey:
            self.lastset.append(args)
        else:
            if self.lastkey is not self.UNSET_SENTINEL:
                yield self.lastkey, self.lastset
            self.lastkey = thiskey
            self.lastset = [args]

    def phase(self, context, phase):
        """Flush any trailing group so nothing is lost between phases."""
        if self.lastkey is not self.UNSET_SENTINEL:
            yield self.lastkey, self.lastset
        self.lastkey = self.UNSET_SENTINEL
        self.lastset = []


class StreamSampler(ReactiveBuilder):
    """Emit each artifact with a fixed probability."""

    def __init__(self, provides, requires, probability: float, seed=42, **kwargs):
        super().__init__(provides, requires, **kwargs)
        self.probability = probability
        self.seed = seed
        self.random = random.Random(seed)

    def build(
        self,
        context,
        *args,
    ):
        """Yield ``args`` whenever the RNG draw is beneath ``probability``."""
        if self.random.random() < self.probability:
            yield args


class ReservoirSampler(ReactiveBuilder):
    """Bounded-size random sample maintained via reservoir sampling."""

    def __init__(self, provides, requires, size: int, **kwargs):
        super().__init__(provides, requires, **kwargs)
        self.size = size
        self.heap = []

    def build(
        self,
        context: Context,
        *args,
    ):
        """Keep a max-heap of random priorities and trim it to ``size``."""
        seed = deepdiff.DeepHash(args)[args]
        rng = random.Random(seed)
        heapq.heappush(self.heap, (rng.random(), args))
        if len(self.heap) > self.size:
            heapq.heappop(self.heap)
        yield from []

    def phase(self, context, phase):
        """Drain the heap and emit the sampled items."""
        while self.heap:
            _, item = heapq.heappop(self.heap)
            yield item
