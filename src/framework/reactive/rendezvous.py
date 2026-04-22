"""Multi-stream key-based rendezvous / join primitives."""

from __future__ import annotations

import abc
import logging
from collections import OrderedDict
from collections.abc import Callable, Hashable
from typing import Any, Collection, Optional, Tuple

from ..core import Context
from .builder import ReactiveBuilder

logger = logging.getLogger(__name__)


class RendezvousOrphanPolicy(abc.ABC):
    """Strategy for handling unmatched rendezvous keys."""

    @abc.abstractmethod
    def on_partial(
        self,
        index: OrderedDict[Hashable, dict[int, Any]],
        required_streams: int,
    ) -> int:
        """Apply policy after a partial update. Returns number of evicted keys."""
        pass


class WaitForRendezvousOrphans(RendezvousOrphanPolicy):
    """Default rendezvous policy: retain partial keys indefinitely."""

    def on_partial(
        self,
        index: OrderedDict[Hashable, dict[int, Any]],
        required_streams: int,
    ) -> int:
        return 0


class MaxPendingRendezvousKeys(RendezvousOrphanPolicy):
    """Evict oldest partial keys once pending-key count exceeds ``max_pending``."""

    def __init__(self, max_pending: int):
        if max_pending < 1:
            raise ValueError("max_pending must be >= 1")
        self.max_pending = max_pending

    def on_partial(
        self,
        index: OrderedDict[Hashable, dict[int, Any]],
        required_streams: int,
    ) -> int:
        evicted = 0
        while len(index) > self.max_pending:
            index.popitem(last=False)
            evicted += 1
        return evicted


class RendezvousDuplicatePolicy(abc.ABC):
    """Strategy for handling duplicate keys arriving from the same stream."""

    @abc.abstractmethod
    def on_duplicate(self, key: Hashable, stream_index: int, old: Any, new: Any) -> Any:
        """Return the value to keep in the slot. Raise to abort the build."""
        pass


class OverwriteDuplicates(RendezvousDuplicatePolicy):
    """Default policy: newer value overwrites older with a warning log."""

    def on_duplicate(self, key: Hashable, stream_index: int, old: Any, new: Any) -> Any:
        logger.warning(
            "Rendezvous: duplicate key '%s' from stream %s, overwriting previous value",
            key,
            stream_index,
        )
        return new


class RejectDuplicates(RendezvousDuplicatePolicy):
    """Fail-fast policy: raise ValueError when a duplicate key arrives."""

    def on_duplicate(self, key: Hashable, stream_index: int, old: Any, new: Any) -> Any:
        raise ValueError(
            f"Rendezvous: duplicate key '{key}' from stream {stream_index}; "
            "duplicate policy is RejectDuplicates"
        )


class RendezvousPublisher(ReactiveBuilder):
    """Joins items from multiple streams by matching keys.

    Items from each input stream are collected and indexed by their extracted key.
    When all streams have contributed an item with the same key, the joined tuple
    is emitted, ordered by stream index.
    """

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        keys: Collection[Callable[[Any], Hashable]],
        orphan_policy: Optional[RendezvousOrphanPolicy] = None,
        duplicate_policy: Optional[RendezvousDuplicatePolicy] = None,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.index: OrderedDict[Hashable, dict[int, Any]] = OrderedDict()
        self.keys = keys
        self.orphan_policy = (
            WaitForRendezvousOrphans() if orphan_policy is None else orphan_policy
        )
        self.duplicate_policy = (
            OverwriteDuplicates() if duplicate_policy is None else duplicate_policy
        )

    def build(self, context: Context, item: Tuple[int, Any]):
        i, (v, *_) = item
        key_func = self.keys[i]
        k = key_func(v)
        slot = self.index.setdefault(k, {})

        if i in slot:
            v = self.duplicate_policy.on_duplicate(k, i, slot[i], v)

        slot[i] = v

        if len(slot) == len(self.keys):
            result = tuple(slot[idx] for idx in range(len(self.keys)))
            del self.index[k]
            yield result
            return

        evicted = self.orphan_policy.on_partial(self.index, len(self.keys))
        if evicted:
            logger.warning(
                "Rendezvous: evicted %d pending key(s) due to orphan policy %s",
                evicted,
                self.orphan_policy.__class__.__name__,
            )

    def clear_orphaned_keys(self) -> int:
        """Remove keys that will never complete. Returns count of removed keys."""
        count = len(self.index)
        self.index.clear()
        return count


class RendezvousReceiver(ReactiveBuilder):
    """Tags incoming artifacts with a stream index for the RendezvousPublisher."""

    def __init__(
        self,
        provides: str,
        requires: Collection[str],
        index: int,
        **kwargs,
    ) -> None:
        super().__init__(provides, requires, persist=False, **kwargs)
        self.index = index

    def build(self, context: Context, *args, **kwargs):
        yield self.index, args


def make_rendezvous(
    provides: str,
    requires: Collection[str],
    keys: Collection[Callable[[Any], Hashable]],
) -> Tuple[RendezvousPublisher, ...]:
    """Create a rendezvous pattern that joins multiple input streams by key.

    Returns ``(publisher, receiver_0, receiver_1, ...)``. Receivers tag incoming
    artifacts with their stream index; the publisher joins matching keys.
    """
    import uuid

    keys_list = list(keys)  # Materialize to support generators
    if len(requires) != len(keys_list):
        raise ValueError(
            f"Number of key functions ({len(keys_list)}) must match "
            f"number of required streams ({len(requires)})"
        )

    topic = str(uuid.uuid4())
    publisher = RendezvousPublisher(provides, [topic], keys_list)
    receivers = tuple(
        RendezvousReceiver(topic, [require], i) for i, require in enumerate(requires)
    )
    return (publisher, *receivers)
