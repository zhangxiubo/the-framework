from framework.core import Event
from .conftest import RecordingSink, LeafBuilder, PairBuilder, AbstractBuilder


class TopBuilder(AbstractBuilder):
    """
    Top-level builder requiring a single prerequisite 'x', returning f"Y[{x}]".
    """
    def build(self, context, target, x, *args, **kwargs):
        return f"Y[{x}]"


class SingleBuilder(AbstractBuilder):
    """
    Builder that requires a single prerequisite 'a' and returns it as the artifact.
    Used to validate FIFO replies to multiple requestors without argument-mismatch on dedup.
    """
    def build(self, context, target, a, *args, **kwargs):
        return f"{a}"


def test_integration_chain_a_b_to_x_to_y(make_pipeline, run_to_quiescence):
    """
    Purpose: End-to-end chain validation: build 'y' from 'x', where 'x' composes 'a' and 'b'.
    Ensures that PairBuilder resolves 'a' then 'b' before building, and that TopBuilder consumes
    the result to produce 'Y[A|B]'.

    Rationale: Confirms that dependency fan-out, prerequisite collection, and top-level composition
    interact correctly across multiple processors within a single Pipeline.run().

    Failure signals:
    - Resolve ordering mismatch indicates non-deterministic or incorrect fan-out.
    - Missing/incorrect built(y) implies broken data flow or build argument ordering issues.
    """
    # Leaves
    la = LeafBuilder(provides="a", requires=[], cache=False)  # -> "A"
    lb = LeafBuilder(provides="b", requires=[], cache=False)  # -> "B"
    # Middle pair x
    px = PairBuilder(provides="x", requires=["a", "b"], cache=False)  # -> "A|B"
    # Top y
    ty = TopBuilder(provides="y", requires=["x"], cache=False)  # -> "Y[A|B]"

    client = RecordingSink("client")
    tap = RecordingSink("tap")

    p = make_pipeline(la, lb, px, ty, client, tap)
    p.submit(Event(name="resolve", target="y", sender=client))
    run_to_quiescence(p)

    # Ensure resolves for a and b occurred before px built
    resolves = [e for e in tap.by_name("resolve") if getattr(e, "sender", None) is px]
    targets = [getattr(e, "target", None) for e in resolves]
    assert targets[:2] == ["a", "b"]  # order a then b

    # Final artifact
    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "y"]
    assert built_for_client, "Expected built(y) to client"
    assert built_for_client[-1].artifact == "Y[A|B]"


def test_multiple_concurrent_requestors_fifo(make_pipeline, run_to_quiescence):
    """
    Purpose: With two concurrent requestors of the same target, verify that built replies
    are delivered in FIFO order and that artifacts are equal.

    Rationale: The waiting state's RSVP queue must preserve order across multiple clients.

    Failure signals:
    - If idx_s1 >= idx_s2, the queue processing is not FIFO.
    - If artifacts differ, the builder produced inconsistent outputs for identical requests.
    """
    # Builder provides 't' requiring only 'a' (dedup no-ops). Focus: FIFO replies.
    la = LeafBuilder(provides="a", requires=[], cache=False)
    sb = SingleBuilder(provides="t", requires=["a"], cache=False)

    s1 = RecordingSink("s1")
    s2 = RecordingSink("s2")
    tap = RecordingSink("tap")

    p = make_pipeline(la, sb, s1, s2, tap)
    p.submit(Event(name="resolve", target="t", sender=s1))
    p.submit(Event(name="resolve", target="t", sender=s2))
    run_to_quiescence(p)

    built_events = [e for e in tap.by_name("built") if getattr(e, "target", None) == "t"]
    idx_s1 = next(i for i, e in enumerate(built_events) if getattr(e, "to", None) is s1)
    idx_s2 = next(i for i, e in enumerate(built_events) if getattr(e, "to", None) is s2)
    assert idx_s1 < idx_s2, "Expected FIFO built replies to multiple requestors"
    # Artifacts equal
    assert built_events[idx_s1].artifact == built_events[idx_s2].artifact