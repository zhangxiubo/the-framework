
from framework.core import Event

from .conftest import (
    RecordingSink,
    TriggerAfterReadySink,
    LeafBuilder,
    PairBuilder,
    CountingPairBuilder,
    ErrorBuilder,
    DecoratorBuilder,
)


def test_state_new_fanout(make_pipeline, run_to_quiescence):
    """
    Purpose: Verify that a PairBuilder initially in 'new' state fans out prerequisite
    resolves in declared order (requires = ["a", "b"]) and eventually delivers built(x)
    to the requester.

    Rationale: The order of fan-out dictates argument ordering to build(); preserving it
    is critical for deterministic and correct artifact composition.

    Failure signals:
    - If ordering assertion fails, the state machine may not respect requires ordering or
      is emitting resolves non-deterministically.
    - If built(x) is missing, RSVP draining/ready transition or prerequisite collection may
      be broken, preventing final replies.
    """
    # Builders
    la = LeafBuilder(provides="a", requires=[], cache=False)
    lb = LeafBuilder(provides="b", requires=[], cache=False)
    pb = PairBuilder(provides="x", requires=["a", "b"], cache=False)

    # Sinks
    client = RecordingSink("client")
    tap = RecordingSink("tap")  # global observer

    p = make_pipeline(la, lb, pb, client, tap)
    p.submit(Event(name="resolve", target="x", sender=client))
    run_to_quiescence(p)

    # Check fan-out order from pb: resolve a then resolve b
    resolves_from_pb = [e for e in tap.by_name("resolve") if getattr(e, "sender", None) is pb]
    targets = [getattr(e, "target", None) for e in resolves_from_pb]
    # There must be at least two resolves from the pair builder, first 'a' then 'b'
    assert targets.count("a") >= 1 and targets.count("b") >= 1
    first_a_idx = next(i for i, t in enumerate(targets) if t == "a")
    first_b_idx = next(i for i, t in enumerate(targets) if t == "b")
    assert first_a_idx < first_b_idx

    # Ensure we eventually built x for client
    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "x"]
    assert built_for_client, "Expected built(x) delivered to client"


def test_rsvp_fifo(make_pipeline, run_to_quiescence):
    """
    Purpose: Ensure that resolve requests that arrive before readiness are queued and
    answered in FIFO order with identical artifacts.

    Rationale: The waiting state maintains an RSVP queue; FIFO guarantees fairness and
    predictability to multiple clients.

    Failure signals:
    - If the index order is inverted/non-FIFO, the RSVP queue or its draining policy is incorrect.
    - If artifacts differ, the builder is producing inconsistent outputs for the same target.
    """
    # One leaf providing 't'
    leaf = LeafBuilder(provides="t", requires=[], cache=False)
    s1 = RecordingSink("s1")
    s2 = RecordingSink("s2")
    tap = RecordingSink("tap")

    p = make_pipeline(leaf, s1, s2, tap)

    # Two resolves before ready; they should be queued and answered FIFO
    p.submit(Event(name="resolve", target="t", sender=s1))
    p.submit(Event(name="resolve", target="t", sender=s2))
    run_to_quiescence(p)

    # Verify FIFO across the global tap
    built_events = [e for e in tap.by_name("built") if getattr(e, "target", None) == "t"]
    # Find first built to s1 and to s2
    idx_s1 = next(i for i, e in enumerate(built_events) if getattr(e, "to", None) is s1)
    idx_s2 = next(i for i, e in enumerate(built_events) if getattr(e, "to", None) is s2)
    assert idx_s1 < idx_s2, "RSVP FIFO not preserved"

    # Artifacts equal and non-None
    art1 = built_events[idx_s1].artifact
    art2 = built_events[idx_s2].artifact
    assert art1 is not None
    assert art1 == art2


def test_ready_immediate_reply(make_pipeline, run_to_quiescence):
    """
    Purpose: Validate that once a builder transitions to 'ready', subsequent resolves for
    the same target are answered immediately without re-building, and only one build/ready
    cycle occurs.

    Rationale: The ready state must serve cached artifacts to avoid redundant work and
    preserve idempotence within a run.

    Failure signals:
    - Multiple internal build/ready events: the state transition or guard is incorrect,
      causing duplicate builds.
    - Fewer than two built replies to the trigger: the immediate reply path is not functioning.
    """
    # Leaf builder t
    leaf = LeafBuilder(provides="t", requires=[], cache=False)
    # Trigger sink will submit a second resolve after it observes leaf's internal 'ready'
    trigger = TriggerAfterReadySink(target="t", builder_obj=leaf)
    tap = RecordingSink("tap")

    p = make_pipeline(leaf, trigger, tap)

    # First resolve from trigger
    p.submit(Event(name="resolve", target="t", sender=trigger))
    run_to_quiescence(p)

    # Observe that the trigger submitted two resolves (original + after ready)
    resolve_events = [e for e in tap.by_name("resolve") if getattr(e, "sender", None) is trigger and getattr(e, "target", None) == "t"]
    assert len(resolve_events) == 2

    # The leaf should perform exactly one internal build and exactly one internal ready transition
    build_events_for_leaf = [e for e in tap.by_name("build") if getattr(e, "to", None) is leaf]
    ready_events_for_leaf = [e for e in tap.by_name("ready") if getattr(e, "to", None) is leaf]
    assert len(build_events_for_leaf) == 1, "Expected exactly one internal build for leaf"
    assert len(ready_events_for_leaf) == 1, "Expected exactly one internal ready for leaf"

    # Both resolves to 't' must receive a built reply to the trigger; ordering across processors is not enforced
    built_to_trigger = [e for e in tap.by_name("built") if getattr(e, "to", None) is trigger and getattr(e, "target", None) == "t"]
    assert len(built_to_trigger) == 2, "Expected two built replies to trigger (before and after ready)"


def test_prerequisite_collection_single_build(make_pipeline, run_to_quiescence):
    """
    Purpose: Confirm that the builder triggers exactly one build() after collecting all
    prerequisites once per resolve request.

    Rationale: check_prerequisites() should submit a single internal 'build' event when
    the last prerequisite arrives; duplicate triggers would waste work or break semantics.

    Failure signals:
    - CountingPairBuilder.calls != 1 indicates duplicate triggering or missed build.
    - Missing built(x) to client suggests failure in ready/built emission or prerequisite handling.
    """
    # Pair builder 'x' requiring a, b; use CountingPairBuilder to verify single invocation
    la = LeafBuilder(provides="a", requires=[], cache=False)
    lb = LeafBuilder(provides="b", requires=[], cache=False)
    pb = CountingPairBuilder(provides="x", requires=["a", "b"], cache=False)

    client = RecordingSink("client")
    tap = RecordingSink("tap")

    p = make_pipeline(la, lb, pb, client, tap)
    p.submit(Event(name="resolve", target="x", sender=client))
    run_to_quiescence(p)

    # Exactly one build invocation
    assert CountingPairBuilder.calls == 1

    # Sanity: ready then built to client for x
    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "x"]
    assert built_for_client, "Expected built(x) to client"


def test_prerequisite_ordering_and_dedup(make_pipeline, run_to_quiescence):
    """
    Purpose: Ensure that duplicate requirements are deduplicated while preserving the
    first-occurrence order, and that argument order to build() respects the deduped sequence.

    Rationale: Dedup with order preservation avoids redundant dependencies while keeping
    deterministic argument mapping.

    Failure signals:
    - Artifact not equal to "A|B": wrong prerequisite order or missing dedup.
    - CountingPairBuilder.calls != 1: dedup failed or multiple builds were triggered.
    """
    # Requires declared with a duplicate; AbstractBuilder deduplicates preserving first occurrence
    la = LeafBuilder(provides="a", requires=[], cache=False)
    lb = LeafBuilder(provides="b", requires=[], cache=False)
    # Use CountingPairBuilder to also assert single build call
    pb = CountingPairBuilder(provides="x", requires=["a", "b", "a"], cache=False)

    client = RecordingSink("client")
    tap = RecordingSink("tap")

    p = make_pipeline(la, lb, pb, client, tap)
    p.submit(Event(name="resolve", target="x", sender=client))
    run_to_quiescence(p)

    # Dedup implies args order [a, b]; artifact must equal "A|B"
    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "x"]
    assert built_for_client, "Expected built(x) to client"
    assert built_for_client[-1].artifact == "A|B"
    assert CountingPairBuilder.calls == 1


def test_error_propagation_no_success(make_pipeline, run_to_quiescence, caplog):
    """
    Purpose: Verify that when a builder raises during build(), no ready or built(to=client)
    events are emitted for the failed target, and errors surface via logging.

    Rationale: The runtime should not produce success signals on failures; it should log
    at ERROR and avoid delivering invalid artifacts.

    Failure signals:
    - Presence of ready(to=err) or built(to=client) for 't' means failures are incorrectly
      being treated as success.
    - Absence of any ERROR logs is tolerated (stdout prints are allowed), but if logs exist
      at ERROR they should mention failure context.
    """
    err = ErrorBuilder(provides="t", requires=[], cache=False)
    client = RecordingSink("client")
    tap = RecordingSink("tap")

    p = make_pipeline(err, client, tap)
    p.submit(Event(name="resolve", target="t", sender=client))
    with caplog.at_level("ERROR"):
        run_to_quiescence(p)

    # No ready or built for error builder should have been emitted
    assert not any(e.name == "ready" and getattr(e, "to", None) is err for e in tap.by_name("ready"))
    assert not any(getattr(e, "to", None) is client and getattr(e, "target", None) == "t" for e in tap.by_name("built"))

    # Optional: sanity-check that something was logged; not asserting exact text to avoid brittleness
    # Accept either no logs (if runtime prints) or an error log
    # If logs exist at ERROR, they should mention the processor or event
    error_logs = [rec for rec in caplog.records if rec.levelname == "ERROR"]
    # Do not hard fail if logger uses print instead; presence of no-success events is the primary assertion
    # If there are error logs, they should contain some indication
    if error_logs:
        texts = " ".join(r.getMessage() for r in error_logs)
        assert "Error" in texts or "failed" in texts or "exception" in texts or "Traceback" in texts


def test_decorator_behavior_end_to_end(make_pipeline, run_to_quiescence):
    """
    Purpose: Validate that the @builder decorator correctly wires AbstractBuilder.__init__
    with provides/requires and that the decorated builder works end-to-end.

    Rationale: Users should be able to declare simple builders with @builder and construct
    them with no-arg __init__ while retaining correct behavior.

    Failure signals:
    - Attribute assertions fail: decorator did not bind provides/requires properly.
    - No built(d) to client or wrong artifact indicates runtime integration issues.
    """
    # Leaves for 'a' and 'b'
    la = LeafBuilder(provides="a", requires=[], cache=False)
    lb = LeafBuilder(provides="b", requires=[], cache=False)
    # Decorator-bound builder provides 'd' requires ['a', 'b']
    db = DecoratorBuilder()  # __init__ bound by @builder

    client = RecordingSink("client")
    tap = RecordingSink("tap")

    # Attribute assertions
    assert db.provides == "d"
    assert db.requires == ["a", "b"]

    p = make_pipeline(la, lb, db, client, tap)
    p.submit(Event(name="resolve", target="d", sender=client))
    run_to_quiescence(p)

    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "d"]
    assert built_for_client, "Expected built(d) to client"
    assert built_for_client[-1].artifact == "D:A+B"


def test_idempotent_build_triggering(make_pipeline, run_to_quiescence):
    """
    Purpose: Assert idempotent build triggering within a single resolve flow, producing
    exactly one build() invocation and a single built reply to the client.

    Rationale: Prevent duplicate internal 'build' submissions or multiple built replies
    for one resolve request.

    Failure signals:
    - CountingPairBuilder.calls != 1: duplicate builds or missed build.
    - More than one built(x) to client: reply-path duplication.
    """
    la = LeafBuilder(provides="a", requires=[], cache=False)
    lb = LeafBuilder(provides="b", requires=[], cache=False)
    pb = CountingPairBuilder(provides="x", requires=["a", "b"], cache=False)

    client = RecordingSink("client")
    tap = RecordingSink("tap")

    p = make_pipeline(la, lb, pb, client, tap)
    p.submit(Event(name="resolve", target="x", sender=client))
    run_to_quiescence(p)

    # Exactly one build invocation for a single resolve cycle
    assert CountingPairBuilder.calls == 1

    built_for_client = [e for e in tap.built_to(client) if getattr(e, "target", None) == "x"]
    assert len(built_for_client) == 1