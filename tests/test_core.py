import ast
import queue
from typing import List

import pytest
from pydantic import ValidationError

from framework.core import (
    Event,
    wrap,
    retry,
    parse_pattern,
    infer_interests,
    AbstractProcessor,
    Pipeline,
    Context,
    Inbox,
    caching,
)


# -------------------------
# Helper processors for tests
# -------------------------


class AnySink(AbstractProcessor):
    """Receives all Event() instances via explicit match-case to infer (Event, None) interest."""

    def __init__(self, name: str = "any"):
        super().__init__()
        self.name = name
        self.events: List[Event] = []

    def process(self, context, event):
        match event:
            case Event():
                self.events.append(event)


class FooSink(AbstractProcessor):
    """Receives only Event(name='foo')"""

    def __init__(self):
        super().__init__()
        self.events: List[Event] = []

    def process(self, context, event):
        match event:
            case Event(name="foo"):
                self.events.append(event)


class WildcardSink(AbstractProcessor):
    """Declares no match-case; infer_interests yields (None, None). Receives all events only when strict_interest_inference=False."""

    def __init__(self):
        super().__init__()
        self.events: List[Event] = []

    def process(self, context, event):
        # No match-case on purpose; record whatever arrives
        self.events.append(event)


class SeqSink(AbstractProcessor):
    """Receives Event(name='tick', seq=int) and records the arrival order to validate per-processor FIFO"""

    def __init__(self):
        super().__init__()
        self.seq: List[int] = []

    def process(self, context, event):
        match event:
            case Event(name="tick", seq=int() as n):
                self.seq.append(n)


class Crasher(AbstractProcessor):
    """Raises on Event(name='boom') to test done_callback error logging and job accounting"""

    def process(self, context, event):
        match event:
            case Event(name="boom"):
                raise RuntimeError("boom")


class CachedProc(AbstractProcessor):
    """Processor whose emissions are cached/replayed keyed by class source + input event."""

    calls = 0

    @caching(debug=True)
    def process(self, context, event):
        match event:
            case Event(name="work"):
                type(self).calls += 1
                context.submit(Event(name="done"))
            case Event(name="work2"):
                type(self).calls += 1
                context.submit(Event(name="done2"))


class ArchProc(AbstractProcessor):
    """Minimal processor used to exercise AbstractProcessor.archive; process unused."""

    def process(self, context, event):
        pass


# -------------------------
# Event model
# -------------------------


def test_event_model_immutability_and_equality():
    e1 = Event(name="x", payload=123)  # extra fields allowed
    e2 = Event(name="x", payload=123)
    e3 = Event(name="x", payload=999)

    # Frozen/immutable - Pydantic raises ValidationError for frozen models
    with pytest.raises(ValidationError):
        setattr(e1, "name", "y")

    # Hashable and equality by value
    s = {e1, e2}
    assert len(s) == 1
    assert e1 == e2
    assert e1 != e3
    # Extra attrs preserved
    assert e1.payload == 123


# -------------------------
# wrap()
# -------------------------


def test_wrap_logs_and_reraises(caplog):
    def boom():
        raise ValueError("boom")

    wrapped = wrap(boom)

    with caplog.at_level("DEBUG", logger="framework.core"):
        with pytest.raises(ValueError):
            wrapped()

    msgs = " ".join(r.getMessage() for r in caplog.records if r.levelname == "DEBUG")
    assert "exception calling" in msgs
    assert "thread" in msgs  # thread name logged


# -------------------------
# retry()
# -------------------------


def test_retry_success_logs_and_returns(caplog):
    attempts = {"n": 0}

    @retry(3)
    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    with caplog.at_level("DEBUG", logger="framework.core"):
        assert flaky() == "ok"

    # Two retries should have been logged, then success
    warn_text = " ".join(
        r.getMessage() for r in caplog.records if r.levelname == "WARNING"
    )
    assert "retrying" in warn_text


def test_retry_escalates_after_max_attempts(caplog):
    @retry(2)
    def always_fail():
        raise RuntimeError("nope")

    with caplog.at_level("ERROR", logger="framework.core"):
        with pytest.raises(RuntimeError):
            always_fail()

    err_text = " ".join(
        r.getMessage() for r in caplog.records if r.levelname == "ERROR"
    )
    assert "escalating" in err_text


# -------------------------
# parse_pattern()
# -------------------------


def test_parse_pattern_variants_and_nesting():
    # Event(name="hello")
    p1 = ast.MatchClass(
        cls=ast.Name(id="Event"),
        patterns=[],
        kwd_attrs=["name"],
        kwd_patterns=[ast.MatchValue(value=ast.Constant(value="hello"))],
    )
    # Event()
    p2 = ast.MatchClass(
        cls=ast.Name(id="Event"), patterns=[], kwd_attrs=[], kwd_patterns=[]
    )

    # foo.Event(name="hello") via Attribute(cls=..., attr="Event")
    p_attr = ast.MatchClass(
        cls=ast.Attribute(value=ast.Name(id="foo"), attr="Event"),
        patterns=[],
        kwd_attrs=["name"],
        kwd_patterns=[ast.MatchValue(value=ast.Constant(value="hello"))],
    )

    # Or of two patterns
    p_or = ast.MatchOr(patterns=[p1, p2])

    # Nested AST node with 'pattern' attribute
    Node = type("Node", (ast.AST,), {"_fields": ("pattern",)})
    nested = Node(pattern=p1)

    assert list(parse_pattern(p1)) == [("Event", "hello")]
    assert list(parse_pattern(p2)) == [("Event", None)]
    assert list(parse_pattern(p_attr)) == [("Event", "hello")]
    assert list(parse_pattern(p_or)) == [("Event", "hello"), ("Event", None)]
    assert list(parse_pattern(nested)) == [("Event", "hello")]

    # Unrecognized pattern yields (None, None)
    assert list(parse_pattern(ast.MatchValue(value=ast.Constant(value=1)))) == [
        (None, None)
    ]


# -------------------------
# infer_interests()
# -------------------------


def test_infer_interests_from_process_ast(caplog):
    def proc(context, event):
        match event:
            case Event(name="work"):
                pass
            case Event():
                pass

    ints = list(infer_interests(proc))
    assert ("Event", "work") in ints
    assert ("Event", None) in ints
    assert all(len(t) == 2 for t in ints)

    # No match-case path logs a warning and yields (None, None)
    def no_match(context, event):
        return None

    with caplog.at_level("WARNING", logger="framework.core"):
        ints2 = list(infer_interests(no_match))
    assert ints2 == [(None, None)]
    wtxt = " ".join(r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    assert "does not seem to have declared any interest" in wtxt


# -------------------------
# Inbox
# -------------------------


class Dummy(AbstractProcessor):
    def process(self, context, event):
        pass


def test_inbox_slot_gating_and_take_event_errors():
    ready_q = queue.Queue()
    dummy = Dummy()
    inbox = Inbox(dummy, ready_q, concurrency=1)

    e1 = Event(name="a")
    e2 = Event(name="b")

    inbox.put_event(e1)
    inbox.put_event(e2)

    # Only one readiness notification while a slot is held
    assert ready_q.get_nowait() is dummy
    with pytest.raises(queue.Empty):
        ready_q.get_nowait()

    # FIFO take_event
    assert inbox.take_event() == e1

    # Signal task done -> reopen slot -> enqueue processor again since job remains
    inbox.mark_task_done()
    assert ready_q.get_nowait() is dummy

    # Drain second event
    assert inbox.take_event() == e2
    inbox.mark_task_done()

    # Taking from empty inbox raises and logs critical
    with pytest.raises(RuntimeError):
        inbox.take_event()


# -------------------------
# Pipeline: routing and job accounting
# -------------------------


def test_pipeline_routing_non_strict_and_strict():
    # Non-strict: wildcard (no match-case -> (None,None)) receives everything; FooSink only 'foo'
    wildcard = WildcardSink()
    foo_sink = FooSink()
    p = Pipeline(
        processors=[wildcard, foo_sink], strict_interest_inference=False, workspace=None
    )
    p.submit(Event(name="foo"))
    p.submit(Event(name="bar"))
    p.run()

    # Filter out __POISON__ which is used for pipeline shutdown
    wildcard_names = [e.name for e in wildcard.events if e.name != "__POISON__"]
    assert wildcard_names == ["foo", "bar"]
    assert [e.name for e in foo_sink.events] == ["foo"]

    # Strict: wildcard (None,None) should not receive; FooSink still receives 'foo'
    wildcard2 = WildcardSink()
    foo_sink2 = FooSink()
    p2 = Pipeline(
        processors=[wildcard2, foo_sink2],
        strict_interest_inference=True,
        workspace=None,
    )
    p2.submit(Event(name="foo"))
    p2.submit(Event(name="bar"))
    p2.run()

    assert [e.name for e in foo_sink2.events] == ["foo"]
    # Filter out __POISON__ in case it sneaks through
    wildcard2_names = [e.name for e in wildcard2.events if e.name != "__POISON__"]
    assert wildcard2_names == []  # no wildcard on strict


def test_pipeline_get_inbox_cached_and_jobs_quiesce():
    sink = AnySink()
    p = Pipeline(processors=[sink], strict_interest_inference=False, workspace=None)
    inbox1 = p.get_inbox(sink)
    inbox2 = p.get_inbox(sink)
    assert inbox1 is inbox2  # functools.cache

    p.submit(Event(name="x"))
    p.run()
    assert p.jobs == 0


def test_execute_events_preserves_per_processor_fifo():
    seq = SeqSink()
    p = Pipeline(processors=[seq], strict_interest_inference=False, workspace=None)
    for i in range(1, 6):
        p.submit(Event(name="tick", seq=i))
    p.run()
    assert seq.seq == [1, 2, 3, 4, 5]


def test_done_callback_logs_errors_and_quiesces(caplog):
    bad = Crasher()
    p = Pipeline(processors=[bad], strict_interest_inference=False, workspace=None)
    with caplog.at_level("ERROR", logger="framework.core"):
        p.submit(Event(name="boom"))
        p.run()
    msgs = " ".join(r.getMessage() for r in caplog.records if r.levelname == "ERROR")
    assert "processor failed" in msgs
    assert p.jobs == 0


def test_pipeline_run_terminates_and_logs(caplog):
    p = Pipeline(processors=[], strict_interest_inference=False, workspace=None)
    with caplog.at_level("INFO", logger="framework.core"):
        p.run()
    text = " ".join(r.getMessage() for r in caplog.records if r.levelname == "INFO")
    assert "dispatcher terminated" in text
    assert "pipeline run completed" in text


# -------------------------
# AbstractProcessor.archive()
# -------------------------


def test_archive_sql_persistence(tmp_path):
    a1 = ArchProc()
    arch1 = a1.archive(tmp_path)
    arch1["k"] = 42

    # New instance should see persisted value
    a2 = ArchProc()
    arch2 = a2.archive(tmp_path)
    assert "k" in arch2
    assert arch2["k"] == 42


# -------------------------
# caching() decorator
# -------------------------


def test_caching_replays_without_invoking_body(tmp_path):
    # First run: miss -> execute -> cache
    sink1 = AnySink("tap1")
    c1 = CachedProc()
    p1 = Pipeline(
        processors=[c1, sink1], strict_interest_inference=False, workspace=tmp_path
    )
    p1.submit(Event(name="work"))
    p1.run()
    names1 = [e.name for e in sink1.events]
    assert CachedProc.calls == 1
    assert "work" in names1
    assert "done" in names1

    # Second run (fresh pipeline/instance): hit -> replay -> no new underlying call
    sink2 = AnySink("tap2")
    c2 = CachedProc()
    p2 = Pipeline(
        processors=[c2, sink2], strict_interest_inference=False, workspace=tmp_path
    )
    p2.submit(Event(name="work"))
    p2.run()
    names2 = [e.name for e in sink2.events]
    assert CachedProc.calls == 1  # unchanged: no new execution
    assert "work" in names2
    assert "done" in names2

    # Different input -> miss -> execute -> cache increments
    sink3 = AnySink("tap3")
    c3 = CachedProc()
    p3 = Pipeline(
        processors=[c3, sink3], strict_interest_inference=False, workspace=tmp_path
    )
    p3.submit(Event(name="work2"))
    p3.run()
    names3 = [e.name for e in sink3.events]
    assert CachedProc.calls == 2
    assert "work2" in names3
    assert "done2" in names3


# -------------------------
# Context.submit() basic behavior
# -------------------------


def test_context_submit_records_and_enqueues(make_pipeline):
    """Use a real Pipeline + AnySink to validate Context.submit collects events and re-enqueues."""
    sink = AnySink()
    p = make_pipeline(sink)  # workspace=None, non-strict per fixture
    ctx = Context(p)
    # Submit two events via context; they should be recorded, then delivered once pipeline runs.
    ctx.submit(Event(name="alpha"))
    ctx.submit(Event(name="beta"))
    # Context.records should preserve order
    assert [e.name for e in ctx.events] == ["alpha", "beta"]
    # Drain the pipeline
    p.run()
    # Filter out __POISON__ which is used for pipeline shutdown
    sink_names = [e.name for e in sink.events if e.name != "__POISON__"]
    assert sink_names == ["alpha", "beta"]
