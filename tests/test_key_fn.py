"""The dedupe/cache key is computed by an injectable ``Pipeline.key_fn``.

Default behaviour is content-structural hashing (DeepHash); callers may inject
a cheaper, equally-stable key function. These tests pin the seam: a custom
``key_fn`` must drive the dedupe decision in ``ReactiveBuilder.publish``.
"""
from framework.core import Pipeline
from framework.reactive import ReactiveBuilder, ReactiveEvent
from tests.helpers import run_dispatcher_once


def drive(builder, target, inputs, **pipe_kwargs):
    """Resolve `target`, then feed `inputs` as built events for the builder's
    single requirement, returning the pipeline after draining."""
    require = builder.requires[0]
    pipe = Pipeline([builder], **pipe_kwargs)
    pipe.submit(ReactiveEvent(name="resolve", target=target))
    run_dispatcher_once(pipe)
    for art in inputs:
        pipe.submit(ReactiveEvent(name="built", target=require, artifact=art))
    run_dispatcher_once(pipe)
    return pipe


def make_counter(builds):
    class Counter(ReactiveBuilder):
        def __init__(self):
            super().__init__(provides="Y", requires=["X"], persist=False)

        def build(self, context, x):
            builds.append(x)
            yield x

    return Counter()


def test_custom_key_fn_drives_dedupe_collapse():
    # A key_fn that maps every key to one constant => all distinct inputs
    # collide on the dedupe key, so only the first input is ever built.
    builds = []
    drive(make_counter(builds), "Y", [1, 2, 3], key_fn=lambda obj: "SAME")
    assert builds == [1]


def test_default_key_fn_dedupes_by_content():
    # Without an injected key_fn, distinct content builds and exact duplicates
    # are skipped (the historical DeepHash behaviour).
    builds = []
    drive(make_counter(builds), "Y", [1, 2, 2])
    assert builds == [1, 2]


def test_pipeline_digest_uses_injected_key_fn():
    # The digest seam itself is what builders call.
    seen = []

    def key_fn(obj):
        seen.append(obj)
        return "k"

    pipe = Pipeline([], key_fn=key_fn)
    assert pipe.digest(("a", "b")) == "k"
    assert seen == [("a", "b")]
