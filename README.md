# Framework

`framework` is a Python event-processing runtime with a reactive builder layer.

It is built around:

1. Event routing inferred from `match` patterns in processors.
2. Per-processor mailboxes (single in-flight task per processor).
3. Phase-based execution (`__PHASE__`) and explicit shutdown (`__POISON__`).
4. Replay/caching utilities with optional RocksDB persistence.

Source code lives in [`src/framework`](src/framework).

## Modules

| Module | Purpose |
| --- | --- |
| [`framework.core`](src/framework/core.py) | `Event`, `Context`, `AbstractProcessor`, `Pipeline`, `Inbox`, and core decorators/utilities (`caching`, `retry`, `timeit`, interest inference helpers). |
| [`framework.reactive`](src/framework/reactive/__init__.py) | Package: `ReactiveBuilder`/`ReactiveEvent` ([builder](src/framework/reactive/builder.py)), stream helpers ([stream](src/framework/reactive/stream.py)), rendezvous ([rendezvous](src/framework/reactive/rendezvous.py)), load-balancer/parallelize ([parallelize](src/framework/reactive/parallelize.py)). |

## Installation

```bash
pip install .
```

With `uv` for local development:

```bash
uv sync --group dev
```

## Core runtime

### Event and processor model

- `Event(name=..., **attrs)` is a lightweight message object.
- Subclass `AbstractProcessor` and implement `process(self, context, event)`.
- `Context.submit(event)` emits downstream events.
- Processor interests are inferred from the `match` statement in `process`.

### Lifecycle events

The runtime emits three well-known events every processor can receive:

- `Event(name="__INIT__")` — emitted once by `Pipeline.__init__`, giving processors a chance to initialize state (e.g. restore persistent cursors).
- `Event(name="__PHASE__", phase=n)` — emitted at each phase boundary during `run()`.
- `Event(name="__POISON__")` — emitted once before shutdown so processors can flush/finalize.

Processors must `match` these events explicitly to receive them.

### Routing behavior

- By default (`strict_interest_inference=False`), processors with unrecognized/no `match` interests may still receive events through a generic `(None, None)` route.
- With `strict_interest_inference=True`, processors that declare no inferrable interests — or whose match patterns can't be statically recognized — raise `InterestInferenceError` at registration time. No silent wildcard subscription.
- Routing includes event class MRO names, so processors matching `Event(...)` can receive subclass instances.

### Dispatch and ordering

- Each processor has an `Inbox`; only one task per processor runs at a time.
- Multiple processors can run concurrently in a shared `ThreadPoolExecutor` (`max_workers`).
- Ready processors are ordered by `priority` in a `PriorityQueue` (lower numeric priority executes first). Ties within a priority resolve FIFO via an internal sequence counter.

### Pipeline lifecycle

`Pipeline.run()` does the following:

1. Starts dispatcher/executor threads.
2. Repeatedly emits `__PHASE__` and waits for quiescence. The phase fixed-point uses an internal-only job counter, so concurrent external `Pipeline.submit(...)` calls can't falsely convince the loop that a phase generated work.
3. Emits `__POISON__` for termination hooks.
4. Closes archives and shuts down workers.

`Pipeline.submit(event)` returns the number of recipient processors.

### Caching and persistence

- `@caching` (from `framework.core`) caches emitted events by input-event digest.
- Default archive backend is in-memory (`InMemCache`).
- Passing `workspace=Path(...)` to `Pipeline` enables RocksDB-backed archives via `pipeline.archive(...)`.
- RocksDB WAL durability is tunable via `Pipeline(archive_options={"wal_sync": False})`. Default is `True` (synchronous WAL writes — safer but slower).
- Processor signatures used as cache namespaces are hashed from AST-normalized class source, so whitespace/comment edits do not bust caches. Override the class attribute `version: str = "..."` to opt out of source hashing entirely and use an explicit version tag.

**`@caching` failure semantics**: if the wrapped method raises after having already submitted some events, those events have already propagated downstream and cannot be undone. The caching wrapper persists the partial transcript under the input digest before re-raising, so the next invocation with the same input replays exactly what downstream observed rather than re-executing the body. For retry-on-error, compose `@retry` *inside* the `@caching`-wrapped method so only the final successful run is captured.

## Quickstart (core)

```python
from framework.core import AbstractProcessor, Context, Event, Pipeline


class Greeter(AbstractProcessor):
    def process(self, context: Context, event: Event):
        match event:
            case Event(name="start", who=who):
                context.submit(Event(name="greet", who=who))
            case Event(name="greet", who=who):
                print(f"hello {who}")


pipeline = Pipeline([Greeter()], strict_interest_inference=True)
pipeline.submit(Event(name="start", who="world"))
pipeline.run()
```

## Reactive layer

`ReactiveBuilder` extends `AbstractProcessor` with dependency-driven build flows.

- `provides`: target this builder emits.
- `requires`: upstream targets this builder depends on.
- `ReactiveEvent(name="resolve", target=...)`: request artifacts for a target.
- `ReactiveEvent(name="built", target=..., artifact=...)`: announce produced artifact.
- Requirement names prefixed with `*` are expanded from tuple artifacts into positional args for `build` (note: not supported under `parallelize(...)`).

### persist vs dedupe

Two independent flags control archive behavior:

| `persist` | `dedupe` | `publish()` behavior |
| --- | --- | --- |
| `False` (default) | `True` (default) | Per-instance in-memory cache; same input digest is not rebuilt twice. |
| `False` | `False` | No memoization; `build` runs for every input, even repeats. |
| `True` | `True` (default) | Persistent archive; replays on `resolve`, dedupes across runs. |
| `True` | `False` | Persistent archive; every invocation runs `build` but results are stored (useful for audit/replay without skipping work). |

## Quickstart (reactive)

```python
from framework.core import AbstractProcessor, Pipeline
from framework.reactive import ReactiveBuilder, ReactiveEvent


class Numbers(ReactiveBuilder):
    def __init__(self):
        super().__init__(provides="numbers", requires=[], persist=False)

    def build(self, context):
        yield from [1, 2, 3]


class Doubles(ReactiveBuilder):
    def __init__(self):
        super().__init__(provides="doubles", requires=["numbers"], persist=False)

    def build(self, context, x):
        yield x * 2


class Sink(AbstractProcessor):
    def process(self, context, event):
        match event:
            case ReactiveEvent(name="built", target="doubles", artifact=value):
                print(value)


pipeline = Pipeline([Numbers(), Doubles(), Sink()])
pipeline.submit(ReactiveEvent(name="resolve", target="doubles"))
pipeline.run()
```

## Built-in reactive utilities

- `Collector`: batches incoming artifacts and emits on phase boundaries.
- `Grouper`: phase-based grouping by key.
- `StreamGrouper`: emits previous group when key changes, flushes on phase.
- `StreamSampler`: Bernoulli sampling.
- `ReservoirSampler`: bounded-size sample emitted on phase.

### Rendezvous

Use `make_rendezvous(provides, requires, keys)` to join multiple input streams by key.

- Returns `(publisher, receiver_0, receiver_1, ...)`.
- `RendezvousPublisher` accepts two policy strategies:
  - `orphan_policy`: `WaitForRendezvousOrphans` (default), `MaxPendingRendezvousKeys(max_pending=...)`.
  - `duplicate_policy`: `OverwriteDuplicates` (default, log-and-overwrite), `RejectDuplicates` (raise `ValueError`).

### Load balancing

Use `parallelize(...)` to fan out work across reactive builders.

- Worker outputs are forwarded by `ParallelResultEmitter` as soon as they're produced — there is **no FIFO reassembly across workers**. Downstream stages must be order-independent.
- **Worker contract**: worker classes must either inherit `ReactiveBuilder.__init__` unchanged, or accept `**kwargs` and forward `provides`/`requires`/`persist`/`name`/`priority` to `super().__init__`. Workers that hardcode wiring fail fast with a `TypeError` pointing at the contract.
- Workers whose `__init__` derives state from wiring fields (e.g. `self.arity = len(self.requires)`) see the *final* wiring directly; no post-hoc rewiring. If you have state that depends on wiring and is set outside `__init__`, override `on_parallelize_rewire()`.
- `*`-prefixed requires are rejected at `parallelize(...)` call time — the sequencing stage does not currently forward expansion metadata.
- Generated processor names automatically include `num_workers` so persisted graphs from different topologies do not share cache namespaces.
- Incoming tasks are sequenced once and distributed round-robin across worker instances. Sequence numbers exist only for routing, dedupe, and persistence — not as a downstream ordering contract.
- Reserved `parallelize(...)` kwargs: `persist`, `name` (namespace prefix), and `priority`.

## Running tests

```bash
uv run pytest -q
```

## Development notes

- Prefer `match` in `process()` so routing can be inferred.
- Keep processor state local to the processor; inbox scheduling guarantees single-task execution per processor.
- Use `strict_interest_inference=True` when you want only explicitly inferred routes — it now raises on unrecognized patterns instead of warning.
- Override `on_terminate` only after calling `super().on_terminate(context)` so persistent archives get a final WAL flush.
