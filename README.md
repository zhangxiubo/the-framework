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
| [`framework.reactive`](src/framework/reactive.py) | `ReactiveBuilder`, `ReactiveEvent`, stream helpers, rendezvous utilities, and load-balancer utilities. |

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

### Routing behavior

- By default (`strict_interest_inference=False`), processors with unrecognized/no `match` interests may still receive events through a generic `(None, None)` route.
- With `strict_interest_inference=True`, only inferred interests are routed.
- Routing includes event class MRO names, so processors matching `Event(...)` can receive subclass instances.

### Dispatch and ordering

- Each processor has an `Inbox`; only one task per processor runs at a time.
- Multiple processors can run concurrently in a shared `ThreadPoolExecutor` (`max_workers`).
- Ready processors are ordered by `priority` in a `PriorityQueue` (lower numeric priority executes first).

### Pipeline lifecycle

`Pipeline.run()` does the following:

1. Starts dispatcher/executor threads.
2. Repeatedly emits `__PHASE__` and waits for quiescence.
3. Emits `__POISON__` for termination hooks.
4. Closes archives and shuts down workers.

`Pipeline.submit(event)` returns the number of recipient processors.

### Caching and persistence

- `@caching` (from `framework.core`) caches emitted events by input-event digest.
- Default archive backend is in-memory (`InMemCache`).
- Passing `workspace=Path(...)` to `Pipeline` enables RocksDB-backed archives via `pipeline.archive(...)`.

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
- `persist=True` stores build outputs in pipeline archives; future resolves replay cache.
- Requirement names prefixed with `*` are expanded from tuple artifacts into positional args for `build`.

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
- `RendezvousPublisher` supports orphan policies:
  - `WaitForRendezvousOrphans` (default)
  - `MaxPendingRendezvousKeys(max_pending=...)`

### Load balancing

Use `make_load_balancer(...)` to fan out work across worker builders.

- `preserve_fifo=True` (default): reassembles worker results in input order via `LoadBalancerCollector`.
- `preserve_fifo=False`: emits worker outputs immediately.
- Sequence gap policies:
  - `WaitForSequenceGaps` (default)
  - `SkipAheadOnBacklog(max_buffered=...)`
- `parallelize(...)` is a backward-compatible alias for `make_load_balancer(...)`.

## Running tests

```bash
uv run pytest -q
```

## Development notes

- Prefer `match` in `process()` so routing can be inferred.
- Keep processor state local to the processor; inbox scheduling guarantees single-task execution per processor.
- Use `strict_interest_inference=True` when you want only explicitly inferred routes.
