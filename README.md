# Framework

Framework is a Python event-processing runtime with deterministic replay and a "reactive" builder layer. It focuses on three things:

1. **Declarative routing.** Processors declare their interests via `match` patterns and the pipeline infers routing automatically.
2. **Deterministic execution.** Single-threaded inboxes per processor, cache replays, and phase markers keep event ordering predictable.
3. **Composable builders.** Reactive builders describe what they *provide* and what they *require*; the runtime handles discovery, fan-out, and caching of intermediate artifacts.

The source lives under [`src/framework`](src/framework). The most important modules are:

| Module | Description |
|--------|-------------|
| [`framework.core`](src/framework/core.py) | Runtime primitives: `Event`, `AbstractProcessor`, `Pipeline`, context utilities, retry/caching helpers, and persistence archives. |
| [`framework.reactive`](src/framework/reactive.py) | Higher-level builders (`ReactiveBuilder`, `StreamGrouper`, `Collector`, samplers, etc.) built on top of the core event loop. |

## Key concepts

- **Event** – plain object with a `name` and arbitrary attributes. Events form the units of work flowing through the system.
- **AbstractProcessor** – subclass this, implement `process(self, context, event)`, and use `match` statements to describe the events you care about. Interests are inferred automatically from your patterns.
- **Pipeline** – orchestrates dispatch. Each processor keeps an inbox with limited concurrency so stateful processors can mutate internal data safely.
- **Context** – helper passed to processors. Use `context.submit()` to emit follow-up events; the context records what you emitted so caching/replay can reproduce the exact sequence later.
- **ReactiveBuilder** – specialization that wires dependencies using `provides`/`requires`. Builders react to `ReactiveEvent` instances: `resolve` asks for a target, `built` announces that a target is available.

## Features

- AST-driven interest inference: `match` statements are parsed once so you do not maintain manual topic lists.
- Deterministic job accounting with cooperative phases (`__PHASE__` events) for multi-stage workflows.
- Interrupt-aware executors: SIGINT triggers a graceful shutdown via `InterruptHandler`.
- Pluggable persistence: archives default to in-memory caches, but a workspace path enables RocksDB-backed stores for long-lived caching.
- Reactive helpers: stream grouping, collectors, and samplers that operate entirely through events (no shared memory required).

## Installation

```bash
pip install .
```

The project targets Python 3.11+. Dependencies are declared in [`pyproject.toml`](pyproject.toml). A [`uv.lock`](uv.lock) is included if you prefer `uv` for reproducible environments.

## Quickstart

### Core pipeline

```python
from framework.core import AbstractProcessor, Context, Event, Pipeline

class Greeter(AbstractProcessor):
    def process(self, context: Context, event: Event) -> None:
        match event:
            case Event(name="start", who=who):
                context.submit(Event(name="greet", who=who))
            case Event(name="greet", who=who):
                print(f"Hello, {who}!")

pipeline = Pipeline([Greeter()])
pipeline.submit(Event(name="start", who="world"))
pipeline.run()
```

### Reactive builder

```python
from framework.reactive import ReactiveBuilder, ReactiveEvent
from framework.core import Pipeline

class Words(ReactiveBuilder):
    def __init__(self):
        super().__init__(provides="words", requires=[], persist=False)

    def build(self, context):
        yield from ["alpha", "beta", "gamma"]

pipeline = Pipeline([Words()])
pipeline.submit(ReactiveEvent(name="resolve", target="words"))
pipeline.run()
```

The builder receives a `resolve` request, materializes its artifacts inside `build`, and publishes each artifact as a `ReactiveEvent(name="built", target="words", artifact=value)`.

## Reactive utilities overview

- `Collector` – buffers artifacts per phase and only forwards if new data arrived since the last phase.
- `Grouper` – groups inputs by key per phase and emits `(key, group)` tuples when phases advance.
- `StreamGrouper` – streaming variant that emits whenever the key changes; useful for streaming joins or windowed aggregation.
- `StreamSampler` / `ReservoirSampler` – probabilistic sampling processors for high-volume streams.

Each helper is a `ReactiveBuilder`, so they inherit the same cache semantics and lifecycle hooks (`on_init`, `on_terminate`, `phase`).

## Running tests

Use the micromamba-managed environment provided in this repository:

```bash
micromamba run -p ./.venv pytest
```

This exercises both the core runtime and the reactive builders.

## Development tips

- When adding processors, prefer `match` instead of manual `if` chains so the pipeline can infer your interests.
- Use the `@caching` decorator for expensive or non-idempotent processors. It records the events you emitted and replays them when the same input arrives.
- The `Pipeline` emits `__PHASE__` events between waves of work. Hook into them for periodic maintenance or to flush streaming state safely.

