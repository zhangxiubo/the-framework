# Framework

1) Project Overview

A lightweight, event-driven runtime with per-processor single-threading, AST-based interest inference, a dependency-oriented builder state machine, and JSONL streaming IO. The core revolves around immutable [Event](src/framework/core.py:67) messages dispatched by [Pipeline](src/framework/core.py:198) to processors derived from [AbstractProcessor](src/framework/core.py:436). Interests are inferred from match-case patterns via [infer_interests()](src/framework/core.py:392); each processor executes on its own single-threaded executor, avoiding reentrancy. The builder layer provides a state machine with [AbstractBuilder](src/framework/builder.py:59) and [builder()](src/framework/builder.py:285). Streaming IO is handled by [JsonLLoader](src/framework/io.py:44) and [JsonLWriter](src/framework/io.py:179). The distribution is typed (PEP 561) and ships [src/framework/py.typed](src/framework/py.typed).


2) Features

- Runtime pipeline
  - [Pipeline](src/framework/core.py:198): single-thread dispatcher; per-processor executors; graceful shutdown.
- Processor abstraction and interest inference
  - [AbstractProcessor](src/framework/core.py:436): base class with per-instance single-threaded executor and archive access.
  - [infer_interests()](src/framework/core.py:392): infers interests from match-case patterns over [Event](src/framework/core.py:67).
  - Follow-up emission via [Context.submit()](src/framework/core.py:172).
- Reliability
  - Caching via [caching()](src/framework/core.py:492): digest of processor class source + input event JSON; deterministic replay of emitted events.
  - Retry via [retry()](src/framework/core.py:113).
- Builder machinery
  - [AbstractBuilder](src/framework/builder.py:59): resolve → prerequisites → internal build → ready → built replies.
  - [builder()](src/framework/builder.py:285): decorator to declare a single provided target and its requirements.
- Streaming IO
  - [JsonLLoader](src/framework/io.py:44)/[JsonLWriter](src/framework/io.py:179): stream JSONL into/out of the pipeline with typed payloads and POISON-based cleanup.
- Typing
  - Ships PEP 561 marker [src/framework/py.typed](src/framework/py.typed).


3) Installation

- With pip
  - If published: 
    ```
    pip install framework
    ```
  - From source (non-editable):
    ```
    pip install .
    ```
  - From source (editable/dev):
    ```
    pip install -e .
    ```
- With uv
  - Non-editable:
    ```
    uv pip install .
    ```
  - Editable/dev:
    ```
    uv pip install -e .
    ```
  - This project uses a uv build backend declared in [pyproject.toml](pyproject.toml) and includes a lock file [uv.lock](uv.lock).
- Python version
  - Classifier indicates Python 3.x (see [pyproject.toml](pyproject.toml)); requires-python is not declared. Tools like uv may warn and default to >=3.11 when resolving.
- Dependencies (from [pyproject.toml](pyproject.toml))
  - dill, klepto, pydantic, sqlalchemy; dev: pytest.


4) Quickstart

Below, a minimal processor that emits follow-up events via [Context.submit()](src/framework/core.py:172) and runs on the [Pipeline](src/framework/core.py:198). Note: [AbstractProcessor](src/framework/core.py:436) interest inference triggers on match-case clauses over [Event](src/framework/core.py:67).

```python
from framework.core import Pipeline, AbstractProcessor, Context, Event

class Greeter(AbstractProcessor):
    def process(self, context: Context, event: Event):
        match event:
            case Event(name="start"):
                context.submit(Event(name="greet", message="Hello, world!"))
            case Event(name="greet", message=str() as msg):
                print(f"Greeter received: {msg}")

p = Pipeline(processors=[Greeter()])
p.submit(Event(name="start"))
p.run()
```

Execution details:
- Single-threaded per-processor execution from [AbstractProcessor](src/framework/core.py:436) ensures serialized state.
- The dispatcher in [Pipeline.run()](src/framework/core.py:240) drains the queue to completion.
- Emitted events are re-queued via [Pipeline.submit()](src/framework/core.py:272) through [Context.submit()](src/framework/core.py:172).


5) Builder Example

Define two leaf builders and a composite builder using [builder()](src/framework/builder.py:285). The flow mirrors the tests: resolve(x) received by the composite, fan-out resolve(a), resolve(b), collect built(a/b), trigger internal build(x), emit internal ready(x), then built(x) back to the requester (RSVP FIFO is preserved in waiting state via [AbstractBuilder.process()](src/framework/builder.py:274)).

```python
from framework.core import Pipeline, Event, AbstractProcessor
from framework.builder import AbstractBuilder, builder

class Leaf(AbstractBuilder):
    def build(self, context, target, *args, **kwargs):
        return target.upper()

@builder(provides="x", requires=["a", "b"])
class Pair(AbstractBuilder):
    def build(self, context, target, a, b, *args, **kwargs):
        return f"{a}|{b}"

class Client(AbstractProcessor):
    def process(self, context, event):
        match event:
            case Event(name="built", target="x", to=self, artifact=artifact):
                print("artifact:", artifact)

client = Client()
p = Pipeline(processors=[Leaf(provides="a", requires=[], cache=False),
                         Leaf(provides="b", requires=[], cache=False),
                         Pair(), client])
p.submit(Event(name="resolve", target="x", sender=client))
p.run()
```

Output (pseudo):
```
resolve(a), resolve(b)  # from Pair
build(x)                # internal to Pair
ready(x)                # internal to Pair
built(x) -> client      # artifact: A|B
```

Key behavior:
- Exactly-once internal build trigger guarded by [AbstractBuilder.check_prerequisites()](src/framework/builder.py:242).
- FIFO RSVP draining when transitioning to ready (see [AbstractBuilder.waiting()](src/framework/builder.py:102) and [AbstractBuilder.ready()](src/framework/builder.py:140)).


6) Streaming IO Example

Stream a JSONL file into the pipeline and write it back out. The loader emits one [Event](src/framework/core.py:67) per line, then a completion sentinel name+"$". Both loader and writer close files on the special "__POISON__" event during shutdown through [Pipeline.run()](src/framework/core.py:240). The loader trigger envelope is [JsonLLoader.LoadJsonL](src/framework/io.py:94); processing is in [JsonLLoader.process()](src/framework/io.py:112) and [JsonLWriter.process()](src/framework/io.py:218).

```python
from pydantic import BaseModel
from framework.core import Pipeline
from framework.io import JsonLLoader, JsonLWriter

class Item(BaseModel):
    id: int
    text: str

loader = JsonLLoader(name="items", filepath="ins.jsonl", item_type=Item, attribute="item")
writer = JsonLWriter(name="items", filepath="outs.jsonl", item_type=Item, attribute="item")

p = Pipeline(processors=[loader, writer])
p.submit(loader.LoadJsonL(name="items", limit=100, sample=1.0))
p.run()  # graceful shutdown closes loader/writer
```

Notes:
- Completion sentinel: name+"$" emitted by [JsonLLoader.process()](src/framework/io.py:112).
- Resource cleanup on "__POISON__" handled in [JsonLLoader.process()](src/framework/io.py:112) and [JsonLWriter.process()](src/framework/io.py:218).
- Writer flushes after each line for durability.


7) Caching and Retry

- Caching semantics
  - Use [caching()](src/framework/core.py:492) on a processor’s process method to deterministically replay emitted events for identical inputs.
  - Digest key = sha256(dill.dumps([processor class source, input event JSON])) as implemented in [caching()](src/framework/core.py:492); code changes invalidate the cache.
  - Archive is obtained via [AbstractProcessor.archive()](src/framework/core.py:468) using klepto’s sql_archive per class when Pipeline has a workspace.
  - Caveat: Only emitted events are captured; external side effects are not recorded or replayed.
- Retry
  - Use [retry()](src/framework/core.py:113) to wrap functions with bounded retries. Intermediate failures log warnings; the final failure logs an error and re-raises.

Example (decorators on a processor):
```python
from framework.core import AbstractProcessor, Context, Event, caching, retry

class Flaky(AbstractProcessor):
    @caching(debug=False)
    @retry(3)
    def process(self, context: Context, event: Event):
        match event:
            case Event(name="work"):
                # may raise; on cache miss the emissions will be archived
                context.submit(Event(name="done"))
```


8) API Reference

- Package root [src/framework/__init__.py](src/framework/__init__.py)
  - [__version__](src/framework/__init__.py:19): package version string.
  - [__author__](src/framework/__init__.py:20): package author string.
  - [get_version()](src/framework/__init__.py:23): returns the version string.
  - Note: Only these symbols are exported at the package root; import all other APIs from their modules below.

- Core [src/framework/core.py](src/framework/core.py)
  - [Event](src/framework/core.py:67): immutable event model (Pydantic, frozen, extra allowed).
  - [wrap()](src/framework/core.py:80): middleware wrapper that logs exceptions in executor threads.
  - [retry()](src/framework/core.py:113): decorator factory to retry a callable up to N times.
  - [Context](src/framework/core.py:153) with [Context.submit()](src/framework/core.py:172): per-dispatch context and emission API.
  - [Pipeline](src/framework/core.py:198) with [Pipeline.run()](src/framework/core.py:240) and [Pipeline.submit()](src/framework/core.py:272): dispatcher loop, graceful shutdown, and event enqueue.
  - [parse_pattern()](src/framework/core.py:346): extract (class id, event name) from a match-case AST pattern.
  - [infer_interests()](src/framework/core.py:392): infer processor interests from process() AST.
  - [AbstractProcessor](src/framework/core.py:436): base processor with single-threaded executor and archive access.
  - [caching()](src/framework/core.py:492): cache/replay emitted events keyed by class source + input event JSON.

- Builder [src/framework/builder.py](src/framework/builder.py)
  - [AbstractBuilder](src/framework/builder.py:59) with [AbstractBuilder.build()](src/framework/builder.py:227) and [AbstractBuilder.process()](src/framework/builder.py:274): dependency-driven builder state machine.
  - [builder()](src/framework/builder.py:285): decorator to pre-bind provides/requires for subclasses.

- IO [src/framework/io.py](src/framework/io.py)
  - [JsonLLoader](src/framework/io.py:44) with [JsonLLoader.process()](src/framework/io.py:112): stream JSONL into events and emit name+"$" sentinel.
  - [JsonLWriter](src/framework/io.py:179) with [JsonLWriter.process()](src/framework/io.py:218): write typed events to JSONL; closes on "__POISON__".


9) Testing

- Summary: “10 passed in 0.15s” on Python 3.12.1.
- Commands:
  ```
  pip install -e .
  pytest -q
  ```
  Or with uv:
  ```
  uv pip install -e .
  uv run -m pytest -q
  ```
- Notes:
  - uv may warn about defaulting to Python >=3.11 due to missing requires-python in [pyproject.toml](pyproject.toml).
  - Tests cover builder fan-out ordering, RSVP FIFO, single build triggering, and integration chains; see [tests/test_builder_unit.py](tests/test_builder_unit.py) and [tests/test_builder_integration.py](tests/test_builder_integration.py). Fixtures in [tests/conftest.py](tests/conftest.py) provide runtime helpers.


10) License and Contributing

- License: MIT (per classifier in [pyproject.toml](pyproject.toml)).
- Contributing: Issues and pull requests are welcome. Keep changes typed (PEP 561 via [src/framework/py.typed](src/framework/py.typed)), add tests, and run pytest locally before submission.