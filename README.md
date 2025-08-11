# Framework

A Python library for building applications with a clean, modular architecture.

## Installation

Install the library using pip:

```bash
pip install framework
```

Or install in development mode:

```bash
pip install -e .
```

## Usage

```python
from framework import hello

print(hello())
```

## Features

- Modular design
- Easy to extend
- Well-documented code
## Caching decorator

The caching decorator is now a decorator factory and must be used with parentheses.

Example:
```python
from framework.framework import AbstractProcessor, Event, Context, caching

class MyProcessor(AbstractProcessor):
    @caching(topic_names=("load",), independent=True, debug=False)  # parentheses required
    def process(self, context: Context, event: Event):
        # do work, emit events via context.submit(...)
        ...
```

Signature:
- caching(topic_names: Collection[str] = (), independent: bool = True, debug: bool = False)

Behavior:
- topic_names: cache only when the incoming Event.name is in this collection; empty means cache all.
- independent=True: cache key depends only on the current event (stateless).
- independent=False: cache key also incorporates the previous event history of the decorated function call chain.
- debug: print cache digests and hit information.

Note:
- Usage without parentheses (e.g., using @caching directly) is not supported.