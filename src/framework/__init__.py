"""Event-driven framework for building application pipelines.

This package is organized into three high-level modules:
- framework.core: runtime primitives including Event, AbstractProcessor, Pipeline, Context, and a caching decorator.
- framework.builder: a dependency-oriented builder state machine layered on the event runtime.
- framework.io: processors that bridge JSON Lines (JSONL) files and events (streaming loader and writer).

Typing:
- This distribution ships a 'py.typed' marker and is intended to be used with type checkers (PEP 561).

Public API exposed by this package:
- __version__: the package version string.
- __author__: the package author string.
- get_version(): returns the package version. Implemented here to keep imports minimal and avoid cycles.

No behavior is executed at import time beyond exposing these symbols. The concrete runtime behavior lives in the modules listed above.
"""

__version__ = "0.1.0"
__author__ = "Xiubo Zhang"


def get_version() -> str:
    """Return the version of the framework."""
    return __version__
