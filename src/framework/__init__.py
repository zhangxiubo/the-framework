"""Event-driven runtime with deterministic replay and reactive builders.

The package exposes two user-facing modules:

* ``framework.core`` – runtime primitives such as :class:`~framework.core.Event`,
  :class:`~framework.core.AbstractProcessor`, :class:`~framework.core.Pipeline`,
  retry/caching helpers, and archive utilities.
* ``framework.reactive`` – higher-level builders that describe dependencies via
  ``provides``/``requires`` targets, plus ready-made processors for grouping,
  collecting, and sampling streams.

Typing support:

* The distribution ships a ``py.typed`` marker and is safe to consume from
  static type checkers (PEP 561).

Public API exposed at import time:

* ``__version__`` – package version string.
* ``__author__`` – package author string.
* ``get_version()`` – helper returning ``__version__`` without importing the
  full runtime, which keeps import cycles and heavy dependencies at bay.
"""

__version__ = "0.1.0"
__author__ = "Xiubo Zhang"


def get_version() -> str:
    """Return the version of the framework."""
    return __version__
