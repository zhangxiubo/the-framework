"""Framework library for building applications."""

__version__ = "0.1.0"
__author__ = "Xiubo Zhang"

from .framework import (
    Pipeline,
    Event,
    Context,
    AbstractProcessor,
    caching,
    AbstractBuilder,
    builder,
    JsonLLoader,
    JsonLWriter,
    retry,
)

__all__ = [
    "Pipeline",
    "Event",
    "Context",
    "AbstractProcessor",
    "caching",
    "AbstractBuilder",
    "builder",
    "JsonLLoader",
    "JsonLWriter",
    "retry",
]


def get_version() -> str:
    """Return the version of the framework."""
    return __version__
