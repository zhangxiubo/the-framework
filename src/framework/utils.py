"""Utility helpers shared by framework runtime primitives."""

from __future__ import annotations

import ast
import functools
import inspect
import logging
import textwrap
import time
from contextlib import contextmanager
from typing import Any, Optional

import deepdiff

logger = logging.getLogger(__name__)


@contextmanager
def timeit(name: str, logger: logging.Logger, logging_threshold: float = 5.0):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        if elapsed > logging_threshold:
            logger.debug("%s finished after %.4f seconds", name, elapsed)


def retry(max_attempts: int):
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                started = time.perf_counter()
                logger.debug("attempt %d / %d at %s began", attempt, max_attempts, func.__qualname__)
                try:
                    return func(*args, **kwargs)
                except Exception:
                    elapsed = time.perf_counter() - started
                    last = attempt == max_attempts
                    level = logger.error if last else logger.warning
                    action = "escalating" if last else "retrying"
                    level(
                        "attempt %d / %d at %s failed after %.4fs ... %s",
                        attempt,
                        max_attempts,
                        func.__qualname__,
                        elapsed,
                        action,
                        exc_info=True,
                    )
                    if last:
                        raise

        return wrapper

    return decorator


def class_identifier(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def parse_pattern(pattern):
    if isinstance(pattern, ast.MatchOr):
        for branch in pattern.patterns:
            yield from parse_pattern(branch)
        return

    nested = getattr(pattern, "pattern", None)
    if isinstance(nested, ast.AST):
        yield from parse_pattern(nested)
        return

    if not isinstance(pattern, ast.MatchClass):
        yield None, None
        return

    class_id = class_identifier(pattern.cls)
    if class_id is None:
        yield None, None
        return

    if "name" in pattern.kwd_attrs:
        name_idx = pattern.kwd_attrs.index("name")
        if name_idx < len(pattern.kwd_patterns):
            name = pattern.kwd_patterns[name_idx]
            if isinstance(name, ast.MatchValue) and isinstance(name.value, ast.Constant):
                yield class_id, name.value.value
                return

    yield class_id, None


def find_match_stmt(tree: ast.AST) -> Optional[ast.Match]:
    if not isinstance(tree, ast.Module):
        return None
    fn = next((node for node in tree.body if isinstance(node, ast.FunctionDef)), None)
    if fn is None:
        return None
    return next((node for node in reversed(fn.body) if isinstance(node, ast.Match)), None)


def infer_interests(func):
    tree = ast.parse(textwrap.dedent(get_source(func)))
    match_stmt = find_match_stmt(tree)
    if match_stmt is None:
        logger.warning(
            "The processor %s does not seem to have declared any interest to events",
            func,
        )
        yield None, None
        return

    for case in match_stmt.cases:
        for interest in parse_pattern(case.pattern):
            if interest == (None, None):
                logger.warning(
                    "failed to identify interests in processor %s: there is "
                    "a match-case clause but we couldn't identify a valid "
                    "event-name combination.",
                    func,
                )
            yield interest


def get_source(obj) -> str:
    if in_jupyter_notebook():
        from IPython.core import oinspect

        return oinspect.getsource(obj)
    return inspect.getsource(obj)


def in_jupyter_notebook() -> bool:
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        return shell in {"ZMQInteractiveShell", "TerminalInteractiveShell"}
    except (NameError, ImportError, AttributeError):
        return False


def caching(func=None, *, debug: bool = True):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, context, event, *args, **kwargs):
            try:
                from .core import AbstractProcessor

                if not isinstance(self, AbstractProcessor):
                    raise TypeError("caching can only wrap AbstractProcessor methods")

                digest = event_digest(event)
                if debug:
                    logger.debug("digest %s", digest)

                archive = context.pipeline.archive(self.name)
                if digest in archive:
                    if debug:
                        logger.debug("cache hit: %s", digest)
                    for cached_event in archive[digest]:
                        context.submit(cached_event)
                    return

                func(self, context, event, *args, **kwargs)
                archive[digest] = tuple(context.events)
            except Exception:
                logger.exception("caching wrapper failed")
                raise

        return wrapper

    return decorator if func is None else decorator(func)


def event_digest(event) -> str:
    cache_key = [event]
    return deepdiff.DeepHash(cache_key)[cache_key]
