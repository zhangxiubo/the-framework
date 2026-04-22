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


class InterestInferenceError(Exception):
    """Raised by ``infer_interests(..., strict=True)`` when interests cannot be inferred."""


def infer_interests(func, *, strict: bool = False):
    """Yield ``(event_class_name, event_name)`` pairs inferred from a
    processor's ``process`` method via AST inspection of ``match`` statements.

    When ``strict=True``, raise :class:`InterestInferenceError` instead of
    yielding the wildcard ``(None, None)`` fallback. This lets
    ``Pipeline(strict_interest_inference=True)`` fail loudly on processors
    that would otherwise silently subscribe to nothing (or everything).
    """

    def _fallback(reason: str):
        if strict:
            raise InterestInferenceError(f"{reason} for {func}")
        logger.warning(reason + " for %s; falling back to generic event interest", func)

    try:
        source = get_source(func)
    except (OSError, TypeError, IOError):
        _fallback("source unavailable")
        yield None, None
        return

    try:
        tree = ast.parse(textwrap.dedent(source))
    except SyntaxError:
        _fallback("could not parse source")
        yield None, None
        return
    match_stmts = find_match_stmts(tree)
    if not match_stmts:
        _fallback("processor declares no match-case interests")
        yield None, None
        return

    for match_stmt in match_stmts:
        for case in match_stmt.cases:
            for interest in parse_pattern(case.pattern):
                if interest == (None, None):
                    if strict:
                        raise InterestInferenceError(
                            f"failed to identify interests in processor {func}: "
                            "there is a match-case clause but we couldn't identify "
                            "a valid event-name combination."
                        )
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


@functools.lru_cache(maxsize=None)
def normalized_source_digest(cls) -> Optional[str]:
    """Return a stable SHA-256 hex digest of a class's source, or None if unavailable.

    Normalizes via ``ast.parse`` + ``ast.unparse`` so whitespace/comment edits
    don't bust the digest. Cached by class so many instances of the same class
    share the work.
    """
    import hashlib

    try:
        source = get_source(cls)
    except (OSError, TypeError, IOError):
        return None
    try:
        key_material = ast.unparse(ast.parse(textwrap.dedent(source)))
    except (SyntaxError, ValueError):
        key_material = source
    return hashlib.sha256(key_material.encode()).hexdigest()


def find_match_stmts(tree: ast.AST) -> list[ast.Match]:
    if not isinstance(tree, ast.Module):
        return []

    fn = next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ),
        None,
    )
    if fn is None:
        return []

    def walk(node: ast.AST):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            return
        if isinstance(node, ast.Match):
            yield node
        for child in ast.iter_child_nodes(node):
            yield from walk(child)

    matches = []
    for statement in fn.body:
        matches.extend(walk(statement))
    return matches


def in_jupyter_notebook() -> bool:
    try:
        from IPython import get_ipython

        shell = get_ipython().__class__.__name__
        return shell in {"ZMQInteractiveShell", "TerminalInteractiveShell"}
    except (NameError, ImportError, AttributeError):
        return False


def caching(func=None, *, debug: bool = True):
    """Cache events emitted by a processor's ``process`` method.

    The wrapped method must belong to an :class:`AbstractProcessor`; the
    decorator records every event submitted via ``context.submit(...)`` and
    replays them on the next invocation with an equivalent input digest.

    Runtime behavior on the happy path: on a cache hit, emitted events from
    the prior run are re-submitted and the underlying method is skipped.

    Behavior on exception (explicit contract):
      * Any events submitted before the exception have already propagated to
        downstream processors — this cannot be undone.
      * To keep replay consistent with what downstream processors observed,
        the currently-captured ``context.events`` are still persisted under
        the input digest *before* the exception is re-raised.
      * Future invocations with the same input will therefore be served from
        cache (exactly what downstream saw) rather than re-running the body.

    This favors at-least-once+replay consistency over "retry on next call";
    callers that want retry-on-error should compose :func:`retry` *inside* the
    cached method rather than around it.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, context, event, *args, **kwargs):
            digest = event_digest(event)
            if debug:
                logger.debug("digest %s", digest)

            archive = context.pipeline.archive(self.signature())
            if digest in archive:
                if debug:
                    logger.debug("cache hit: %s", digest)
                for cached_event in archive[digest]:
                    context.submit(cached_event)
                return

            try:
                func(self, context, event, *args, **kwargs)
            except Exception:
                # Persist whatever was emitted before the failure so replay
                # mirrors the observable downstream effect. Re-raise so the
                # pipeline can still log/propagate the error.
                archive[digest] = tuple(context.events)
                logger.exception(
                    "caching wrapper: method raised; persisted %d partial event(s) under digest %s",
                    len(context.events),
                    digest,
                )
                raise
            archive[digest] = tuple(context.events)

        return wrapper

    return decorator if func is None else decorator(func)


def event_digest(event) -> str:
    cache_key = [event]
    return deepdiff.DeepHash(cache_key)[cache_key]
