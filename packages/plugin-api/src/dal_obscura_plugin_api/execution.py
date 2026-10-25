"""Execution guards and deterministic resource cleanup for plugin authors."""

import sys
from collections.abc import Callable, Generator, Iterable
from functools import wraps
from inspect import isgeneratorfunction, signature
from typing import cast

from dal_obscura_plugin_api.contracts import ExecutionContext


def close_preserving_error(resource: object) -> None:
    """Close a resource without replacing an exception already in flight."""
    active_error = sys.exception()
    try:
        close = getattr(resource, "close", None)
        if callable(close):
            close()
    except BaseException:
        if active_error is None:
            raise


def close_all_preserving_error(resources: Iterable[object]) -> None:
    """Attempt every close, retaining the primary error or first cleanup failure."""
    first_error: BaseException | None = None
    for resource in resources:
        try:
            close_preserving_error(resource)
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def checked_iterator[T](values: Iterable[T], context: ExecutionContext) -> Generator[T]:
    """Guard each lazy pull and close both the iterator and its owner."""
    iterator = None
    try:
        iterator = iter(values)
        while True:
            context.check_active()
            try:
                value = next(iterator)
            except StopIteration:
                return
            context.check_active()
            yield value
    finally:
        try:
            if iterator is not values:
                close_preserving_error(iterator)
        finally:
            close_preserving_error(values)


def active[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Guard a synchronous provider operation taking a named ``context``.

    Lazy results require ``checked_iterator``. Long operations still need
    checkpoints between blocking provider calls.
    """
    parameters = signature(function)

    @wraps(function)
    def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
        context = parameters.bind(*args, **kwargs).arguments["context"]
        context.check_active()
        result = function(*args, **kwargs)
        if isgeneratorfunction(function):
            return cast(R, checked_iterator(cast(Iterable, result), context))
        try:
            context.check_active()
        except BaseException:
            close_preserving_error(result)
            raise
        return result

    return guarded
