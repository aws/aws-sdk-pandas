"""Annotations Module."""

from __future__ import annotations

import warnings
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from awswrangler._config import _insert_str, config

FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


class SDKPandasDeprecatedWarning(Warning):
    """Deprecated Warning."""


class SDKPandasExperimentalWarning(Warning):
    """Experimental Warning."""


def _inject_note(
    doc: str | None,
    message: str,
) -> str | None:
    token: str = "\n    Parameters"
    if not doc or token not in doc:
        return doc
    note: str = f"\n\n    Warning\n    ----\n    {message}\n\n"
    return _insert_str(text=doc, token=token, insert=note)


def warn_message(
    message: str,
    warning_class: type[Warning],
    stacklevel: int = 2,
) -> Callable[[FunctionType], FunctionType]:
    """Decorate functions with this to print warnings."""

    def decorator(func: FunctionType) -> FunctionType:
        @wraps(func)
        def inner(*args: Any, **kwargs: Any) -> Any:
            if not config.suppress_warnings:
                warnings.warn(f"`{func.__name__}`: {message}", warning_class, stacklevel=stacklevel)

            return func(*args, **kwargs)

        inner.__doc__ = _inject_note(
            doc=func.__doc__,
            message=message,
        )

        return cast(FunctionType, inner)

    return decorator


Deprecated = warn_message(
    "This API is deprecated and will be removed in future AWS SDK for Pandas releases. ",
    SDKPandasDeprecatedWarning,
)


Experimental = warn_message(
    "This API is experimental and may change in future AWS SDK for Pandas releases. ",
    SDKPandasExperimentalWarning,
)
