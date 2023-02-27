"""Annotations Module."""

import warnings
from functools import wraps
from typing import Any, Callable, Optional, Type, TypeVar, cast

from awswrangler._config import _insert_str, config

FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


class SDKPandasDeprecatedWarning(Warning):
    """Deprecated Warning."""


class SDKPandasExperimentalWarning(Warning):
    """Experiental Warning."""


def _inject_note(
    doc: Optional[str],
    message: str,
) -> Optional[str]:
    if not doc or "\n    Parameters" not in doc:
        return doc
    note: str = f"\n\n    Note\n    ----\n    {message}\n\n"
    return _insert_str(text=doc, token="", insert=note)


def warn_message(
    message: str,
    warning_class: Type[Warning],
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
