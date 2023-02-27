"""Annotations Module."""

import logging
import warnings
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast

from awswrangler._config import _insert_str

FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


class SDKPandasDeprecatedWarning(Warning):
    """Deprecated Warning"""

    pass


class SDKPandasExperimentalWarning(Warning):
    """Experiental Warning"""

    pass


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
    warning_class: Warning,
    stacklevel: int = 2,
) -> Callable[[FunctionType], FunctionType]:
    def decorator(func: FunctionType) -> FunctionType:
        @wraps(func)
        def inner(*args: Any, **kwargs: Any) -> Any:
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
