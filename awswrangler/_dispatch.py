"""Dispatch engine and memory format configuration (PRIVATE)."""
from typing import Any, Callable, Dict, Optional

from awswrangler._distributed import engine


def dispatch_on_engine(func: Callable[..., Any]) -> Callable[..., Any]:
    """Dispatch on engine function decorator.

    Transforms a function into a dispatch function,
    which can have different behaviors based on the value of the distribution engine.
    """
    registry: Dict[str, Callable[..., Any]] = {}

    def dispatch(value: str) -> Callable[..., Any]:
        try:
            return registry[value]
        except KeyError:
            return func

    def register(value: str, func: Optional[Callable[..., Any]] = None) -> Callable[..., Any]:
        if func is None:
            return lambda f: register(value, f)
        registry[value] = func
        return func

    def wrapper(*args: Any, **kw: Dict[str, Any]) -> Any:
        return dispatch(engine.get())(*args, **kw)

    wrapper.register = register  # type: ignore
    wrapper.dispatch = dispatch  # type: ignore
    wrapper.registry = registry  # type: ignore

    return wrapper
