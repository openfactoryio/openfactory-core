import inspect
from typing import Any, Callable, Dict


def ofa_method(name: str | None = None) -> Callable:
    """
    Decorator to mark an instance method as an OpenFactory callable method.

    This decorator inspects the function signature and attaches structured
    metadata to the method object for later registration inside `OpenFactoryApp`.

    It enforces the following constraints on the decorated method:
        1. The first parameter must be `self`.
        2. Only named parameters are allowed (no positional-only parameters).
        3. No `*args` or `**kwargs` are allowed.

    The attached metadata includes:
        - `method_name`: external name of the method (defaults to the Python function name)
        - `parameters`: dictionary of parameter names with their type, default, and required flag
        - `docstring`: the original method docstring

    Args:
        name (str | None): External name to register the method under.
            Defaults to `None`, which uses the Python function name.

    Raises:
        TypeError: If the decorated function does not have `self` as the first parameter.
        TypeError: If any parameter is positional-only, `*args`, or `**kwargs`.

    Returns:
        Callable: The original function with `_ofa_method_metadata` attached.

    .. admonition:: Example:

       .. code-block:: python

            >>> class MyApp(OpenFactoryApp):
            ...     @ofa_method()
            ...     def move_axis(self, x: float, y: float, speed: int = 100):
            ...         return x, y, speed
            >>> MyApp.move_axis._ofa_method_metadata['method_name']
            'move_axis'
    """
    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        params = sig.parameters

        if not params:
            raise TypeError(
                "@ofa_method requires instance method with 'self' as first argument "
                f"({func.__name__} has no parameters)"
            )

        first_param = next(iter(params.values()))
        if first_param.name != "self":
            raise TypeError(
                "@ofa_method must decorate instance methods "
                "(first parameter must be 'self')"
            )

        parameter_metadata: Dict[str, Dict[str, Any]] = {}

        for param_name, param in list(params.items())[1:]:

            if param.kind not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                raise TypeError(
                    f"OpenFactory methods only support named parameters. "
                    f"Invalid parameter '{param_name}' in '{func.__name__}'."
                )

            parameter_metadata[param_name] = {
                "annotation": None
                if param.annotation is inspect._empty
                else param.annotation,
                "default": None
                if param.default is inspect._empty
                else param.default,
                "required": param.default is inspect._empty,
            }

        func._ofa_method_metadata = {
            "method_name": name or func.__name__,
            "parameters": parameter_metadata,
            "docstring": inspect.getdoc(func),
        }

        return func

    return decorator
