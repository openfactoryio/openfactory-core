import inspect
from typing import Any, Callable, Dict, Annotated, get_type_hints, get_args, get_origin


def ofa_method(
        name: str | None = None,
        description: str | None = None,
        param_description: dict[str, str] | None = None
        ) -> Callable:
    """
    Decorator to mark an instance method as an OpenFactory callable method.

    This decorator inspects the function signature and attaches structured
    metadata to the method object for later registration with OpenFactory
    inside a class derived from :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`.

    It enforces the following constraints on the decorated method:
        1. The first parameter must be ``self``.
        2. Only named parameters are allowed (no positional-only parameters).
        3. No ``*args`` or ``**kwargs`` are allowed.

    The attached metadata includes:
        - ``method_name``: name of the method (defaults to the Python function name)
        - ``parameters``: dictionary of parameter names with their description, type, default, and required flag
        - ``description``: the method description

    Args:
        name (str | None): External name to register the method under.
            Defaults to `None`, which uses the Python function name.
        description (str | None): Optional short description of the method.
                                  Defaults to the function docstring if not provided.
        param_description (dict[str, str] | None): Optional dict mapping parameter
            names to user-friendly descriptions. Takes precedence over Annotated types.

    Raises:
        TypeError: If the decorated function does not have ``self`` as the first parameter.
        TypeError: If any parameter is positional-only, ``*args``, or ``**kwargs``.

    Returns:
        Callable: The original function with ``_ofa_method_metadata`` attached.

    .. admonition:: Decorator usage examples:

       .. code-block:: python

            from typing import Annotated

            class MyApp(OpenFactoryApp):

                # Decorator uses docstring
                @ofa_method()
                def home_axis(self):
                    \"""Move to home position.""\"
                    ...

                # Decorator overrides docstring with description argument
                @ofa_method(description="Move axis with optional speed.")
                def move_axis(self, x: float, y: float, speed: int = 100):
                    \"""This docstring will be ignored.""\"
                    ...

                # Using param_description dict
                @ofa_method(param_description={"x": "X coordinate", "y": "Y coordinate"})
                def move_axis_fast(self, x: float, y: float):
                    ...

                # Using Annotated types
                @ofa_method()
                def move_z_axis(
                    self,
                    z: Annotated[float, "Z coordinate"],
                    speed: Annotated[int, "Feed rate (optional; defaults to 100)"] = 100
                ):
                    ...
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
        type_hints = get_type_hints(func, include_extras=True)

        for param_name, param in list(params.items())[1:]:

            if param.kind not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                raise TypeError(
                    f"OpenFactory methods only support named parameters. "
                    f"Invalid parameter '{param_name}' in '{func.__name__}'."
                )

            # Determine parameter description
            param_desc = ""
            if param_description and param_name in param_description:
                param_desc = param_description[param_name]
            else:
                hint = type_hints.get(param_name)
                if hint is not None and get_origin(hint) is Annotated:
                    args = get_args(hint)
                    if len(args) > 1 and isinstance(args[1], str):
                        param_desc = args[1]

            parameter_metadata[param_name] = {
                "annotation": None if param.annotation is inspect._empty else param.annotation,
                "default": None if param.default is inspect._empty else param.default,
                "required": param.default is inspect._empty,
                "description": param_desc.strip()
            }

        doc = inspect.getdoc(func) or ""
        desc = description.strip() if description else doc

        func._ofa_method_metadata = {
            "method_name": name or func.__name__,
            "parameters": parameter_metadata,
            "description": desc.strip(),
        }

        return func

    return decorator
