""" OpenFactory apps module. """

from openfactory.apps.ofaapp import OpenFactoryApp
from openfactory.apps.decorators import ofa_method
from openfactory.apps.attributefield import AttributeField, EventAttribute, SampleAttribute

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openfactory.apps.ofa_fastapi_app import OpenFactoryFastAPIApp


__all__ = [
    "OpenFactoryApp",
    "ofa_method",
    "AttributeField",
    "EventAttribute",
    "SampleAttribute",
    "OpenFactoryFastAPIApp",
]


def __getattr__(name):
    if name == "OpenFactoryFastAPIApp":
        try:
            from openfactory.apps.ofa_fastapi_app import OpenFactoryFastAPIApp
        except ImportError as e:
            raise ImportError(
                "OpenFactoryFastAPIApp requires optional dependencies. "
                "Install it with `pip install openfactory[fastapi]`."
            ) from e

        globals()[name] = OpenFactoryFastAPIApp  # cache it
        return OpenFactoryFastAPIApp

    raise AttributeError(f"module {__name__} has no attribute {name}")
