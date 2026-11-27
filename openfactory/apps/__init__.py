""" OpenFactory apps module. """

from openfactory.apps.ofaapp import OpenFactoryApp
from openfactory.apps.decorators import ofa_method
from openfactory.apps.attributefield import AttributeField, EventAttribute, SampleAttribute

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openfactory.apps.ofa_fastapi_app import OpenFactoryFastAPIApp
    from openfactory.apps.ofa_flask_app import OpenFactoryFlaskApp
    from openfactory.apps.ofa_streamlit_app import OpenFactoryStreamlitApp


__all__ = [
    "OpenFactoryApp",
    "ofa_method",
    "AttributeField",
    "EventAttribute",
    "SampleAttribute",
    "OpenFactoryFastAPIApp",
    "OpenFactoryFlaskApp",
    "OpenFactoryStreamlitApp",
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

    if name == "OpenFactoryFlaskApp":
        try:
            from openfactory.apps.ofa_flask_app import OpenFactoryFlaskApp
        except ImportError as e:
            raise ImportError(
                "OpenFactoryFlaskApp requires optional dependencies. "
                "Install it with `pip install openfactory[flask]`."
            ) from e

        globals()[name] = OpenFactoryFlaskApp
        return OpenFactoryFlaskApp

    if name == "OpenFactoryStreamlitApp":
        try:
            from openfactory.apps.ofa_streamlit_app import OpenFactoryStreamlitApp
        except ImportError as e:
            raise ImportError(
                "OpenFactoryStreamlitApp requires optional dependencies. "
                "Install it with `pip install openfactory[streamlit]`."
            ) from e

        globals()[name] = OpenFactoryStreamlitApp
        return OpenFactoryStreamlitApp

    raise AttributeError(f"module {__name__} has no attribute {name}")