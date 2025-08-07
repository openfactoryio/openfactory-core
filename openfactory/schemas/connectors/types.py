"""
Union of all supported connector schemas with type-based discrimination.

This module defines the `Connector` type as a discriminated union of all
connector-specific Pydantic models (e.g., MTConnect, OpenFactory, etc.).
It allows automatic parsing and validation of connector configurations based
on the `type` field present in the input data.

Add new connector schemas here to extend supported device connectors.
"""

from typing import Annotated, Union
from pydantic import Field
from openfactory.schemas.connectors.mtconnect import MTConnectConnector


Connector = Annotated[
    Union[
        MTConnectConnector,
        # Add other connector models here as needed
    ],
    Field(
        discriminator='type',
        description="Discriminator field to select the correct connector schema based on the 'type' value."
    )
]
