"""
Connector Type Union

This module defines the `Connector` type as a discriminated union of all supported
connector-specific Pydantic schemas (e.g., MTConnectConnectorSchema).

The union enables automatic parsing and validation of connector configurations
based on the `type` discriminator field present in the input data.

To extend support for additional connector types, add their schemas to the union.

Attributes:
    Connector (Annotated[Union[...], Field]): Type alias representing all known connector schemas with a discriminator on the `type` field.
"""

from typing import Annotated, Union
from pydantic import Field
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema


Connector = Annotated[
    Union[
        MTConnectConnectorSchema,
        OPCUAConnectorSchema,
        # Add other connector models here as needed
    ],
    Field(
        discriminator='type',
        description="Discriminator field to select the correct connector schema based on the 'type' value."
    )
]
"""
Union of all supported connector schemas with type-based discrimination.

This is a type alias for all known connector schemas, used for validation
and parsing based on the `type` discriminator field.
"""
