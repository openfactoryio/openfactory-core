"""
Connector Type

This module defines the `Connector` type used in OpenFactory application schemas.
It represents a discriminated union of all supported connector-specific Pydantic schemas
(e.g., MTConnectConnectorSchema, OPCUAConnectorSchema).

By using a Pydantic `Annotated[..., Field(discriminator='type')]`, the input data
is automatically parsed and validated against the correct connector schema based on
the `type` field.

Adding a New Connector Type:
---------------------------
1. Create a new connector schema subclass (e.g., MyNewConnectorSchema).
2. Add it to the `Connector` type using the `|` syntax.
3. Ensure the subclass defines a unique `type` literal matching the input `type` field.
"""

from typing import Annotated
from pydantic import Field
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema


Connector = Annotated[
    MTConnectConnectorSchema | OPCUAConnectorSchema,  # Add other connector models here as needed
    Field(
        discriminator='type',
        description="Discriminator field to select the correct connector schema based on the 'type' value."
    )
]
"""
Discriminated union of all supported connector schemas.

This is a type alias for all known connector schemas, used for validation
and parsing based on the `type` discriminator field.
"""
