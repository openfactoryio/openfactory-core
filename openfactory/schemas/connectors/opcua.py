"""
OPC UA Connector Schemas

This module provides Pydantic models to define and validate configuration
schemas for OPC UA devices within OpenFactory.

Key Models:
-----------
- OPCUAServerConfig:
  Configuration for the OPC UA server, including the endpoint URI and namespace URI.

- OPCUADeviceConfig:
  Configuration for a device on the OPC UA server. Allows specifying the device
  either by a hierarchical path or a NodeId. Variables and methods are optional.

- OPCUAConnectorSchema:
  Wrapper schema that encapsulates the server and device configurations.

Validation Features:
--------------------
- Ensures that either 'path' or 'node_id' is provided for a device.
- Variables and methods are optional, providing flexibility for different server setups.
- Forbids unknown fields to ensure strict schema conformance.

YAML Example:
-------------
.. code-block:: yaml

    server:
      uri: opc.tcp://127.0.0.1:4840/freeopcua/server/
      namespace_uri: http://examples.openfactory.local/opcua

    device:
      path: Sensors/TemperatureSensor_1
      variables:
        temp: Temperature
        hum: Humidity
      methods:
        calibrate: Calibrate

This module is essential for configuring OPC UA connectors in OpenFactory,
ensuring consistent and valid device subscriptions.
"""

import re
from typing import Optional, Dict, Literal
from pydantic import BaseModel, ConfigDict, model_validator, Field


class OPCUAServerConfig(BaseModel):
    """ OPC UA Server configuration. """
    uri: str = Field(..., description="OPC UA server endpoint URI.")
    namespace_uri: str = Field(..., description="Namespace URI of the OPC UA server.")

    model_config = ConfigDict(extra="forbid")


class OPCUADeviceConfig(BaseModel):
    """ OPC UA Device configuration. """
    path: Optional[str] = Field(
        default=None,
        description="Hierarchical path to the device (e.g., 'Sensors/TemperatureSensor_1')."
    )
    node_id: Optional[str] = Field(
        default=None,
        description=(
            "NodeId of the device, used if 'path' is not defined. "
            "Must follow the format 'ns=<namespace_index>;(i|s)=<identifier>'"
        ),
        pattern=r'^ns=\d+;(i|s)=.+$'
    )
    variables: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping of local names to OPC UA variable BrowseNames."
    )
    methods: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping of local names to OPC UA method BrowseNames."
    )

    # Parsed fields (not in input YAML)
    namespace_index: Optional[int] = Field(default=None, exclude=True, allow_mutation=False)
    identifier_type: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)
    identifier: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    def validate_and_parse_node_id(cls, values: dict) -> dict:
        """
        Ensure that exactly one of 'path' or 'node_id' is provided.
        If node_id is provided, validate its format and parse namespace_index, identifier_type, and identifier.
        """
        path = values.get('path')
        node_id = values.get('node_id')

        # XOR check: exactly one must be provided
        if bool(path) == bool(node_id):
            raise ValueError("Exactly one of 'path' or 'node_id' must be specified for the device")

        if node_id:
            # Validate format
            pattern = r"^ns=\d+;(i|s)=.+$"
            if not re.match(pattern, node_id):
                raise ValueError("Invalid node_id format")

            # Parse node_id into fields
            ns_part, id_part = node_id.split(";")
            ns_index = int(ns_part.replace("ns=", ""))
            id_type, identifier = id_part.split("=", 1)
            values["namespace_index"] = ns_index
            values["identifier_type"] = id_type
            values["identifier"] = identifier

        return values


class OPCUAConnectorSchema(BaseModel):
    """
    OPC UA Connector schema wrapping the server and device configuration.

    The `type` field is a discriminator for Pydantic to select this schema.
    """
    type: Literal['opcua'] = Field(
        ...,  # no default, means required
        description="Discriminator field to identify OPC UA connector type."
    )
    server: OPCUAServerConfig = Field(..., description="OPC UA server configuration.")
    device: OPCUADeviceConfig = Field(..., description="Device configuration on the server.")

    model_config = ConfigDict(extra="forbid")
