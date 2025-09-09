"""
OPC UA Connector Schemas

This module provides Pydantic models to define and validate configuration
schemas for OPC UA devices within OpenFactory.

Key Models:
-----------
- OPCUASubscriptionConfig:
  Optional subscription parameters (publishing interval, queue size,
  sampling interval). Can be defined at the server level or overridden
  per variable.

- OPCUAServerConfig:
  Configuration for the OPC UA server, including the endpoint URI,
  namespace URI, and optional default subscription parameters.

- OPCUAVariableConfig:
  Configuration for a single variable. Contains a `browse_name` plus
  optional overrides for queue size and sampling interval. If overrides
  are not provided, server-level defaults are applied automatically.

- OPCUADeviceConfig:
  Configuration for a device on the OPC UA server. Devices may be
  specified either by a hierarchical `path` or a `node_id`. Variables
  can be defined as simple strings (which are normalized to
  OPCUAVariableConfig using server defaults) or as full
  OPCUAVariableConfig objects with overrides. Methods are mapped
  by local name to OPC UA BrowseNames. If `node_id` is given, its
  namespace index, identifier type, and identifier are parsed out.

- OPCUAConnectorSchema:
  Wrapper schema that encapsulates the server and device
  configurations. During initialization, all device variables are
  normalized into OPCUAVariableConfig instances, with server-level
  subscription defaults applied where necessary.

Validation Features:
--------------------
- Ensures exactly one of `path` or `node_id` is provided for a device.
- Validates `node_id` format and parses it into namespace_index,
  identifier_type, and identifier fields.
- Normalizes all variables into OPCUAVariableConfig, applying
  server-level subscription defaults when not overridden.
- Variables and methods are optional, providing flexibility for
  different server setups.
- Forbids unknown fields to ensure strict schema conformance.

YAML Example:
-------------
.. code-block:: yaml

    type: opcua

    server:
      uri: opc.tcp://127.0.0.1:4840/freeopcua/server/
      namespace_uri: http://examples.openfactory.local/opcua
      subscription:               # optional server-level defaults
        publishing_interval: 100
        queue_size: 1
        sampling_interval: 0

    device:
      path: Sensors/TemperatureSensor_1
      variables:
        temp: Temperature         # simple string (inherits server defaults)
        hum:                      # explicit variable config (overrides defaults)
          browse_name: Humidity
          queue_size: 5
          sampling_interval: 50
      methods:
        calibrate: Calibrate

.. seealso::

   The runtime class of the class OPCUAConnectorSchema schema is :class:`openfactory.connectors.opcua.opcua_connector.OPCUAConnector`.
"""

import re
from typing import Optional, Dict, Literal, Union, Any
from pydantic import BaseModel, ConfigDict, model_validator, Field


class OPCUASubscriptionConfig(BaseModel):
    """ Optional subscription parameters for server or individual variables. """
    publishing_interval: Optional[float] = Field(
        default=None,
        description="Publishing interval in ms for subscription object (server-level default)."
    )
    queue_size: Optional[int] = Field(
        default=None,
        description="Queue size per monitored item (server or variable-level default)."
    )
    sampling_interval: Optional[float] = Field(
        default=None,
        description="Sampling interval in ms for monitored item; 0 = event-driven if supported."
    )

    model_config = ConfigDict(extra="forbid")


class OPCUAServerConfig(BaseModel):
    """ OPC UA Server configuration. """
    uri: str = Field(..., description="OPC UA server endpoint URI.")
    namespace_uri: str = Field(..., description="Namespace URI of the OPC UA server.")
    subscription: Optional[OPCUASubscriptionConfig] = Field(
        default=None,
        description="Server-level default subscription parameters."
    )

    model_config = ConfigDict(extra="forbid")


class OPCUAVariableConfig(BaseModel):
    """
    Configuration for a single OPC UA variable.

    All device variables are normalized into this model during initialization.
    If the YAML contains only a string (BrowseName), it is automatically expanded into
    an OPCUAVariableConfig using server-level subscription defaults.
    If overrides are provided here, they take precedence over server defaults.
    """
    browse_name: str = Field(..., description="OPC UA BrowseName of the variable.")
    queue_size: Optional[int] = Field(
        default=None,
        description="Override server-level queue size for this variable."
    )
    sampling_interval: Optional[float] = Field(
        default=None,
        description="Override server-level sampling interval for this variable."
    )

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
    variables: Optional[Dict[str, Union[str, OPCUAVariableConfig]]] = Field(
        default=None,
        description=(
            "Mapping of local names to variables. "
            "Accepts either simple BrowseName strings or full OPCUAVariableConfig objects. "
            "After validation, all variables are normalized into OPCUAVariableConfig."
        )
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

    During initialization, all device variables are normalized into `OPCUAVariableConfig` instances,
    inheriting server-level subscription defaults where no overrides are given.

    The `type` field is a discriminator for Pydantic to select this schema.

    .. seealso::

       The runtime class of the class OPCUAConnectorSchema schema is :class:`openfactory.connectors.opcua.opcua_connector.OPCUAConnector`.
    """
    type: Literal['opcua'] = Field(
        ...,  # no default, means required
        description="Discriminator field to identify OPC UA connector type."
    )
    server: OPCUAServerConfig = Field(..., description="OPC UA server configuration.")
    device: OPCUADeviceConfig = Field(..., description="Device configuration on the server.")

    model_config = ConfigDict(extra="forbid")

    def model_post_init(self, __context: Any) -> None:
        """ Normalize all device variables to OPCUAVariableConfig with defaults applied. """
        if not self.device.variables:
            return

        server_sub = self.server.subscription or OPCUASubscriptionConfig()
        normalized_vars = {}

        for local_name, var_cfg in self.device.variables.items():
            if isinstance(var_cfg, str):
                normalized_vars[local_name] = OPCUAVariableConfig(
                    browse_name=var_cfg,
                    queue_size=server_sub.queue_size,
                    sampling_interval=server_sub.sampling_interval,
                )
            elif isinstance(var_cfg, OPCUAVariableConfig):
                normalized_vars[local_name] = OPCUAVariableConfig(
                    browse_name=var_cfg.browse_name,
                    queue_size=(
                        var_cfg.queue_size
                        if var_cfg.queue_size is not None
                        else server_sub.queue_size
                    ),
                    sampling_interval=(
                        var_cfg.sampling_interval
                        if var_cfg.sampling_interval is not None
                        else server_sub.sampling_interval
                    ),
                )
            else:
                raise ValueError(f"Invalid variable config for {local_name}")

        self.device.variables = normalized_vars
