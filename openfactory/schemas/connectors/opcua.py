"""
OPC UA Connector Schemas

This module provides Pydantic models to define and validate configuration
schemas for OPC UA devices within OpenFactory.

Key Models:
-----------
- OPCUAServerConfig:
  Configuration for the OPC UA server, including the endpoint URI and
  optional default subscription parameters.

- OPCUASubscriptionConfig:
  Optional subscription parameters (publishing interval, queue size,
  sampling interval). Can be defined at the server level or overridden
  per variable. If not provided at the server level, default values are
  applied (publishing_interval=100 ms, queue_size=1, sampling_interval=0 ms).

- OPCUAVariableConfig:
  Configuration for a single variable. Contains `node_id` to identify the
  OPC UA node and a `tag` used by OpenFactory to label data. Optional
  overrides for queue size and sampling interval can be provided.

- OPCUAConnectorSchema:
  Wrapper schema that encapsulates the server configuration. During
  initialization, all variables are normalized into OPCUAVariableConfig
  instances, with server-level subscription defaults applied where necessary.

Validation Features:
--------------------
- Validates `node_id` format and parses it into namespace_index, identifier_type, and identifier fields.
- Normalizes all variables into OPCUAVariableConfig, applying server-level subscription defaults when not overridden. If the server subscription is omitted, default values are applied.
- Forbids unknown fields to ensure strict schema conformance.

YAML Example:
-------------
.. code-block:: yaml

    # ---------------------------------------------------------
    # Example 1: Server subscription omitted → defaults applied
    # ---------------------------------------------------------

    type: opcua

    server:
        uri: opc.tcp://127.0.0.1:4840/freeopcua/server/

        # subscription omitted → defaults will be used:
        #   publishing_interval: 100
        #   queue_size: 1
        #   sampling_interval: 0

    variables:
        temp:
            node_id: ns=3;i=1050
            tag: Temperature
            deadband: 0.1
        hum:
            node_id: ns=2;i=10
            tag: Humidity

    events:
        iolinkmaster: ns=6;i=43

    # ---------------------------------------------------------
    # Example 2: Server subscription explicitly provided
    # ---------------------------------------------------------

    type: opcua

    server:
        uri: opc.tcp://127.0.0.1:4840/freeopcua/server/

        subscription:
            publishing_interval: 200
            queue_size: 10
            sampling_interval: 25

    variables:
        temp:
            node_id: ns=3;i=1050
            tag: Temperature
        hum:
            node_id: ns=2;i=10
            tag: Humidity
            queue_size: 5          # overrides server subscription
            sampling_interval: 50  # overrides server subscription

.. seealso::

   The runtime class of the OPCUAConnectorSchema schema is :class:`openfactory.connectors.opcua.opcua_connector.OPCUAConnector`.
"""

import re
from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, ConfigDict, model_validator, Field


class OPCUASubscriptionConfig(BaseModel):
    """ Optional subscription parameters for server or individual variables. """
    publishing_interval: Optional[float] = Field(
        default=100.0,
        description="Publishing interval in ms for subscription object (server-level default)."
    )
    queue_size: Optional[int] = Field(
        default=1,
        description="Queue size per monitored item (server or variable-level default)."
    )
    sampling_interval: Optional[float] = Field(
        default=0.0,
        description="Sampling interval in ms for monitored item; 0 = event-driven if supported."
    )

    model_config = ConfigDict(extra="forbid")


class OPCUAVariableConfig(BaseModel):
    """ Configuration for a single OPC UA variable. """
    node_id: str = Field(
        ...,
        description="NodeId of the variable, e.g., 'ns=3;i=1050'. Must be unique.",
        pattern=r'^ns=\d+;(i|s)=.+$'
    )
    tag: str = Field(..., description="Tag used by OpenFactory to label the variable's data.")
    queue_size: Optional[int] = Field(
        default=None,
        description="Override server-level queue size for this variable."
    )
    sampling_interval: Optional[float] = Field(
        default=None,
        description="Override server-level sampling interval for this variable."
    )
    deadband: float = Field(
        default=0.0,
        description="Deadband for the variable; values changes smaller than this are ignored."
    )

    # Parsed fields (not in input YAML)
    namespace_index: Optional[int] = Field(default=None, exclude=True, allow_mutation=False)
    identifier_type: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)
    identifier: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    def validate_and_parse_node_id(cls, values: dict) -> dict:
        """ Validate node_id format and parse it into namespace_index, identifier_type, and identifier. """
        node_id = values.get("node_id")
        if not node_id:
            raise ValueError("node_id must be provided for each variable")

        pattern = r"^ns=\d+;(i|s)=.+$"
        if not re.match(pattern, node_id):
            raise ValueError(f"Invalid node_id format: {node_id}")

        ns_part, id_part = node_id.split(";")
        ns_index = int(ns_part.replace("ns=", ""))
        id_type, identifier = id_part.split("=", 1)
        values["namespace_index"] = ns_index
        values["identifier_type"] = id_type
        values["identifier"] = identifier
        return values


class OPCUAEventConfig(BaseModel):
    """ Configuration for an OPC UA event source. """
    node_id: str = Field(
        ...,
        description="NodeId of the event source, e.g., 'ns=6;i=10'.",
        pattern=r'^ns=\d+;(i|s)=.+$'
    )

    # Parsed fields (not in input YAML)
    namespace_index: Optional[int] = Field(default=None, exclude=True, allow_mutation=False)
    identifier_type: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)
    identifier: Optional[str] = Field(default=None, exclude=True, allow_mutation=False)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    def validate_and_parse_node_id(cls, values: dict) -> dict:
        node_id = values.get("node_id")
        if not node_id:
            raise ValueError("node_id must be provided for each event")

        pattern = r"^ns=\d+;(i|s)=.+$"
        if not re.match(pattern, node_id):
            raise ValueError(f"Invalid node_id format: {node_id}")

        ns_part, id_part = node_id.split(";")
        ns_index = int(ns_part.replace("ns=", ""))
        id_type, identifier = id_part.split("=", 1)

        values["namespace_index"] = ns_index
        values["identifier_type"] = id_type
        values["identifier"] = identifier

        return values


class OPCUAServerConfig(BaseModel):
    """ OPC UA Server configuration with variables. """
    uri: str = Field(..., description="OPC UA server endpoint URI.")
    subscription: Optional[OPCUASubscriptionConfig] = Field(
        default_factory=OPCUASubscriptionConfig,
        description="Server-level default subscription parameters."
    )

    model_config = ConfigDict(extra="forbid")


class OPCUAConnectorSchema(BaseModel):
    """
    OPC UA Connector schema wrapping the server configuration.

    During initialization, all variables are normalized into `OPCUAVariableConfig` instances,
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
    variables: Optional[Dict[str, OPCUAVariableConfig]] = Field(
        default=None,
        description="Mapping of local variable names to their configurations."
    )
    events: Optional[Dict[str, OPCUAEventConfig]] = Field(
        default=None,
        description="Mapping of named event sources."
    )

    model_config = ConfigDict(extra="forbid")

    def model_post_init(self, __context: Any) -> None:
        """ Normalize all server variables with subscription defaults and enforce unique keys. """

        if self.variables:
            server_sub = self.server.subscription or OPCUASubscriptionConfig()
            normalized_vars = {}
            seen_node_ids_vars = set()

            for local_name, var_cfg in self.variables.items():
                # Local name uniqueness
                if local_name in normalized_vars:
                    raise ValueError(f"Duplicate variable local name: {local_name}")

                # Node_id uniqueness within variables
                if var_cfg.node_id in seen_node_ids_vars:
                    raise ValueError(f"Duplicate node_id within variables: {var_cfg.node_id}")
                seen_node_ids_vars.add(var_cfg.node_id)

                normalized_vars[local_name] = OPCUAVariableConfig(
                    node_id=var_cfg.node_id,
                    tag=var_cfg.tag,
                    queue_size=(
                        var_cfg.queue_size
                        if var_cfg.queue_size is not None else
                        server_sub.queue_size
                    ),
                    sampling_interval=(
                        var_cfg.sampling_interval
                        if var_cfg.sampling_interval is not None else
                        server_sub.sampling_interval
                    ),
                deadband=var_cfg.deadband if getattr(var_cfg, "deadband", None) is not None else 0.0,
                )
            self.variables = normalized_vars

        if self.events:
            normalized_events = {}
            seen_node_ids_events = set()

            for local_name, evt_cfg in self.events.items():
                # Local name uniqueness
                if local_name in normalized_events:
                    raise ValueError(f"Duplicate event local name: {local_name}")
                # Local name conflict with variables
                if self.variables and local_name in self.variables:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both variables and events"
                    )
                # Node_id uniqueness within events
                if evt_cfg.node_id in seen_node_ids_events:
                    raise ValueError(f"Duplicate node_id within events: {evt_cfg.node_id}")
                seen_node_ids_events.add(evt_cfg.node_id)

                normalized_events[local_name] = evt_cfg
            self.events = normalized_events
