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
            browse_path: 0:Root/0:Objects/2:Sensors/2:Humidity
            tag: Humidity

    events:
        iolinkmaster:
            node_id: ns=6;i=43

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


class OPCUANodeConfig(BaseModel):
    """ Base class for configs that can have node_id or path to identify an OPC UA node. """
    node_id: Optional[str] = Field(
        default=None,
        description="NodeId of the object, e.g., 'ns=3;i=1050'.",
        pattern=r'^ns=\d+;(i|s)=.+$'
    )
    browse_path: Optional[str] = Field(
        default=None,
        description="Optional hierarchical BrowsePath instead of node_id. Format: 0:Root/nsIndex:Identifier/nsIndex:Identifier/…"
    )

    # Parsed fields (not in input YAML)
    namespace_index: Optional[int] = Field(default=None, exclude=True)
    identifier_type: Optional[str] = Field(default=None, exclude=True)
    identifier: Optional[str] = Field(default=None, exclude=True)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    def validate_browse_path_format(cls, values: dict) -> dict:
        path = values.get("browse_path")
        if path:
            segments = path.split('/')

            # Enforce that the path starts with the root node
            if segments[0] != "0:Root":
                raise ValueError(
                    f"BrowsePath must start with '0:Root', but got '{segments[0]}'"
                )

            for seg in segments:
                if ':' not in seg:
                    raise ValueError(
                        f"Invalid path segment '{seg}' in path '{path}'. Expected format: ns_index:Identifier"
                    )
                ns_index, identifier = seg.split(':', 1)

                # Reject leading/trailing spaces on the segment itself
                if seg != seg.strip():
                    raise ValueError(
                        f"Segment '{seg}' has leading/trailing spaces in path '{path}'"
                    )

                # Reject leading/trailing spaces around the colon
                if ns_index != ns_index.strip() or identifier != identifier.strip():
                    raise ValueError(
                        f"Spaces around colon not allowed in segment '{seg}' of path '{path}'"
                    )

                # Namespace index must be numeric
                if not ns_index.isdigit():
                    raise ValueError(
                        f"Namespace index must be numeric in segment '{seg}' of path '{path}'"
                    )

                # Identifier must be non-empty
                if not identifier:
                    raise ValueError(
                        f"Identifier must be non-empty in segment '{seg}' of path '{path}'"
                    )
        return values

    @model_validator(mode="before")
    def validate_node_id_or_browse_path(cls, values: dict) -> dict:
        node_id, browse_path = values.get("node_id"), values.get("browse_path")
        if not (node_id or browse_path):
            raise ValueError("Either 'node_id' or 'browse_path' must be provided.")
        if node_id and browse_path:
            raise ValueError("Provide only one of 'node_id' or 'browse_path', not both.")
        return values

    @model_validator(mode="before")
    def validate_and_parse_node_id(cls, values: dict) -> dict:
        node_id = values.get("node_id")
        if not node_id:
            return values  # skip if using path

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


class OPCUAVariableConfig(OPCUANodeConfig):
    """ Configuration for a single OPC UA variable. """
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


class OPCUAEventConfig(OPCUANodeConfig):
    """ Configuration for an OPC UA event source. """
    pass


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
            seen_ids_vars = set()

            for local_name, var_cfg in self.variables.items():
                # Local name uniqueness
                if local_name in normalized_vars:
                    raise ValueError(f"Duplicate variable local name: {local_name}")

                # Determine unique key for uniqueness check
                unique_key = var_cfg.node_id or var_cfg.browse_path
                if unique_key in seen_ids_vars:
                    if var_cfg.node_id:
                        raise ValueError(f"Duplicate node_id within variables: {var_cfg.node_id}")
                    else:
                        raise ValueError(f"Duplicate path within variables: {var_cfg.browse_path}")
                seen_ids_vars.add(unique_key)

                normalized_vars[local_name] = OPCUAVariableConfig(
                    node_id=var_cfg.node_id,
                    browse_path=var_cfg.browse_path,
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
                    deadband=getattr(var_cfg, "deadband", 0.0)
                )
            self.variables = normalized_vars

        if self.events:
            normalized_events = {}
            seen_ids_events = set()

            for local_name, evt_cfg in self.events.items():
                # Local name uniqueness
                if local_name in normalized_events:
                    raise ValueError(f"Duplicate event local name: {local_name}")
                # Local name conflict with variables
                if self.variables and local_name in self.variables:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both variables and events"
                    )

                # Determine unique key for uniqueness check
                unique_key = evt_cfg.node_id or evt_cfg.browse_path
                if unique_key in seen_ids_events:
                    if evt_cfg.node_id:
                        raise ValueError(f"Duplicate node_id within events: {evt_cfg.node_id}")
                    else:
                        raise ValueError(f"Duplicate path within events: {evt_cfg.browse_path}")
                seen_ids_events.add(unique_key)

                normalized_events[local_name] = evt_cfg
            self.events = normalized_events
