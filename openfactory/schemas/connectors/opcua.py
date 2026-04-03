"""
OPC UA Connector Schemas

This module provides Pydantic models to define and validate configuration
schemas for OPC UA devices within OpenFactory.

Key Models:
-----------
- :class:`OPCUAServerConfig`:

  Configuration for the OPC UA server, including the endpoint URI and
  optional default subscription parameters.

- :class:`OPCUASubscriptionConfig`:

  Optional subscription parameters (``publishing_interval``, ``queue_size``,
  ``sampling_interval``). Can be defined at the server level or overridden
  per variable. If not provided at the server level, default values are
  applied (``publishing_interval=100`` ms, ``queue_size=1``, ``sampling_interval=0`` ms).

- :class:`OPCUAConstantConfig`:

  Configuration for a constant OPC UA value. Contains ``node_id`` or
  ``browse_path`` to identify the OPC UA node and a ``tag`` used by OpenFactory.
  Constants are read once at deployment time and are not subscribed.

- :class:`OPCUAVariableConfig`:

  Configuration for a single variable. Contains ``node_id`` or ``browse_path``
  to identify the OPC UA node and a ``tag`` used by OpenFactory to label data.
  Variables may also declare an ``access_level`` (``ro`` or ``rw``) describing
  how OpenFactory is allowed to interact with the node.
  Optional overrides for queue size and sampling interval can be provided.

- :class:`OPCUAEventConfig`:

  Configuration for an event source. Contains ``node_id`` or ``browse_path``
  identifying the OPC UA node that emits events. Events do not use tags
  or subscription overrides.

- :class:`OPCUAMethodConfig`:

  Configuration for a method source. Each entry references an OPC UA node
  (``node_id`` or ``browse_path``) from which all available OPC UA methods
  will be discovered at runtime. Methods are not individually declared in
  the schema; instead, the node acts as a method container.

- :class:`OPCUAConnectorSchema`:

  Wrapper schema that encapsulates the server configuration along with
  constants, variables, events, and method sources. During initialization:

  - variables are normalized into :class:`OPCUAVariableConfig` with server-level defaults
  - constants, events, and methods are validated and normalized
  - cross-section conflicts are checked

Validation Features:
--------------------
- Validates ``node_id`` format and parses it into ``namespace_index``,
  ``identifier_type``, and ``identifier`` fields.
- Enforces that exactly one of ``node_id`` or ``browse_path`` is provided
  for constants, variables, events, and methods.
- Normalizes variables into :class:`OPCUAVariableConfig`, applying server-level
  subscription defaults when not overridden.
- Ensures uniqueness of ``node_id`` and ``browse_path`` within each section
  (constants, variables, events, methods).
- Prevents local name conflicts across sections (constants, variables,
  events, methods).
- Allows reuse of the same OPC UA node across sections (e.g. a variable
  and an event can reference the same node).
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

    constants:
        device_model:
            node_id: ns=2;i=1001
            tag: Model

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

    methods:
        tempSensor:
            browse_path: 0:Root/0:Objects/2:Sensors/2:TemperatureSensor
        humiSensor:
            node_id: ns=5;i=12

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

    constants:
        serial:
            browse_path: 0:Root/0:Objects/2:Device/2:SerialNumber
            tag: SerialNumber

    variables:
        temp:
            node_id: ns=3;i=1050
            tag: Temperature
            queue_size: 5          # overrides server subscription
            sampling_interval: 50  # overrides server subscription
        setpoint:
            node_id: ns=2;i=10
            tag: Setpoint
            access_level: rw       # read-write variable

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
    """
    Base configuration for OPC UA nodes identified by either a ``node_id``
    or a ``browse_path``.

    Exactly one of ``node_id`` or ``browse_path`` must be provided.
    """
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

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Serialize the model while excluding fields with ``None`` values.

        Args:
            *args: Positional arguments forwarded to ``BaseModel.model_dump``.
            **kwargs: Keyword arguments forwarded to ``BaseModel.model_dump``.

        Returns:
            Serialized model dictionary with ``None`` fields excluded.
        """
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(*args, **kwargs)

    @model_validator(mode="before")
    def validate_browse_path_format(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate the format of ``browse_path``.

        Ensures:
            - Path starts with ``"0:Root"``.
            - Each segment follows ``ns_index:identifier`` format.
            - No leading/trailing whitespace.
            - Namespace index is numeric.
            - Identifier is non-empty.

        Args:
            values: Raw input values.

        Returns:
            Validated values.

        Raises:
            ValueError: If the browse path format is invalid.
        """
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
    def validate_node_id_or_browse_path(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure exactly one of ``node_id`` or ``browse_path`` is provided.

        Args:
            values: Raw input values.

        Returns:
            Validated values.

        Raises:
            ValueError: If neither or both fields are provided.
        """
        node_id, browse_path = values.get("node_id"), values.get("browse_path")
        if not (node_id or browse_path):
            raise ValueError("Either 'node_id' or 'browse_path' must be provided.")
        if node_id and browse_path:
            raise ValueError("Provide only one of 'node_id' or 'browse_path', not both.")
        return values

    @model_validator(mode="before")
    def validate_and_parse_node_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and parse ``node_id`` into structured components.

        Extracts:
            - ``namespace_index``
            - ``identifier_type``
            - ``identifier``

        Args:
            values: Raw input values.

        Returns:
            Updated values including parsed fields.

        Raises:
            ValueError: If ``node_id`` format is invalid.
        """
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


class OPCUAConstantConfig(OPCUANodeConfig):
    """ Configuration for a constant OPC UA value (read once at deployment). """

    tag: str = Field(..., description="Tag used by OpenFactory to label the variable's data.")


AccessLevel = Literal["ro", "rw"]


class OPCUAVariableConfig(OPCUANodeConfig):
    """ Configuration for a single OPC UA variable. """
    tag: str = Field(..., description="Tag used by OpenFactory to label the variable's data.")
    access_level: AccessLevel = Field(
        default="ro",
        description="Access level declared by the connector: 'ro' = read-only, 'rw' = read-write."
    )
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


class OPCUAMethodConfig(OPCUANodeConfig):
    """ Configuration for an OPC UA method source. """
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

    During initialization, all variables are normalized into :class:`OPCUAVariableConfig` instances,
    inheriting server-level subscription defaults where no overrides are given.

    The ``type`` field is a discriminator for Pydantic to select this schema.

    .. seealso::

       The runtime class of the :class:`OPCUAConnectorSchema` schema is :class:`openfactory.connectors.opcua.opcua_connector.OPCUAConnector`.
    """
    type: Literal['opcua'] = Field(
        ...,  # no default, means required
        description="Discriminator field to identify OPC UA connector type."
    )
    server: OPCUAServerConfig = Field(..., description="OPC UA server configuration.")

    constants: Optional[Dict[str, OPCUAConstantConfig]] = Field(
        default=None,
        description="Mapping of named constants read once from OPC UA at deployment."
    )

    variables: Optional[Dict[str, OPCUAVariableConfig]] = Field(
        default=None,
        description="Mapping of local variable names to their configurations."
    )

    events: Optional[Dict[str, OPCUAEventConfig]] = Field(
        default=None,
        description="Mapping of named event sources."
    )

    methods: Optional[Dict[str, OPCUAMethodConfig]] = Field(
        default=None,
        description="Mapping of named OPC UA nodes from which methods will be discovered."
    )

    model_config = ConfigDict(extra="forbid")

    def model_post_init(self, __context: Any) -> None:
        """
        Normalize configuration sections and enforce uniqueness constraints.

        Ensures:
            - Unique local names within each section.
            - No cross-section name conflicts.
            - No duplicate node identifiers (node_id or browse_path).
            - Variables inherit server-level subscription defaults.

        Args:
            __context: Pydantic initialization context.

        Raises:
            ValueError: If uniqueness or validation constraints are violated.
        """

        # IMPORTANT: normalization order matters for cross-section validation
        # Order: variables → constants → events → methods

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
                    access_level=var_cfg.access_level,
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

        if self.constants:
            normalized_constants = {}
            seen_ids_constants = set()

            for local_name, const_cfg in self.constants.items():
                # Local name uniqueness
                if local_name in normalized_constants:
                    raise ValueError(f"Duplicate constant local name: {local_name}")

                # Name conflicts with variables
                if self.variables and local_name in self.variables:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both variables and constants"
                    )

                # Name conflicts with events
                if self.events and local_name in self.events:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both constants and events"
                    )

                # Name conflicts with methods
                if self.methods and local_name in self.methods:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both constants and methods"
                    )

                # Determine unique key for uniqueness check
                unique_key = const_cfg.node_id or const_cfg.browse_path
                if unique_key in seen_ids_constants:
                    if const_cfg.node_id:
                        raise ValueError(f"Duplicate node_id within constants: {const_cfg.node_id}")
                    else:
                        raise ValueError(f"Duplicate path within constants: {const_cfg.browse_path}")
                seen_ids_constants.add(unique_key)

                normalized_constants[local_name] = const_cfg

            self.constants = normalized_constants

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
                # Local name conflict with constants
                if self.constants and local_name in self.constants:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both events and constants"
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

        if self.methods:
            normalized_methods = {}
            seen_ids_methods = set()

            for local_name, m_cfg in self.methods.items():
                # Local name uniqueness
                if local_name in normalized_methods:
                    raise ValueError(f"Duplicate method source local name: {local_name}")

                # Name conflicts with constants, variables or events
                if self.constants and local_name in self.constants:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both methods and constants"
                    )
                if self.variables and local_name in self.variables:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both variables and methods"
                    )
                if self.events and local_name in self.events:
                    raise ValueError(
                        f"Local name conflict: '{local_name}' exists in both events and methods"
                    )

                # Unique node check within methods
                unique_key = m_cfg.node_id or m_cfg.browse_path
                if unique_key in seen_ids_methods:
                    if m_cfg.node_id:
                        raise ValueError(f"Duplicate node_id within methods: {m_cfg.node_id}")
                    else:
                        raise ValueError(f"Duplicate path within methods: {m_cfg.browse_path}")
                seen_ids_methods.add(unique_key)

                normalized_methods[local_name] = m_cfg

            self.methods = normalized_methods
