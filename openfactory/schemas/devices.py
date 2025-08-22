"""
OpenFactory Device Schemas

This module defines Pydantic models and utility functions to parse and validate
device configuration files in OpenFactory. Device definitions include connection
details, UNS metadata, ksql table mappings, and supervisor configurations.

Key Components:
---------------
- **Device**: Defines a single device including its UUID, connector, optional supervisor,
  UNS metadata, and supported Kafka ksqlDB tables.
- **DevicesConfig**: Validates a dictionary of device entries and ensures UUID uniqueness.
- **get_devices_from_config_file**: Loads, validates, and enriches devices from a YAML file.

Features:
---------
- Supports UNS (Unified Namespace) enrichment through the `AttachUNSMixin`.
- Restricts configuration fields with `extra="forbid"` to ensure strict schema conformance.
- Includes validation logic to ensure all device UUIDs are unique.
- Enforces allowed values for `ksql_tables`.

YAML Example:
-------------
.. code-block:: yaml

    devices:
      press-1:
        uuid: "press-001"
        uns:
          location: building-a
          workcenter: press
        connector:
          type: mtconnect
          ip: 192.168.1.100
        supervisor:
          image: ghcr.io/openfactoryio/opcua-supervisor:v4.0.1
          adapter:
            ip: 192.168.0.201
            port: 4840
            environment:
            - NAMESPACE_URI=openfactory
            - BROWSE_NAME=PRESS

Usage:
------
Use `get_devices_from_config_file(path, uns_schema)` to load and validate a
device configuration YAML file, with automatic UNS enrichment.

This module is used by OpenFactory agents and deployment tools for runtime configuration.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict, ValidationError
from openfactory.config import load_yaml
from openfactory.models.user_notifications import user_notify
from openfactory.schemas.supervisors import Supervisor
from openfactory.schemas.connectors.types import Connector
from openfactory.schemas.uns import AttachUNSMixin, UNSSchema


class Device(AttachUNSMixin, BaseModel):
    """ OpenFactory Device Schema. """
    uuid: str = Field(..., description="Unique device identifier.")
    uns: Optional[Dict[str, Any]] = Field(None, description="Unified Namespace metadata.")
    connector: Connector = Field(..., description="Connector configuration for the device.")
    supervisor: Optional[Supervisor] = Field(None, description="Supervisor configuration.")
    ksql_tables: Optional[List[str]] = Field(None, description="List of Kafka ksqlDB tables.")

    model_config = ConfigDict(extra="forbid")

    @field_validator('ksql_tables', mode='before', check_fields=False)
    def validate_ksql_tables(cls, value: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validates the ksql_tables field.

        Args:
            value (List[str]): List of ksql tables.

        Returns:
            List[str]: Validated list of ksql tables.

        Raises:
            ValueError: If the provided ksql tables contain invalid entries.
        """
        allowed_values = {'device', 'producer', 'agent'}
        if value:
            invalid = set(value) - allowed_values
            if invalid:
                raise ValueError(f"Invalid entries in ksql_tables: {invalid}")
        return value


class DevicesConfig(BaseModel):
    """
    Schema for OpenFactory device configurations loaded from YAML files.

    This schema validates the structure of device configurations.

    Example usage:
        .. code-block:: python

            devices = DevicesConfig(devices=yaml_data['devices'])
            # or
            devices = DevicesConfig(**yaml_data)

    Args:
        devices (dict): Dictionary of device configurations.

    Raises:
        pydantic.ValidationError: If the input data does not conform to the expected schema.
    """
    devices: Dict[str, Device] = Field(..., description="Mapping of device names to Device schemas.")

    def validate_devices(self, uns_schema: UNSSchema) -> None:
        """
        Validates the devices configuration at the collection level.

        Checks that all device UUIDs are unique and attaches UNS metadata
        using the provided UNS schema.

        Args:
            uns_schema (UNSSchema): Schema instance used to extract and validate UNS metadata for each device.

        Raises:
            ValueError: If the devices configuration is invalid or UUID are not unique.
        """
        seen_uuids = {}
        for name, device in self.devices.items():
            uuid = device.uuid  # guaranteed by Device schema
            if uuid in seen_uuids:
                raise ValueError(
                    f"Duplicate uuid '{uuid}' found for devices '{seen_uuids[uuid]}' and '{name}'"
                )
            seen_uuids[uuid] = name

            try:
                device.attach_uns(uns_schema)
            except Exception as e:
                raise ValueError(f"Device '{name}': UNS validation failed: {e}")

    @property
    def devices_dict(self) -> Dict[str, Any]:
        """ Return plain dict of devices suitable for serialization. """
        return self.model_dump()['devices']


def get_devices_from_config_file(devices_yaml_config_file: str, uns_schema: UNSSchema) -> Optional[Dict[str, Device]]:
    """
    Load, validate, and enrich device configurations from a YAML file using UNS metadata.

    Args:
        devices_yaml_config_file (str): Path to the YAML file defining device configurations.
        uns_schema (UNSSchema): Schema instance used to extract and validate UNS metadata for each device.

    Returns:
        Optional[Dict[str, Device]]: A dictionary of validated and enriched device configurations, or `None` if validation fails.

    Note:
        In case of validation errors, user notifications will be triggered and `None` will be returned.
    """
    # load yaml description file
    cfg = load_yaml(devices_yaml_config_file)

    # validate and create devices configuration
    try:
        devices_cfg = DevicesConfig(**cfg)
        devices_cfg.validate_devices(uns_schema)
    except (ValidationError, ValueError) as err:
        user_notify.fail(f"Invalid YAML config: {err}")
        return None

    # Attach and enrich UNS for each device
    return devices_cfg.devices
