"""
OpenFactory Device schemas defining the device, supervisor, and device configurations.

This module uses the generic Connector union for device connectors (agents),
supports UNS enrichment, deployment defaults, and config validation.
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

    def validate_devices(self) -> None:
        """
        Validates the devices configuration at the collection level.

        Checks that all device UUIDs are unique.

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

    @property
    def devices_dict(self) -> Dict[str, Any]:
        """ Return plain dict of devices suitable for serialization. """
        return self.model_dump()['devices']


def get_devices_from_config_file(devices_yaml_config_file: str, uns_schema: UNSSchema) -> Optional[Dict[str, Device]]:
    """
    Load, validate, and enrich device configurations from a YAML file using UNS metadata.

    This function reads a YAML file containing device definitions, validates its content
    using the :class:`DevicesConfig` Pydantic model, and augments each validated device entry
    with Unified Namespace (UNS) metadata derived from the provided schema.

    Args:
        devices_yaml_config_file (str): Path to the YAML file defining device configurations.
        uns_schema (UNSSchema): Schema instance used to extract and validate UNS metadata
                                for each device.

    Returns:
        Optional[Dict[str, Device]]: A dictionary of validated and enriched device configurations,
                                     or `None` if validation fails.

    Note:
        In case of validation errors, user notifications will be triggered and `None` will be returned.
    """
    # load yaml description file
    cfg = load_yaml(devices_yaml_config_file)

    # validate and create devices configuration
    try:
        devices_cfg = DevicesConfig(**cfg)
        devices_cfg.validate_devices()
    except (ValidationError, ValueError) as err:
        user_notify.fail(f"Invalid YAML config: {err}")
        return None

    # Attach and enrich UNS for each device
    devices = devices_cfg.devices
    for name, device in devices.items():
        try:
            device.attach_uns(uns_schema)
        except Exception as e:
            user_notify.fail(f"Device '{name}': UNS validation failed: {e}")
            return None

    return {k: v.model_dump() for k, v in devices.items()}
