""" Pydantic schemas for validating OpenFactory OpenFactory Adapter, Agent, Supervisor and Device definitions. """

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator, ConfigDict
from openfactory.models.user_notifications import user_notify
from openfactory.config import load_yaml
from openfactory.schemas.uns import UNSSchema, AttachUNSMixin


class ResourcesDefinition(BaseModel):
    """ Resources Definition Schema. """
    cpus: float = None
    memory: str = None


class Resources(BaseModel):
    """ Resources Schema. """
    reservations: ResourcesDefinition = None
    limits: ResourcesDefinition = None


class Placement(BaseModel):
    """ Placement Schema. """
    constraints: List[str] = None


class Deploy(BaseModel):
    """ Deploy Schema. """
    replicas: Optional[int] = Field(default=1)
    resources: Resources = None
    placement: Placement = None


class Adapter(BaseModel):
    """ OpenFactory Adapter Schema. """
    ip: str = None
    image: str = None
    port: int
    environment: List[str] = None
    deploy: Optional[Deploy] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode='before')
    def validate_adapter(cls, values: Dict) -> Dict:
        """
        Validates the adapter configuration.

        Args:
            values (Dict): Dictionary of values to validate.

        Returns:
            Dict: Validated values.

        Raises:
            ValueError: If 'ip' or 'image' is missing or incorrectly defined.
        """
        ip = values.get('ip')
        image = values.get('image')
        # Either 'ip' or 'image' must be specified, but not both
        if (ip is None and image is None) or (ip and image):
            raise ValueError("Either 'ip' or 'image' must be specified in the adapter.")
        return values


class Agent(BaseModel):
    """ OpenFactory Agent Schema. """
    ip: str = None
    port: int
    device_xml: str = None
    adapter: Optional[Adapter] = None
    deploy: Optional[Deploy] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode='before')
    def validate_agent(cls, values: Dict) -> Dict:
        """
        Validates the agent configuration.

        Args:
            values (Dict): Dictionary of values to validate.

        Returns:
            Dict: Validated values.

        Raises:
            ValueError: If 'device_xml' or 'adapter' is missing or incorrectly defined.
        """
        ip = values.get('ip')
        adapter = values.get('adapter')
        if ip is None:
            if values.get('device_xml') is None:
                raise ValueError("'device_xml' is missing")
            if adapter is None:
                raise ValueError("'adapter' definition is missing")
        else:
            if adapter:
                raise ValueError("'adapter' can not be defined for an external agent")
            if values.get('device_xml'):
                raise ValueError("'device_xml' can not be defined for an external agent")
        return values


class Supervisor(BaseModel):
    """ OpenFactory Supervisor Schema. """
    image: str
    adapter: Adapter
    deploy: Optional[Deploy] = None


class Device(AttachUNSMixin, BaseModel):
    """ OpenFactory Device Schema. """
    uuid: str
    uns: Optional[Dict[str, Any]] = None
    agent: Agent
    supervisor: Optional[Supervisor] = None
    ksql_tables: Optional[List[str]] = None

    model_config = ConfigDict(extra="forbid")

    def __init__(self, **data: Dict):
        """
        Initialize the Device model.

        Args:
            **data (Dict): Keyword arguments to initialize the model.
        """
        super().__init__(**data)
        if self.agent.deploy is None:
            # If deploy is not provided, create a default Deploy with replicas=1
            self.agent.deploy = Deploy(replicas=1)
        elif self.agent.deploy.replicas is None:
            # If deploy is provided but replicas is missing, set replicas to 1
            self.agent.deploy.replicas = 1

    @field_validator('ksql_tables', mode='before', check_fields=False)
    def validate_ksql_tables(cls, value: List[str]) -> List[str]:
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
            invalid_entries = set(value) - allowed_values
            if invalid_entries:
                raise ValueError(f"Invalid entries in ksql-tables: {invalid_entries}")
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

    devices: Dict[str, Device]

    def validate_devices(self) -> None:
        """
        Validates the devices configuration.

        Raises:
            ValueError: If the devices configuration is invalid.
        """
        for dev_name, dev in self.devices.items():
            if dev.agent.ip:
                if dev.agent.device_xml:
                    raise ValueError("'device_xml' can not be defined for an external agent")
                if dev.agent.adapter:
                    raise ValueError("'adapter' can not be defined for an external agent")
            else:
                if not dev.agent.adapter:
                    raise ValueError(f"Device '{dev_name}': agent requires an 'adapter' block.")

    @property
    def devices_dict(self):
        """ Dictionary with all configured devices. """
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
    except ValidationError as err:
        user_notify.fail(f"Provided YAML configuration file has invalid format\n{err}")
        return None
    except ValueError as err:
        user_notify.fail(f"Provided YAML configuration file has invalid format\n{err}")
        return None

    # Attach and enrich UNS for each device
    devices = devices_cfg.devices
    for dev_name, device in devices.items():
        try:
            device.attach_uns(uns_schema)
        except Exception as e:
            user_notify.fail(f"Device '{dev_name}': UNS validation failed: {e}")
            return None

    # return plain dict form (with `uns.levels` and `uns.uns_id`)
    return {k: v.model_dump() for k, v in devices.items()}
