"""
MTConnect-specific Pydantic schemas used to configure adapters and agents.

This module defines validation logic for MTConnect data source connectors in OpenFactory,
supporting both embedded and external agents/adapters. It ensures that configurations
follow the expected rules, such as mutual exclusivity of `ip` and `image` in adapters,
and valid combinations of `device_xml`, `adapter`, and `ip` in agents.
"""

from typing import Dict, List, Optional, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from openfactory.schemas.common import Deploy


class Adapter(BaseModel):
    """ MTConnect Adapter Schema. """
    ip: Optional[str] = Field(
        default=None,
        description="IP address of the adapter. Must be specified if `image` is not."
    )
    image: Optional[str] = Field(
        default=None,
        description="Container image name for the adapter. Must be specified if `ip` is not."
    )
    port: int = Field(
        ...,
        description="Port number the adapter listens on."
    )
    environment: Optional[List[str]] = Field(
        default=None,
        description="List of environment variables to set for the adapter container."
    )
    deploy: Optional[Deploy] = Field(
        default=None,
        description="Deployment configuration such as resource limits and placement."
    )

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

        Note:
            Either 'ip' or 'image' must be specified, but not both.
        """
        ip = values.get('ip')
        image = values.get('image')
        # Either 'ip' or 'image' must be specified, but not both
        if (ip is None and image is None) or (ip and image):
            raise ValueError("Either 'ip' or 'image' must be specified in the adapter.")
        return values

    @model_validator(mode='after')
    def set_deploy_defaults(cls, values: "Adapter") -> "Adapter":
        """
        Ensures deploy is set and deploy.replicas has default value 1 if missing.

        Args:
            values (Adapter): Adapter instance after initial validation.

        Returns:
            Adapter: Adapter instance with defaults set.
        """
        if values.deploy is None:
            values.deploy = Deploy(replicas=1)
        elif values.deploy.replicas is None:
            values.deploy.replicas = 1
        return values


class Agent(BaseModel):
    """ MTConnect Agent Schema. """
    ip: Optional[str] = Field(
        default=None,
        description="IP address of an external MTConnect agent. If set, 'adapter' and 'device_xml' must be omitted."
    )
    port: int = Field(
        ...,
        description="Port number the agent listens on."
    )
    device_xml: Optional[str] = Field(
        default=None,
        description="Path to the device XML file, required if 'ip' is not set."
    )
    adapter: Optional[Adapter] = Field(
        default=None,
        description="Embedded adapter configuration, required if 'ip' is not set."
    )
    deploy: Optional[Deploy] = Field(
        default=None,
        description="Deployment configuration such as resource limits and placement."
    )

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode='after')
    def set_deploy_defaults(cls, values: "Agent") -> "Agent":
        """
        Sets default deployment configuration values after model initialization.

        Args:
            values (Agent): The Agent instance after initial validation.

        Returns:
            Agent: The Agent instance with default values set for deploy if missing.

        Notes:
            - If 'deploy' is None, it will be set to a Deploy instance with replicas=1.
            - If 'deploy' exists but 'replicas' is None, replicas will be set to 1.
        """
        if values.deploy is None:
            values.deploy = Deploy(replicas=1)
        elif values.deploy.replicas is None:
            values.deploy.replicas = 1
        return values

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

        Note:
            - If 'ip' is None, both 'device_xml' and 'adapter' must be defined.
            - If 'ip' is set, 'device_xml' and 'adapter' must NOT be defined.
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


class MTConnectConnector(BaseModel):
    """
    MTConnect Connector schema that wraps the Agent configuration.

    The `type` field is a discriminator for Pydantic to select this schema.
    """

    type: Literal['mtconnect'] = Field(
        ...,  # no default, means required
        description="Discriminator field to identify MTConnect connector type."
    )
    agent: Agent = Field(
        ...,
        description="Configuration of the MTConnect agent."
    )
