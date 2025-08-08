"""
OpenFactory Supervisor Schemas

This module defines Pydantic models for configuring OpenFactory supervisors and their
associated adapters. It provides structured schemas with validation logic to ensure
correct and consistent configuration of supervisor components within OpenFactory devices.

Validation Features:
--------------------
- Strict enforcement of mutually exclusive 'ip' and 'image' fields in adapter configurations.
- Support for specifying deployment options including resource limits and placement constraints.
- Clear separation of supervisor container settings and adapter-specific parameters.
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator
from openfactory.schemas.common import Deploy


class SupervisorAdapter(BaseModel):
    """
    Supervisor Adapter Schema.

    Represents the adapter configuration used by the OpenFactory supervisor.
    Exactly one of `ip` or `image` must be provided (mutually exclusive).
    """
    ip: Optional[str] = Field(
        default=None,
        description="IP address of the adapter. Must be set if 'image' is not."
    )
    image: Optional[str] = Field(
        default=None,
        description="Container image of the adapter. Must be set if 'ip' is not."
    )
    port: int = Field(
        ...,
        description="Port number the adapter listens on."
    )
    environment: Optional[List[str]] = Field(
        default=None,
        description="List of environment variables to set in the adapter container."
    )
    deploy: Optional[Deploy] = Field(
        default=None,
        description="Deployment configuration including resources and placement."
    )

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode='before')
    def validate_adapter(cls, values: Dict) -> Dict:
        """
        Validates that either 'ip' or 'image' is specified, but not both.

        Args:
            values (Dict): Values to validate.

        Returns:
            Dict: Validated values.

        Raises:
            ValueError: If both or neither of 'ip' and 'image' are specified.
        """
        if not isinstance(values, dict):
            raise TypeError("Adapter configuration must be a dictionary.")

        ip = values.get('ip')
        image = values.get('image')
        if (ip is None and image is None) or (ip and image):
            raise ValueError("Either 'ip' or 'image' must be specified in the adapter, but not both.")
        return values


class Supervisor(BaseModel):
    """
    OpenFactory Supervisor Schema.

    Defines the supervisor container including image, adapter configuration,
    and optional deployment parameters.
    """
    image: str = Field(
        ...,
        description="Supervisor container image name."
    )
    adapter: SupervisorAdapter = Field(
        ...,
        description="Configuration for the supervisor's adapter."
    )
    deploy: Optional[Deploy] = Field(
        default=None,
        description="Deployment options such as resource constraints and placement."
    )

    model_config = ConfigDict(extra="forbid")
