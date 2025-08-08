"""
OpenFactory Application Schemas

This module provides Pydantic models to validate and represent OpenFactory application
definitions and configurations. It supports loading from YAML configuration files,
validating the structure and content, and enriching each application with Unified Namespace (UNS)
metadata for enhanced semantic context.

Validation Features:
--------------------
- Strict schema enforcement with `extra="forbid"` to prevent unexpected fields.
- Support for UNS metadata attachment and validation.
- Utilities to load and validate application configs from YAML files with user-friendly error handling.
- Provides a structured way to represent Docker image info, environment variables, and app-specific metadata.
"""

from pydantic import BaseModel, Field, ValidationError, ConfigDict
from typing import List, Dict, Optional, Any
from openfactory.config import load_yaml
from openfactory.models.user_notifications import user_notify
from openfactory.schemas.uns import UNSSchema, AttachUNSMixin


class OpenFactoryApp(AttachUNSMixin, BaseModel):
    """ OpenFactory Application Schema."""
    uuid: str = Field(..., description="Unique identifier for the app")
    uns: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Unified Namespace (UNS) configuration for the app"
    )
    image: str = Field(..., description="Docker image for the app")
    environment: Optional[List[str]] = Field(
        default=None, description="List of environment variables"
    )

    model_config = ConfigDict(extra="forbid")


class OpenFactoryAppsConfig(BaseModel):
    """
    Schema for OpenFactory application configurations loaded from YAML files.

    This schema validates the structure of application configuration data.

    Example usage:
        .. code-block:: python

            apps_config = OpenFactoryAppsConfig(apps=yaml_data['apps'])
            # or
            apps_config = OpenFactoryAppsConfig(**yaml_data)

    Args:
        apps (dict): Dictionary containing application configurations.

    Raises:
        pydantic.ValidationError: If the input data does not conform to the expected schema.
    """

    apps: Dict[str, OpenFactoryApp] = Field(
        ..., description="Dictionary of OpenFactory applications"
    )

    @property
    def apps_dict(self):
        """ Dictionary with all configured OpenFactory applications. """
        return self.model_dump()['apps']


def get_apps_from_config_file(apps_yaml_config_file: str, uns_schema: UNSSchema) -> Optional[Dict[str, OpenFactoryApp]]:
    """
    Load, validate, and enrich OpenFactory application configurations from a YAML file using UNS metadata.

    This function reads a YAML file containing OpenFactory application definitions, validates its content
    using the :class:`OpenFactoryAppsConfig` Pydantic model, and augments each validated application entry
    with Unified Namespace (UNS) metadata derived from the provided schema.

    Args:
        apps_yaml_config_file (str): Path to the YAML file defining application configurations.
        uns_schema (UNSSchema): Schema instance used to extract and validate UNS metadata
                                for each application.

    Returns:
        Optional[Dict[str, OpenFactoryApp]]: A dictionary of validated and enriched application configurations,
                                             or `None` if validation fails.

    Note:
        In case of validation errors, user notifications will be triggered and `None` will be returned.
    """
    # load yaml description file
    cfg = load_yaml(apps_yaml_config_file)

    # validate and create apps configuration
    try:
        apps_cfg = OpenFactoryAppsConfig(**cfg)
    except ValidationError as err:
        user_notify.fail(f"Provided YAML configuration file has invalid format\n{err}")
        return None
    except ValueError as err:
        user_notify.fail(f"Provided YAML configuration file has invalid format\n{err}")
        return None

    # Attach and enrich UNS for each app
    apps = apps_cfg.apps
    for app_name, app in apps.items():
        try:
            app.attach_uns(uns_schema)
        except Exception as e:
            user_notify.fail(f"App '{app_name}': UNS validation failed: {e}")
            return None

    # return plain dict form (with `uns.levels` and `uns.uns_id`)
    return {k: v.model_dump() for k, v in apps.items()}
