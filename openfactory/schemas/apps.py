"""
This module defines Pydantic models and utility functions to parse, validate,
and enrich application configuration files in OpenFactory. Application definitions
include Docker image info, environment variables, optional UNS metadata, storage
backends, and container networks.

This module is used by OpenFactory deployment tools and runtime components to
ensure application configurations are consistent, valid, and semantically enriched.

Key Components
--------------
- ``OpenFactoryApp``: Defines a single application including its UUID, Docker image,
  environment variables, UNS metadata, storage backend, and container networks.
- ``OpenFactoryAppsConfig``: Validates a dictionary of application entries and ensures
  correct schema structure.
- ``get_apps_from_config_file``: Loads, validates, and enriches applications from a
  YAML file with UNS metadata.

Features
--------
- Supports UNS (Unified Namespace) enrichment through the ``AttachUNSMixin``.
- Restricts configuration fields with `extra="forbid"` to ensure strict schema conformance.
- Supports storage backends, including:

  - ``LocalBackend``: Bind-mount a local host directory into containers (for development).
  - ``NFSBackend``: Mount an NFS share into containers with configurable mount options.

- Supports connecting containers to multiple Docker networks.
- Provides utilities to load application configs from YAML with user-friendly
  error handling and notifications.
- Ensures validated and enriched applications are returned as plain dictionaries.

Usage
-----
Use ``get_apps_from_config_file(path, uns_schema)`` to load and validate an application
configuration YAML file, with automatic UNS enrichment.

.. admonition:: YAML Example

  .. code-block:: yaml

      apps:
        scheduler:
          uuid: "app-scheduler"
          image: ghcr.io/openfactoryio/scheduler:v1.0.0

          uns:
            location: building-a
            workcenter: scheduler

          environment:
           - ENV=production

          storage:
            type: nfs
            server: deskfab.openfactory.com
            remote_path: /nfs/deskfab
            mount_point: /mnt
            mount_options:
              - ro

          networks:
            - factory-net
            - monitoring-net

Note:
    - **Networks**: All network names must exist in Docker before deployment.
    - **UNS metadata**: Must match the `UNSSchema` used in the environment for semantic consistency.
    - **Storage backends**: Can be extended to support new types if needed.
    - Use the `apps_dict` property to access validated apps in runtime code.

.. seealso::

   The runtime class of OpenFactory Apps is :class:`openfactory.apps.ofaapp.OpenFactoryApp`.
"""

from pydantic import BaseModel, Field, ValidationError, ConfigDict, field_validator
from typing import List, Dict, Optional, Any
from openfactory.config import load_yaml
from openfactory.models.user_notifications import user_notify
from openfactory.schemas.uns import UNSSchema, AttachUNSMixin
from openfactory.schemas.filelayer.types import StorageBackend


class OpenFactoryAppSchema(AttachUNSMixin, BaseModel):
    """ OpenFactory Application Schema. """
    uuid: str = Field(..., description="Unique identifier for the app")

    uns: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Unified Namespace (UNS) configuration for the app"
    )

    image: str = Field(..., description="Docker image for the app")

    environment: Optional[List[str]] = Field(
        default=None, description="List of environment variables"
    )

    storage: Optional[StorageBackend] = Field(
        default=None,
        description="Optional storage backend for the application"
    )

    networks: Optional[List[str]] = Field(
        default=None,
        description="Optional list of Docker networks the App container should connect to"
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("networks")
    @classmethod
    def validate_networks(cls, v):
        if v:
            for name in v:
                if not name.strip():
                    raise ValueError("Network names must not be empty")
        return v


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

    apps: Dict[str, OpenFactoryAppSchema] = Field(
        ..., description="Dictionary of OpenFactory applications"
    )

    @property
    def apps_dict(self):
        """ Dictionary with all configured OpenFactory applications. """
        return self.model_dump()['apps']


def get_apps_from_config_file(apps_yaml_config_file: str, uns_schema: UNSSchema) -> Optional[Dict[str, OpenFactoryAppSchema]]:
    """
    Load, validate, and enrich OpenFactory application configurations from a YAML file using UNS metadata.

    This function reads a YAML file containing OpenFactory application definitions, validates its content
    using the :class:`OpenFactoryAppsConfig` Pydantic model, and augments each validated application entry
    with Unified Namespace (UNS) metadata derived from the provided schema.

    Args:
        apps_yaml_config_file (str): Path to the YAML file defining application configurations.
        uns_schema (UNSSchema): Schema instance used to extract and validate UNS metadata for each application.

    Returns:
        Optional[Dict[str, OpenFactoryApp]]: A dictionary of validated and enriched application configurations, or `None` if validation fails.

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

    return apps
