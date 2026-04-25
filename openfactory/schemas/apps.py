"""
This module defines Pydantic models and utility functions to parse, validate,
and enrich application configuration files in OpenFactory. Application definitions
include Docker image info, environment variables, optional UNS metadata, storage
backends, and container networks.

This module is used by OpenFactory deployment tools and runtime components to
ensure application configurations are consistent, valid, and semantically enriched.

Key Components
--------------
- :class:`OpenFactoryAppSchema`: Defines a single application including its UUID, Docker image,
  environment variables, UNS metadata, storage backend, and container networks.
- :class:`OpenFactoryAppsConfig`: Validates a dictionary of application entries and ensures
  correct schema structure.
- :meth:`get_apps_from_config_file`: Loads, validates, and enriches applications from a
  YAML file with UNS metadata.

Features
--------
- Supports UNS (Unified Namespace) enrichment through the ``AttachUNSMixin``.
- Restricts configuration fields with `extra="forbid"` to ensure strict schema conformance.
- Supports storage backends, including:

  - ``LocalBackend``: Bind-mount a local host directory into containers (for development).
  - ``NFSBackend``: Mount an NFS share into containers with configurable mount options.

- Supports connecting containers to multiple Docker networks.
- Supports application exposure via Traefik using host-based routing.
  Routing configuration allows defining an internal port and optional hostname.
  Canonical and optional alias hostnames are generated automatically.
- Provides utilities to load application configs from YAML with user-friendly
  error handling and notifications.
- Ensures validated and enriched applications are returned as plain dictionaries.

Usage
-----
Use :meth:`get_apps_from_config_file` to load and validate an application
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

          routing:
            expose: true
            port: 8000
            hostname: dashboard

          networks:
            - factory-net
            - monitoring-net

          deploy:
            replicas: 2

            resources:
              reservations:
                cpus: 0.5
                memory: "512Mi"
              limits:
                cpus: 1.0
                memory: "1Gi"

            placement:
                constraints:
                - node.labels.zone == building-a

Note:
    - **Networks**: All network names must exist in Docker before deployment.
    - **UNS metadata**: Must match the ``UNSSchema`` used in the environment for semantic consistency.
    - **Storage backends**: Will be extended in future to support more types.
    - **Routing**: Hostnames are normalized and validated to comply with DNS constraints.
    - Use the ``apps_dict`` property to access validated apps in runtime code.

.. seealso::

   The runtime class of OpenFactory Apps is :class:`openfactory.apps.ofaapp.OpenFactoryApp`.
"""

import re
import openfactory.config as Config
from pydantic import BaseModel, Field, ValidationError, ConfigDict, field_validator
from typing import List, Dict, Optional, Any
from openfactory.config import load_yaml
from openfactory.models.user_notifications import user_notify
from openfactory.schemas.uns import UNSSchema, AttachUNSMixin
from openfactory.schemas.filelayer.types import StorageBackend
from openfactory.schemas.common import Deploy


class RoutingError(ValueError):
    """ Raised when routing configuration is invalid. """


def normalize_name(value: str) -> str:
    """
    Normalize a string to be DNS-compatible.

    This function transforms an arbitrary string into a valid DNS label by:
    - converting to lowercase
    - replacing underscores with hyphens
    - replacing invalid characters with hyphens
    - collapsing consecutive hyphens
    - stripping leading and trailing hyphens

    Args:
        value (str): Input string to normalize.

    Returns:
        str: Normalized string suitable for use in DNS labels.

    Note:
        This function does not enforce DNS length constraints (e.g. 63 characters per label).
        Length validation must be handled by the caller.
    """
    value = value.lower()
    value = value.replace("_", "-")
    value = re.sub(r"[^a-z0-9-]", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


class Routing(BaseModel):
    """
    Routing configuration for exposing an OpenFactory application via Traefik.

    This schema defines whether an application should be exposed externally,
    which internal port should be used, and optionally a desired hostname.

    During enrichment, canonical and optional alias hostnames are generated
    based on the application name, UUID, and base domain.

    Raises:
        RoutingError: If generated hostname labels exceed DNS limits.

    .. admonition:: YAML example

        .. code-block:: yaml

            routing:
                expose: true
                port: 8000
                hostname: dashboard
    """

    expose: bool = Field(
        default=False,
        description="Whether the application should be exposed via Traefik"
    )

    port: Optional[int] = Field(
        default=None,
        description="Internal container port to expose via Traefik"
    )

    hostname: Optional[str] = Field(
        default=None,
        description="Optional desired hostname (alias) for the application"
    )

    # --- normalized fields (computed) ---
    canonical_hostname: Optional[str] = None
    alias_hostname: Optional[str] = None

    def build_hostnames(self, app_name: str, app_uuid: str, base_domain: str) -> None:
        """
        Generate canonical and optional alias hostnames for the application.

        The canonical hostname is always generated and guarantees uniqueness by
        combining the normalized application name with a short UUID suffix.

        The alias hostname is optional and derived from the user-provided hostname
        field if present.

        All hostnames are normalized to be DNS-compliant:
        - lowercase
        - invalid characters replaced
        - maximum label length enforced (63 characters)

        Args:
            app_name (str): Name of the application (key in config).
            app_uuid (str): Unique identifier of the application.
            base_domain (str): Base domain used for hostname generation.

        Raises:
            RoutingError: If generated hostname labels exceed DNS limits.
        """
        short_uuid = normalize_name(app_uuid)[:4]
        norm_name = normalize_name(app_name)

        suffix = f"-{short_uuid}"
        label = f"{norm_name}{suffix}"

        # enforce DNS label length
        MAX_LABEL = 63
        if len(label) > MAX_LABEL:
            raise RoutingError(
                f"Canonical hostname label too long: '{label}' ({len(label)} > {MAX_LABEL}). "
                f"Shorten app name '{app_name}'."
            )

        self.canonical_hostname = f"{label}.{base_domain}"

        # optional alias
        if self.hostname:
            alias = normalize_name(self.hostname)

            if len(alias) > MAX_LABEL:
                raise RoutingError(
                    f"Alias hostname label too long: '{alias}' ({len(alias)} > {MAX_LABEL}). "
                    f"Shorten hostname '{self.hostname}'."
                )

            self.alias_hostname = f"{alias}.{base_domain}"
        else:
            self.alias_hostname = None


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

    routing: Optional[Routing] = Field(
        default=None,
        description="Optional routing configuration to expose the application via Traefik (host-based routing)"
    )

    networks: Optional[List[str]] = Field(
        default=None,
        description="Optional list of Docker networks the App container should connect to"
    )

    deploy: Optional[Deploy] = Field(
        default=None,
        description="Deployment configuration including resources and placement."
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

    .. admonition:: Usage example

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
        Optional[Dict[str, OpenFactoryAppSchema]]: A dictionary of validated and enriched application configurations, or ``None`` if validation fails.

    Note:
        In case of validation errors, user notifications will be triggered and ``None`` will be returned.
    """
    # load yaml description file
    cfg = load_yaml(apps_yaml_config_file)

    # validate and create apps configuration
    try:
        apps_cfg = OpenFactoryAppsConfig(**cfg)
    except (ValidationError, ValueError) as err:
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

        if app.routing and app.routing.expose:
            app.routing.build_hostnames(
                app_name=app_name,
                app_uuid=app.uuid,
                base_domain=Config.OPENFACTORY_BASE_DOMAIN,
            )

    return apps
