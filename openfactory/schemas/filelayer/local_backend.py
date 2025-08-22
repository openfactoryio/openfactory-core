"""
LocalBackend configuration schema for OpenFactory applications.

This module defines the :class:`.LocalBackendConfig`, which extends
:class:`.BaseBackendConfig` to configure local host directory backends.

Warning:
    The LocalBackend is primarily intended for development or testing purposes.
    It allows OpenFactory services to bind-mount a directory from the host filesystem
    into Docker containers. For production or network-shared storage, use networked
    backends (e.g., NFS or S3).

Note:
    - Subclasses must implement :meth:`.LocalBackendConfig.create_backend_instance`
      to return the runtime backend.
    - All paths are validated to be absolute and to exist on the host.
    - Mount points inside containers are normalized and restricted to safe characters.
    - This module only provides configuration and validation; it does not perform
      actual file operations.
"""

import os
import re
from typing import Literal
from pydantic import Field, field_validator
from openfactory.schemas.filelayer.base_backend import BaseBackendConfig
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openfactory.filelayer.local_backend import LocalBackend


class LocalBackendConfig(BaseBackendConfig):
    """
    Configuration schema for LocalBackend.

    This backend is intended primarily for development or testing purposes.
    It allows OpenFactory services to bind-mount a local host directory
    into Docker containers. For production or network-shared storage,
    use networked backends.
    """

    type: Literal["local"] = Field(description="Type of the backend")
    local_path: str = Field(..., description="Absolute path on the host machine")
    mount_point: str = Field(..., description="Mount point inside the container")

    def create_backend_instance(self) -> "LocalBackend":
        """
        Instantiate the runtime LocalBackend using this configuration.

        Returns:
            LocalBackend: Runtime backend instance.
        """
        from openfactory.filelayer.local_backend import LocalBackend
        return LocalBackend(self)

    @field_validator("local_path")
    def validate_path(cls, value: str) -> str:
        """
        Validates that the host path exists and is a valid absolute path.

        Args:
            value (str): Path to validate.

        Returns:
            str: Normalized absolute path.

        Raises:
            ValueError: If the path is not absolute or does not exist on the host.
        """
        value = value.strip()
        if not value.startswith("/"):
            raise ValueError("path must be an absolute path on the host")

        norm_path = os.path.normpath(value)

        if not os.path.exists(norm_path):
            raise ValueError(f"host path '{norm_path}' does not exist")

        if not os.path.isdir(norm_path):
            raise ValueError(f"host path '{norm_path}' must be a directory")

        return norm_path

    @field_validator("mount_point", mode="before")
    def validate_mount_point(cls, value: str) -> str:
        """
        Validates and normalizes the mount point inside the container.

        Args:
            value (str): Container mount point path.

        Returns:
            str: Normalized absolute path.

        Raises:
            ValueError: If the path is relative, contains '..', invalid chars, or too long.
        """
        if ".." in value:
            raise ValueError("mount_point must not contain '..'")

        norm = os.path.normpath(value)
        if not norm.startswith("/"):
            raise ValueError("mount_point must be an absolute path inside the container")
        if re.search(r"[^a-zA-Z0-9._/-]", norm):
            raise ValueError("mount_point contains invalid characters")
        if len(norm) > 255:
            raise ValueError("mount_point is too long (max 255 chars)")
        return norm

    model_config = {
        "extra": "forbid",
        "str_strip_whitespace": True
    }
