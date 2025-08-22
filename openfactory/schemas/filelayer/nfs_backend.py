"""
NFSBackend configuration schema for OpenFactory applications.

This module defines the :class:`.NFSBackendConfig`, which extends
:class:`.BaseBackendConfig` to configure NFS (POSIX filesystem) backends.

The NFSBackend allows OpenFactory services to mount a network-shared
directory from an NFS server into Docker containers, providing a standard
FileBackend interface for file operations.

Note:
    - Server addresses are validated as hostnames, IPv4, or IPv6 (IPv6 must
      be in brackets).
    - Remote paths and container mount points are validated to be absolute,
      normalized, and safe.
    - Optional mount options can be specified for fine-tuning NFS mounts.
    - This module only provides configuration and validation; it does not perform
      actual file operations.
"""

import re
import os
from pydantic import Field, field_validator
from typing import Optional, List, Literal
from openfactory.schemas.filelayer.base_backend import BaseBackendConfig
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from openfactory.filelayer.nfs_backend import NFSBackend


class NFSBackendConfig(BaseBackendConfig):
    """
    Configuration schema for NFSBackend.

    This schema is used by the deployment tool to mount an NFS share
    into OpenFactory service containers. It validates paths, server addresses,
    and optional mount options.
    """

    type: Literal["nfs"] = Field(description="Type of the backend")
    server: str = Field(..., description="Hostname or IP of the NFS server")
    remote_path: str = Field(..., description="Path to the shared directory on the NFS server")
    mount_point: str = Field(..., description="Path where the NFS share is mounted on the OpenFactory service container")
    mount_options: Optional[List[str]] = Field(
        None, description="List of mount options (e.g., ['rw', 'vers=4.1', 'noatime'])"
    )

    def create_backend_instance(self) -> "NFSBackend":
        """
        Instantiate the runtime NFSBackend using this configuration.

        Returns:
            NFSBackend: Runtime backend instance.
        """
        from openfactory.filelayer.nfs_backend import NFSBackend
        return NFSBackend(self)

    @field_validator("server")
    def validate_server(cls, value: str) -> str:
        """
        Validates that the server is a non-empty string representing
        a valid hostname, IPv4 address, or IPv6 address (IPv6 must be in brackets).

        Args:
            value (str): Server string to validate.

        Returns:
            str: Cleaned server string.

        Raises:
            ValueError: If the server string is empty or invalid.
        """
        value = value.strip()
        if not value:
            raise ValueError("server cannot be empty")
        # Hostname, IPv4, or IPv6 (in brackets)
        ipv4_pattern = r"(?:\d{1,3}\.){3}\d{1,3}"
        hostname_pattern = r"(?:[a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+"
        ipv6_pattern = r"\[[0-9a-fA-F:]+\]"

        if not re.fullmatch(f"(?:{hostname_pattern}|{ipv4_pattern}|{ipv6_pattern})", value):
            raise ValueError("server must be a valid hostname, IPv4, or IPv6 address (IPv6 must be in [brackets])")
        return value

    @field_validator("remote_path")
    def validate_export_path(cls, value: str) -> str:
        """
        Validates and normalizes the remote_path.

        Args:
            value (str): Remote path on the NFS server.

        Returns:
            str: Normalized absolute path.

        Raises:
            ValueError: If the path is not absolute.
        """
        value = value.strip()
        if not value.startswith("/"):
            raise ValueError("remote_path must be an absolute path")
        return value

    @field_validator("mount_point")
    def validate_mount_point(cls, value: str) -> str:
        """
        Validates and normalizes the mount point inside the container.

        Args:
            value (str): Mount point path.

        Returns:
            str: Normalized absolute path.

        Raises:
            ValueError: If the path is invalid (relative, contains '..', invalid chars, or too long).
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

    @field_validator("mount_options", mode="before")
    def clean_mount_options(cls, value):
        """
        Cleans the mount_options list by stripping whitespace and removing empty strings.

        Args:
            value (Optional[List[str]]): List of mount options.

        Returns:
            Optional[List[str]]: Cleaned list of mount options or None.
        """
        if value is None:
            return None
        return [opt.strip() for opt in value if isinstance(opt, str) and opt.strip()]

    model_config = {
        "extra": "forbid",
        "str_strip_whitespace": True
    }
