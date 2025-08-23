"""
Local host directory implementation of the FileBackend interface for OpenFactory.

This backend allows OpenFactory services to access a directory on the host
filesystem via a bind mount in Docker containers. It is intended primarily
for development and testing purposes.

Note:
    - This backend is **not compatible** with Docker Swarm.
    - Mount specifications can be generated via :meth:`.LocalBackend.get_mount_spec`.
    - Ensure the local path exists and has the proper permissions for the container.

.. seealso::

   The schema of the Local Backend is :class:`openfactory.schemas.filelayer.local_backend.LocalBackendConfig`.
"""

from pathlib import Path
from typing import IO, List, Dict, Any
from openfactory.filelayer.backend import FileBackend
from openfactory.schemas.filelayer.local_backend import LocalBackendConfig


class LocalBackend(FileBackend):
    """
    FileBackend implementation for local host directories.

    This backend allows OpenFactory services to access a directory
    on the host filesystem via a bind mount in Docker containers.
    """

    def __init__(self, config: LocalBackendConfig):
        """
        Initialize the LocalBackend.

        Args:
            config (LocalBackendConfig): Configuration object for the local backend.
        """
        self.config = config
        self.root = Path(config.local_path)

    def _full_path(self, path: str) -> Path:
        """
        Compute full path relative to backend root.

        Args:
            path (str): Relative or absolute path.

        Returns:
            Path: Absolute Path object.

        Raises:
            ValueError: If an absolute path is outside the backend root.
        """
        p = Path(path)
        if p.is_absolute() and not p.is_relative_to(self.root):
            raise ValueError(f"Path {p} is outside backend root {self.root}")
        if p.is_absolute():
            p = p.relative_to(self.root)
        return self.root / p

    def open(self, path: str, mode: str = "r") -> IO:
        """
        Open a file at the given path.

        Args:
            path (str): Path relative to backend root.
            mode (str): File mode ('r', 'w', 'rb', 'wb', etc.).

        Returns:
            IO: File-like object.
        """
        full_path = self._full_path(path)
        if "w" in mode or "a" in mode or "x" in mode:
            full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path.open(mode)

    def exists(self, path: str) -> bool:
        """ Check if a file exists at the given path. """
        return self._full_path(path).exists()

    def delete(self, path: str) -> None:
        """ Delete the file at the given path. """
        full_path = self._full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File {full_path} does not exist.")
        full_path.unlink()

    def listdir(self, path: str) -> List[str]:
        """ List files and directories at the given path. """
        full_path = self._full_path(path)
        if not full_path.exists() or not full_path.is_dir():
            raise FileNotFoundError(f"Directory {full_path} does not exist.")
        return [f.name for f in full_path.iterdir()]

    @staticmethod
    def from_config(config: dict) -> "LocalBackend":
        """
        Create a LocalBackend instance from a dictionary config.

        Args:
            config (dict): Configuration dictionary.

        Returns:
            LocalBackend: Configured backend instance.
        """
        validated_config = LocalBackendConfig(**config)
        return LocalBackend(validated_config)

    def get_mount_spec(self) -> Dict[str, Any]:
        """
        Build a Docker-compatible mount specification for local bind mount.

        Returns:
            Dict[str, Any]: Mount spec suitable for LocalDockerDeploymentStrategy.
        """
        return {
            "Type": "bind",
            "Source": str(self.root),
            "Target": self.config.mount_point,
            "ReadOnly": False  # Could add an optional config field for read-only if needed
        }

    def compatible_with_swarm(self) -> bool:
        """
        Returns False because LocalBackend cannot be used with SwarmDeploymentStrategy.
        """
        return False
