"""
NFS (POSIX filesystem) implementation of the FileBackend interface for OpenFactory.

This backend allows OpenFactory services to interact with a local or
network-mounted filesystem (NFS share) using the standard FileBackend API.

Note:
    - This backend is compatible with Docker Swarm and can generate
      mount specifications via :meth:`.NFSBackend.get_mount_spec`.
    - Ensure the NFS server is reachable and that mount options are valid
      for your environment.
"""

from pathlib import Path
from typing import IO, List, Dict, Any
from openfactory.filelayer.backend import FileBackend
from openfactory.schemas.filelayer.nfs_backend import NFSBackendConfig


class NFSBackend(FileBackend):
    """
    FileBackend implementation for POSIX/NFS filesystems.
    """

    def __init__(self, config: NFSBackendConfig):
        """
        Initialize the NFSBackend.

        Args:
            config (NFSBackendConfig): Configuration object for the NFS backend.
        """
        self.config = config
        self.root = Path(config.mount_point)

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
            # Make absolute path relative to root
            p = p.relative_to(self.root)

        return self.root / p

    def open(self, path: str, mode: str = "r") -> IO:
        """
        Open a file at the given path.

        Args:
            path (str): Path to the file relative to the backend root.
            mode (str): File mode ('r', 'w', 'rb', 'wb', etc.). Defaults to 'r'.

        Returns:
            IO: File-like object supporting read/write operations.

        Raises:
            FileNotFoundError: If the file does not exist when opening in read mode.
        """
        full_path = self._full_path(path)
        # Ensure parent directories exist when writing
        if "w" in mode or "a" in mode or "x" in mode:
            full_path.parent.mkdir(parents=True, exist_ok=True)
        return full_path.open(mode)

    def exists(self, path: str) -> bool:
        """
        Check whether a file exists at the given path.

        Args:
            path (str): Path to the file relative to the backend root.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return self._full_path(path).exists()

    def delete(self, path: str) -> None:
        """
        Delete the file at the given path.

        Args:
            path (str): Path to the file relative to the backend root.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        full_path = self._full_path(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File {full_path} does not exist.")
        full_path.unlink()

    def listdir(self, path: str) -> List[str]:
        """
        List all files and directories at the given path.

        Args:
            path (str): Path to the directory relative to the backend root.

        Returns:
            List[str]: List of file and directory names.

        Raises:
            FileNotFoundError: If the directory does not exist.
        """
        full_path = self._full_path(path)
        if not full_path.exists() or not full_path.is_dir():
            raise FileNotFoundError(f"Directory {full_path} does not exist.")
        return [f.name for f in full_path.iterdir()]

    @staticmethod
    def from_config(config: dict) -> "NFSBackend":
        """
        Create an NFSBackend instance from configuration.

        Args:
            config (dict): Configuration dictionary.

        Returns:
            NFSBackend: Configured backend instance.

        Raises:
            pydantic.ValidationError: If the configuration is invalid.
        """
        validated_config = NFSBackendConfig(**config)
        return NFSBackend(validated_config)

    def get_mount_spec(self) -> Dict[str, Any]:
        """
        Build a Docker-compatible mount specification for NFS.

        This specification can be used directly in
        Docker Swarm service creation or container deployment.

        Returns:
            Dict[str, Any]: A dictionary containing the mount configuration in Docker's expected format.
        """
        opts = {
            "type": "nfs",
            "o": f"addr={self.config.server}," + ",".join(self.config.mount_options or []),
            "device": f":{self.config.remote_path}"
        }

        return {
            "Type": "volume",
            "Source": f"nfs_{self.config.server.replace('.', '_')}_{self.config.remote_path.strip('/').replace('/', '_')}",
            "Target": self.config.mount_point,
            "VolumeOptions": {
                "DriverConfig": {
                    "Name": "local",
                    "Options": opts
                }
            }
        }
