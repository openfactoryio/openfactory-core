"""
Abstract base class for file storage backends in OpenFactory.

This module defines the FileBackend interface, which provides a uniform
API for reading, writing, listing, and deleting files across different
storage backends (e.g., NFS, MinIO, WebDAV). Specific backends should
implement this abstract class.

The `from_config` static method provides a factory for instantiating
the appropriate backend based on a configuration dictionary.

Warning:
   The `FileBackend` class is an abstract class not intented to be used.
"""

from abc import ABC, abstractmethod
from typing import IO, List


class FileBackend(ABC):
    """
    Abstract base class defining the interface for all file storage backends.
    """

    def compatible_with_swarm(self) -> bool:
        """
        Returns whether this backend can be safely used with SwarmDeploymentStrategy.

        By default, backends are assumed compatible. Override in subclasses
        that cannot be used with Swarm.

        Returns:
            bool: True if compatible with Swarm, False otherwise.
        """
        return True

    @abstractmethod
    def open(self, path: str, mode: str = "r") -> IO:
        """
        Open a file at the given path.

        Args:
            path (str): Path to the file relative to the backend root.
            mode (str): File mode ('r', 'w', 'rb', 'wb', etc.). Defaults to 'r'.

        Returns:
            IO: A file-like object supporting read/write operations.

        Raises:
            NotImplementedError: Must be implemented in a subclass.
        """
        raise NotImplementedError("open() must be implemented in a subclass")

    @abstractmethod
    def exists(self, path: str) -> bool:
        """
        Check whether a file exists at the given path.

        Args:
            path (str): Path to the file relative to the backend root.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            NotImplementedError: Must be implemented in a subclass.
        """
        raise NotImplementedError("exists() must be implemented in a subclass")

    @abstractmethod
    def delete(self, path: str) -> None:
        """
        Delete the file at the given path.

        Args:
            path (str): Path to the file relative to the backend root.

        Raises:
            FileNotFoundError: If the file does not exist.
            NotImplementedError: Must be implemented in a subclass.
        """
        raise NotImplementedError("delete() must be implemented in a subclass")

    @abstractmethod
    def listdir(self, path: str) -> List[str]:
        """
        List all files and directories at the given path.

        Args:
            path (str): Path to the directory relative to the backend root.

        Returns:
            List[str]: List of file and directory names.

        Raises:
            NotImplementedError: Must be implemented in a subclass.
        """
        raise NotImplementedError("listdir() must be implemented in a subclass")

    @staticmethod
    def from_config(config: dict) -> "FileBackend":
        """
        Factory method to create a FileBackend instance from a configuration dictionary.

        Args:
            config (dict): Configuration dictionary specifying the backend type and options.

        Returns:
            FileBackend: An instance of a concrete FileBackend subclass.

        Raises:
            NotImplementedError: Must be implemented in a subclass or factory function.
        """
        raise NotImplementedError("from_config() must be implemented in a subclass or factory function")

    @abstractmethod
    def get_mount_spec(self) -> dict | None:
        """
        Return the mount specification for Docker, if applicable.

        Returns:
            dict: Docker mount spec (source, target, type, options) or None if not mountable.
        """
        raise NotImplementedError()
