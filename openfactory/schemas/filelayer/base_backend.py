"""
Base configuration schema for all storage backends in OpenFactory applications.

This module defines the :class:`.BaseBackendConfig` class, which serves as the
foundation for backend-specific configuration schemas (e.g., NFSBackendConfig,
LocalBackendConfig, S3BackendConfig).

Note:
    - Use subclasses to define fields specific to each backend.
    - The `extra="forbid"` option ensures that typos or unexpected fields in
      configuration files raise validation errors.
    - Subclasses must implement :meth:`.BaseBackendConfig.create_backend_instance`
      to return the actual runtime backend instance.

Warning:
    The `BaseBackendConfig` class is an abstract class not intented to be used.
"""

from typing import Optional
from pydantic import BaseModel, Field


class BaseBackendConfig(BaseModel):
    """
    Base configuration schema for all storage backends in OpenFactory applications.

    All backend-specific configuration fields must be defined in a subclass
    of :class:`BaseBackendConfig` (e.g., NFSBackendConfig, S3BackendConfig).

    .. admonition:: Ading a new backend

          1. Create a subclass of :class:`BaseBackendConfig` with its specific fields.
          2. Update :class:`openfactory.schemas.filelayer.types.StorageBackend` to accept the new backend type.
          3. Implement the corresponding :class:`openfactory.filelayer.backend.FileBackend` subclass for deployment and usage.
    """

    type: str = Field(..., description="Type of the backend (e.g., NFS, S3, etc.)")
    description: Optional[str] = Field(None, description="Optional description of the backend configuration.")

    model_config = {
        "extra": "forbid",              # Disallow extra fields to avoid typos in config files
        "validate_assignment": True,    # Ensure validation on attribute assignment
    }

    def create_backend_instance(self):
        """
        Create a runtime backend instance from this configuration.

        Subclasses must implement this method to return the actual backend
        instance (e.g., `NFSBackend`).

        Returns:
            Any: Runtime backend instance corresponding to this configuration.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement this method")
