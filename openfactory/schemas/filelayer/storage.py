from pydantic import BaseModel
from openfactory.schemas.filelayer.types import StorageBackend


class StorageBackendSchema(BaseModel):
    """
    Pydantic wrapper for a storage backend configuration.

    This class allows parsing and validation of a dictionary
    that represents a storage backend. It automatically selects the correct
    backend subclass based on the `type` field using the `StorageBackend` discriminated union.

    Example usage:
        .. code-block:: python

            from openfactory.schemas.filelayer.storage import StorageBackendSchema

            # Example storage backend (NFS)
            storage_dict = {
                "type": "nfs",
                "server": "10.0.5.2",
                "remote_path": "/nfs/data",
                "mount_point": "/mnt",
                "mount_options": ["rw", "noatime"]
            }

            # Parse and validate
            storage_schema = StorageBackendSchema(storage=storage_dict)

            # Get runtime backend instance
            backend_instance = storage_schema.storage.create_backend_instance()

    Note:
        - `storage` contains the parsed backend schema (a subclass of :class:`openfactory.schemas.filelayer.base_backend.BaseBackendConfig`).
        - Use `create_backend_instance()` on `.storage` to obtain the runtime
          backend (e.g., `NFSBackend`, `LocalBackend`) for actual file operations.
        - Supports any backend added to the `StorageBackend` discriminated union.
    """
    storage: StorageBackend
