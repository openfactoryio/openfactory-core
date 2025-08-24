"""
Storage Backend Type

This module defines the `StorageBackend` type used in OpenFactory application schemas.
It represents a discriminated union of all supported storage backend configurations
(e.g., NFS, ...).

By using a Pydantic `Annotated[Union[...], Field(discriminator='type')]`, the input
YAML or dictionary is automatically parsed and validated against the correct backend
schema based on the `type` field.

Adding a New Storage Backend:
-----------------------------
1. Create a subclass of BaseBackendConfig (e.g., S3BackendConfig) with its specific fields.
2. Add the new subclass to the `Union` inside `StorageBackend`.
3. Ensure the subclass defines a unique `type` literal matching the YAML `type` field.
"""

from typing import Annotated, Union
from pydantic import Field
from openfactory.schemas.filelayer.nfs_backend import NFSBackendConfig
from openfactory.schemas.filelayer.local_backend import LocalBackendConfig


StorageBackend = Annotated[
    Union[
        NFSBackendConfig,
        LocalBackendConfig,
        # Add other Storage Backend models here as needed
    ],
    Field(
        discriminator="type",
        description="Discriminator field to select the correct storage backend schema"
    )
]
"""
Union of all supported storage backend schemas with type-based discrimination.

This is a type alias for all known storage backend schemas, used for validation
and parsing based on the `type` discriminator field.
"""
