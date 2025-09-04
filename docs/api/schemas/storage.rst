.. _storage-schemas-index:

OpenFactory Storage Schemas
===========================

OpenFactory defines various Storage Backends which allows to add storage to deployed OpenFactory Applications.

.. seealso::

   :ref:`filelayer-index`.

Each Storage Backend is described by a schema.
All Storage Backends schemas derive from the :class:`openfactory.schemas.filelayer.base_backend.BaseBackendConfig` class.

The following schemas are defined so far to describe them:

.. toctree::
   :maxdepth: 1

   storage.types
   storage.storage
   storage.base
   storage.local_backend
   storage.nfs_backend
