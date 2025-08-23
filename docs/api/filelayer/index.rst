.. _filelayer-index:

OpenFactory Storage
===================

OpenFactory defines Storage Backends which allows to add storage to deployed OpenFactory Apps.

All Storage Backends derive from :class:`openfactory.filelayer.backend.FileBackend` class.
Each Storage Backend has a corresponding schema.

.. seealso::

   :ref:`storage-schemas-index`.

So far the following Storage Backends are implemented:

.. toctree::
   :maxdepth: 1

   backend
   local_backend
   nfs_backend
