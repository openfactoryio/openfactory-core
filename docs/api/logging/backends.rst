.. _logging-backends-index:

OpenFactory Logger Backends
===========================

OpenFactory supports multiple logging backends.

The logging backend determines how log entries are emitted (for example,
human-readable text or structured JSON).

OpenFactory Applications always use the same logging API regardless of
the configured backend. No code changes or additional configuration are
required in the application.

For example, the following code behaves identically with every logging
backend:

.. code-block:: python

    logger = self.logger.with_context(
        asset_uuid=device.uuid,
    )

    logger.info(
        "Temperature updated",
        extra={"temperature": 42.3},
    )

The logging backend is selected through the
``OPENFACTORY_LOG_BACKEND`` environment variable. This environment
variable is automatically injected by OpenFactory when the application is
deployed to an OpenFactory Cluster. Applications should never detect or
depend on the selected backend.

The following logging backends are currently available:

.. toctree::
   :maxdepth: 1

   backends/text
   backends/loki
