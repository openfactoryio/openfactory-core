OpenFactory Loggers
===================

Every OpenFactory Application automatically provides a logger through
``self.logger``.

The logger behaves like a standard Python :class:`logging.Logger` and can
be used with the standard logging methods:

.. code-block:: python

    self.logger.info("Application started")
    self.logger.warning("Low battery")
    self.logger.error("Connection lost")

The actual logging backend is selected by the OpenFactory deployment
configuration. Applications continue to use ``self.logger`` regardless of
whether logs are written as human-readable text, JSON for Grafana Loki,
or another backend.

Contextual logging
------------------

Many applications repeatedly log information about the same object, such
as a device, worker, production order or asset.

Instead of repeating this information in every logging call, OpenFactory
allows context to be attached to a logger. Every log entry produced by
that logger automatically includes the attached context.

.. code-block:: python

    device_logger = self.logger.with_context(
        asset_uuid=device.uuid,
        device="Robot 7",
    )

    device_logger.info("Connected")

Per-log-entry context
---------------------

Additional context can be attached to individual log entries using the
standard Python ``extra`` argument.

.. code-block:: python

    device_logger.info(
        "Temperature updated",
        extra={
            "temperature": 42.3,
            "unit": "°C",
        }
    )

The two mechanisms serve different purposes:

- :meth:`OpenFactoryLogger.with_context <openfactory.logging.loggers.OpenFactoryLogger.with_context>`
  attaches **persistent context** to a logger. Every log entry emitted
  through that logger automatically includes the supplied fields.

- The ``extra`` argument attaches **context specific to a single log
  entry**. The additional fields are included only in that particular
  logging call.


API Reference
-------------

OpenFactory provides the following logging classes:

.. toctree::
   :maxdepth: 1

   logger

OpenFactory provides the following logging backends:

.. toctree::
   :maxdepth: 1

   backends
