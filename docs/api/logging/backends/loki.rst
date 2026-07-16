Loki Logger Backend
===================

The ``loki`` backend writes structured JSON log records to the standard
output.

It is intended for structured log aggregation systems such as
Grafana Loki.

Example output:

.. code-block:: json

    {
        "timestamp": "2026-07-16T20:46:01.105+00:00",
        "level": "INFO",
        "logger": "DEV-UUID",
        "message": "Counter=1",
        "asset_uuid": "DEVICE-001",
        "temperature": 42.3
    }

Contextual logging
------------------

The Loki backend automatically serializes all contextual information
attached to a logger using
:meth:`OpenFactoryLogger.with_context <openfactory.logging.loggers.OpenFactoryLogger.with_context>`.

Additional context supplied through the standard Python ``extra``
argument is also included in the JSON output.

Configuration
-------------

The Loki backend is selected by setting the following environment
variable:

.. code-block:: bash

    OPENFACTORY_LOG_BACKEND=loki

Human-readable log files
------------------------

If the ``OPENFACTORY_TEXT_LOG_DIRECTORY`` environment variable is set,
the backend additionally writes human-readable log messages to a text
log file in the specified directory.

This is useful when structured JSON logs are collected by Grafana Loki
while developers still want readable log files on disk.

API Reference
-------------

.. autoclass:: openfactory.logging.backends.loki.LokiLoggerBackend
   :members:
