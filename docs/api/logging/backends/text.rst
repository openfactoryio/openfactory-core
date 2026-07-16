Text Logger Backend
===================

The ``text`` backend is the default OpenFactory logging backend.

It writes human-readable log messages to the standard output.

Example output:

.. code-block:: text

    [APP] (2026-07-16 14:32:18) INFO Application started

The text backend is primarily intended for local development and
interactive use.

Configuration
-------------

The text backend is selected by setting the following environment
variable:

.. code-block:: bash

    OPENFACTORY_LOG_BACKEND=text

Human-readable log files
------------------------

By default, log messages are written only to the standard output.

The ``OPENFACTORY_TEXT_LOG_DIRECTORY`` environment variable is ignored by this backend because
the primary output is already human-readable. If file-based logging is required,
standard operating system or container logging facilities should be used.

API Reference
-------------

.. autoclass:: openfactory.logging.backends.text.TextLoggerBackend
   :members:
