"""
Asset Forwarder Logger
======================

This module provides a singleton logger for the OpenFactory Asset Forwarder
service, pre-configured with a consistent format and log level.

Usage
-----

.. code-block:: python

    from logger import logger

    logger.info("Starting forwarder")
    logger.debug("Debugging details here")

Configuration
-------------

- The log level can be set via the environment variable ``ASSET_FORWARDER_LOG_LEVEL`` (default: ``INFO``).
- The logger outputs to the standard output stream.
- Only one handler is attached to prevent duplicate logs when imported multiple times.

Functions
---------

- ``get_logger(name: str) -> logging.Logger``: Returns a configured logger instance, creating it if necessary.

Singleton
---------

- ``logger``: A module-level singleton logger instance using the default name ``"asset_forwarder"``.
"""

import logging
import os


def get_logger(name: str = "asset_forwarder") -> logging.Logger:
    """
    Returns a configured logger instance.
    """
    logger = logging.getLogger(name)

    # Only configure if it has no handlers (prevents duplicate logs)
    if not logger.hasHandlers():
        log_level = os.getenv("ASSET_FORWARDER_LOG_LEVEL", "INFO").upper()
        logger.setLevel(log_level)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Singleton logger instance
logger = get_logger()
