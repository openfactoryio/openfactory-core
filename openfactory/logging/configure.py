"""
Logging configuration for OpenFactory.
"""

import logging
import os
from openfactory.logging.loggers import OpenFactoryLogger
from openfactory.logging.backends.text import TextLoggerBackend
from openfactory.logging.backends.loki import LokiLoggerBackend


def configure_logger(name: str, level=logging.INFO, prefix="APP") -> OpenFactoryLogger:
    """
    Configure an OpenFactory logger.

    Creates and configures a standard Python :class:`logging.Logger`
    using the logging backend selected through the OpenFactory
    environment configuration.

    Args:
        name (str): Name of the logger.
        level (int): Logging level (e.g. ``logging.INFO`` or ``logging.DEBUG``). Defaults to ``logging.INFO``.
        prefix (str): Prefix added to log messages by backends that support prefixed text output. Defaults to ``"APP"``.

    Returns:
        OpenFactoryLogger: The configured logger instance.

    Raises:
        ValueError: If the configured logging backend is unknown.

    Note:
        - The logging backend is selected using the ``OPENFACTORY_LOG_BACKEND`` environment variable.
        - Human-readable log files may optionally be written to the directory specified by the
          ``OPENFACTORY_TEXT_LOG_DIRECTORY`` environment variable, depending on the selected backend.
    """

    logging.setLoggerClass(OpenFactoryLogger)
    logger = logging.getLogger(name)

    backend_name = os.getenv("OPENFACTORY_LOG_BACKEND", "text").lower()
    text_log_directory = os.getenv("OPENFACTORY_TEXT_LOG_DIRECTORY")

    if backend_name == "text":
        TextLoggerBackend.configure(
            logger=logger,
            level=level,
            prefix=prefix,
            text_log_directory=text_log_directory
        )

    elif backend_name == "loki":
        LokiLoggerBackend.configure(
            logger=logger,
            level=level,
            prefix=prefix,
            text_log_directory=text_log_directory
        )

    else:
        raise ValueError(f"Unknown logging backend '{backend_name}'.")

    return logger
