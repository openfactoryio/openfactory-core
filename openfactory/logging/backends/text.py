"""
Text logging backend.
"""

import logging
from openfactory.logging.formatters import PrefixFormatter


class TextLoggerBackend:
    """ Logging backend for human-readable text logs. """

    @staticmethod
    def configure(logger: logging.Logger, level: int, prefix: str, text_log_directory: str | None) -> None:
        """
        Configure a logger for the text logging backend.

        Args:
            logger (logging.Logger): Logger to configure.
            level (int): Logging level.
            prefix (str): Prefix prepended to log messages.
            text_log_directory (str | None): Directory where human-readable log files are written. Currently unused.
        """

        logger.setLevel(level)

        if logger.handlers:
            return

        handler = logging.StreamHandler()
        handler.setFormatter(PrefixFormatter(prefix))

        logger.addHandler(handler)

        logger.propagate = False
