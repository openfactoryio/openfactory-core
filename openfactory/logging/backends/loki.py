"""
Loki logging backend for OpenFactory.
"""

import logging
from pathlib import Path
from openfactory.logging.formatters import JsonFormatter, PrefixFormatter


class LokiLoggerBackend:
    """ Logging backend for Grafana Loki. """

    @staticmethod
    def configure(logger: logging.Logger, level: int, prefix: str, text_log_directory: str | None) -> None:
        """
        Configure a logger for the Grafana Loki logging backend.

        Args:
            logger (logging.Logger): Logger to configure.
            level (int): Logging level.
            prefix (str): Prefix for human-readable log messages.
            text_log_directory (str | None): Directory where human-readable
                log files are written. If ``None``, the default directory is used.
        """

        logger.setLevel(level)

        if logger.handlers:
            return

        # JSON logs -> stdout
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(JsonFormatter())
        logger.addHandler(stream_handler)

        # Human-readable logs -> file
        log_directory = Path(text_log_directory or ".")
        log_directory.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_directory / f"{logger.name}.log")
        file_handler.setFormatter(PrefixFormatter(prefix))
        logger.addHandler(file_handler)

        logger.propagate = False
