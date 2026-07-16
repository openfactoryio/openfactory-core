"""
Logging formatters for OpenFactory.
"""

import logging
import json
from datetime import datetime, timezone


class PrefixFormatter(logging.Formatter):
    """ Formatter that prepends a prefix to log messages. """

    def __init__(self, prefix: str, fmt: str | None = None, datefmt: str | None = None):
        """
        Initialize the formatter.

        Args:
            prefix (str): Prefix to prepend to every log message.
            fmt (str | None): Log message format. If ``None``, the default OpenFactory text log format is used.
            datefmt (str | None): Date and time format. If ``None``, the default OpenFactory date format is used.
        """

        base_fmt = fmt or "(%(asctime)s) %(levelname)s %(message)s"
        full_fmt = f"[{prefix}] {base_fmt}"

        super().__init__(full_fmt, datefmt or "%Y-%m-%d %H:%M:%S")


class JsonFormatter(logging.Formatter):
    """ Formatter that serializes log records as JSON. """

    # Attributes provided by Python's standard LogRecord implementation.
    STANDARD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.

        Args:
            record (logging.LogRecord): Log record to format.

        Returns:
            str: JSON representation of the log record.
        """
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Serialize all user-defined LogRecord attributes.
        for key, value in record.__dict__.items():
            if key not in self.STANDARD_FIELDS:
                log_record[key] = value

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)
