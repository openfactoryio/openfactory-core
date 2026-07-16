"""
Logging support for OpenFactory.

OpenFactory configures logging through environment variables so that the
logging backend can be selected at deployment time without requiring any
changes to application code.

Environment variables:
    ``OPENFACTORY_LOG_BACKEND``:
        Logging backend to use.

        Supported values:
            - ``text``: Human-readable logs written to stdout.
            - ``loki``: Structured JSON logs written to stdout and human-readable log files.

        Defaults to ``text``.

    ``OPENFACTORY_TEXT_LOG_DIRECTORY``:
        Optional directory where human-readable log files are written by
        logging backends that support text logging.

        If not specified, the backend uses its default location.
"""

from .configure import configure_logger
