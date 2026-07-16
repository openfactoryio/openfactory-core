import logging
import unittest
from unittest.mock import patch

from openfactory.logging.configure import configure_logger
from openfactory.logging.loggers import OpenFactoryLogger


class TestConfigureLogger(unittest.TestCase):
    """ Tests for configure_logger. """

    @patch("openfactory.logging.configure.TextLoggerBackend.configure")
    @patch.dict("os.environ", {}, clear=True)
    def test_default_backend_is_text(self, mock_configure):
        """ The text backend should be used by default. """

        logger = configure_logger("test")

        self.assertIsInstance(logger, OpenFactoryLogger)
        mock_configure.assert_called_once_with(
            logger=logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

    @patch("openfactory.logging.configure.TextLoggerBackend.configure")
    @patch.dict(
        "os.environ",
        {
            "OPENFACTORY_LOG_BACKEND": "text",
            "OPENFACTORY_TEXT_LOG_DIRECTORY": "/tmp/logs",
        },
        clear=True,
    )
    def test_text_backend(self, mock_configure):
        """ The text backend should be configured. """

        logger = configure_logger("test", level=logging.DEBUG, prefix="TEST")

        mock_configure.assert_called_once_with(
            logger=logger,
            level=logging.DEBUG,
            prefix="TEST",
            text_log_directory="/tmp/logs",
        )

    @patch("openfactory.logging.configure.LokiLoggerBackend.configure")
    @patch.dict(
        "os.environ",
        {
            "OPENFACTORY_LOG_BACKEND": "loki",
            "OPENFACTORY_TEXT_LOG_DIRECTORY": "/tmp/logs",
        },
        clear=True,
    )
    def test_loki_backend(self, mock_configure):
        """ The Loki backend should be configured. """

        logger = configure_logger("test")

        mock_configure.assert_called_once_with(
            logger=logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory="/tmp/logs",
        )

    @patch.dict(
        "os.environ",
        {"OPENFACTORY_LOG_BACKEND": "unknown"},
        clear=True,
    )
    def test_unknown_backend(self):
        """ Unknown backends should raise a ValueError. """

        with self.assertRaises(ValueError):
            configure_logger("test")

    @patch("openfactory.logging.configure.TextLoggerBackend.configure")
    @patch.dict("os.environ", {}, clear=True)
    def test_returns_openfactory_logger(self, mock_configure):
        """ configure_logger should return an OpenFactoryLogger. """

        logger = configure_logger("test")

        self.assertIsInstance(logger, OpenFactoryLogger)

    @patch("openfactory.logging.configure.TextLoggerBackend.configure")
    @patch.dict("os.environ", {}, clear=True)
    def test_returns_same_logger_instance(self, mock_configure):
        """ configure_logger should return the existing logger instance. """

        logger1 = configure_logger("test")
        logger2 = configure_logger("test")

        self.assertIs(logger1, logger2)
