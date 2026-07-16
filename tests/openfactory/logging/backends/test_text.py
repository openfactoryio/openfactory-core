import logging
import unittest
from openfactory.logging.backends.text import TextLoggerBackend
from openfactory.logging.formatters import PrefixFormatter


class TestTextLoggerBackend(unittest.TestCase):
    """ Tests for TextLoggerBackend. """

    def setUp(self):
        self.logger = logging.Logger("test")

    def test_sets_logger_level(self):
        """ The logger level should be configured. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.DEBUG,
            prefix="APP",
            text_log_directory=None,
        )

        self.assertEqual(self.logger.level, logging.DEBUG)

    def test_adds_stream_handler(self):
        """ A StreamHandler should be added to the logger. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

        self.assertEqual(len(self.logger.handlers), 1)
        self.assertIsInstance(self.logger.handlers[0], logging.StreamHandler)

    def test_stream_handler_uses_prefix_formatter(self):
        """ The StreamHandler should use a PrefixFormatter. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

        handler = self.logger.handlers[0]

        self.assertIsInstance(handler.formatter, PrefixFormatter)

    def test_disables_propagation(self):
        """ Logger propagation should be disabled. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

        self.assertFalse(self.logger.propagate)

    def test_does_not_add_duplicate_handlers(self):
        """ Existing handlers should not be duplicated. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory=None,
        )

        self.assertEqual(len(self.logger.handlers), 1)

    def test_ignores_text_log_directory(self):
        """ text_log_directory should currently be ignored. """

        TextLoggerBackend.configure(
            logger=self.logger,
            level=logging.INFO,
            prefix="APP",
            text_log_directory="/tmp/logs",
        )

        self.assertEqual(len(self.logger.handlers), 1)
        self.assertIsInstance(self.logger.handlers[0], logging.StreamHandler)
