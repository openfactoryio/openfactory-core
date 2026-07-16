import logging
import tempfile
import unittest
from pathlib import Path

from openfactory.logging.backends.loki import LokiLoggerBackend
from openfactory.logging.formatters import JsonFormatter, PrefixFormatter


class TestLokiLoggerBackend(unittest.TestCase):
    """ Tests for LokiLoggerBackend. """

    def setUp(self):
        self.logger = logging.Logger("test")

    def test_sets_logger_level(self):
        """ The logger level should be configured. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.DEBUG,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        self.assertEqual(self.logger.level, logging.DEBUG)

    def test_adds_stream_and_file_handlers(self):
        """ A StreamHandler and FileHandler should be added to the logger. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        self.assertEqual(len(self.logger.handlers), 2)

        self.assertIsInstance(self.logger.handlers[0], logging.StreamHandler)
        self.assertIsInstance(self.logger.handlers[1], logging.FileHandler)

    def test_stream_handler_uses_json_formatter(self):
        """ The StreamHandler should use a JsonFormatter. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        handler = self.logger.handlers[0]

        self.assertIsInstance(handler.formatter, JsonFormatter)

    def test_file_handler_uses_prefix_formatter(self):
        """ The FileHandler should use a PrefixFormatter. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        handler = self.logger.handlers[1]

        self.assertIsInstance(handler.formatter, PrefixFormatter)

    def test_creates_log_file(self):
        """ A log file should be created for the logger. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

            self.assertTrue(Path(tmpdir, "test.log").exists())

    def test_uses_current_directory_by_default(self):
        """ The current directory should be used when no log directory is specified. """

        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = Path.cwd()

            try:
                import os
                os.chdir(tmpdir)

                LokiLoggerBackend.configure(
                    logger=self.logger,
                    level=logging.INFO,
                    prefix="APP",
                    text_log_directory=None,
                )

                self.assertTrue(Path("test.log").exists())

            finally:
                os.chdir(cwd)

    def test_disables_propagation(self):
        """ Logger propagation should be disabled. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        self.assertFalse(self.logger.propagate)

    def test_does_not_add_duplicate_handlers(self):
        """ Existing handlers should not be duplicated. """

        with tempfile.TemporaryDirectory() as tmpdir:
            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

            LokiLoggerBackend.configure(
                logger=self.logger,
                level=logging.INFO,
                prefix="APP",
                text_log_directory=tmpdir,
            )

        self.assertEqual(len(self.logger.handlers), 2)
