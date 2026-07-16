import io
import json
import logging
import os
import tempfile
import unittest
from unittest.mock import patch
from openfactory.logging.configure import configure_logger


class TestLoggingIntegration(unittest.TestCase):
    """ Integration tests for the OpenFactory logging module. """

    def tearDown(self):
        logging.Logger.manager.loggerDict.clear()

    @patch.dict("os.environ", {"OPENFACTORY_LOG_BACKEND": "text"}, clear=True)
    def test_text_backend_output(self):
        """ The text backend should produce human-readable log output. """

        stream = io.StringIO()

        with patch("sys.stderr", stream):
            logger = configure_logger("test")
            logger.info("Hello")

        output = stream.getvalue()

        self.assertIn("[APP]", output)
        self.assertIn("INFO", output)
        self.assertIn("Hello", output)

    @patch.dict("os.environ", {"OPENFACTORY_LOG_BACKEND": "loki"}, clear=True)
    def test_loki_backend_output(self):
        """ The Loki backend should produce structured JSON log output. """

        stream = io.StringIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                patch("sys.stderr", stream),
                patch.dict(
                    os.environ,
                    {
                        "OPENFACTORY_LOG_BACKEND": "loki",
                        "OPENFACTORY_TEXT_LOG_DIRECTORY": tmpdir,
                    },
                    clear=True,
                ),
            ):
                logger = configure_logger("test")

                logger.info(
                    "Hello",
                    extra={
                        "asset_uuid": "DEVICE-1",
                        "temperature": 42.5,
                    },
                )

        payload = json.loads(stream.getvalue())

        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["logger"], "test")
        self.assertEqual(payload["message"], "Hello")
        self.assertEqual(payload["asset_uuid"], "DEVICE-1")
        self.assertEqual(payload["temperature"], 42.5)
        self.assertIn("timestamp", payload)
