import json
import logging
import unittest
from openfactory.logging.formatters import PrefixFormatter, JsonFormatter


class TestPrefixFormatter(unittest.TestCase):
    """ Tests for PrefixFormatter. """

    def test_default_format(self):
        """ The default format should prepend the prefix. """

        formatter = PrefixFormatter("APP")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        formatted = formatter.format(record)

        self.assertTrue(formatted.startswith("[APP] ("))
        self.assertIn("INFO", formatted)
        self.assertTrue(formatted.endswith("Hello"))

    def test_custom_format(self):
        """ A custom format should be supported. """

        formatter = PrefixFormatter("APP", fmt="%(levelname)s: %(message)s")

        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        self.assertEqual(formatter.format(record), "[APP] WARNING: Hello")


class TestJsonFormatter(unittest.TestCase):
    """ Tests for JsonFormatter. """

    def test_basic_fields(self):
        """ Standard log fields should be serialized. """

        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        payload = json.loads(formatter.format(record))

        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["logger"], "mylogger")
        self.assertEqual(payload["message"], "Hello")
        self.assertIn("timestamp", payload)

    def test_timestamp_is_iso8601(self):
        """ The timestamp should be formatted as an ISO-8601 UTC timestamp. """

        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        payload = json.loads(formatter.format(record))

        self.assertIn("T", payload["timestamp"])
        self.assertTrue(payload["timestamp"].endswith("+00:00"))

    def test_serializes_extra_fields(self):
        """ User-defined fields should be serialized. """

        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        record.asset_uuid = "DEVICE-1"
        record.temperature = 42.5

        payload = json.loads(formatter.format(record))

        self.assertEqual(payload["asset_uuid"], "DEVICE-1")
        self.assertEqual(payload["temperature"], 42.5)

    def test_standard_fields_are_not_serialized(self):
        """ Standard LogRecord fields should not be serialized. """

        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Hello",
            args=(),
            exc_info=None,
        )

        payload = json.loads(formatter.format(record))

        self.assertNotIn("pathname", payload)
        self.assertNotIn("filename", payload)
        self.assertNotIn("module", payload)
        self.assertNotIn("lineno", payload)
        self.assertNotIn("funcName", payload)

    def test_serializes_exception(self):
        """ Exception information should be serialized. """

        formatter = JsonFormatter()

        try:
            raise RuntimeError("Boom")
        except RuntimeError:
            exc_info = logging.sys.exc_info()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="Failed",
            args=(),
            exc_info=exc_info,
        )

        payload = json.loads(formatter.format(record))

        self.assertEqual(payload["message"], "Failed")
        self.assertIn("exception", payload)
        self.assertIn("RuntimeError", payload["exception"])

    def test_message_arguments_are_formatted(self):
        """ Message arguments should be formatted before serialization. """

        formatter = JsonFormatter()

        record = logging.LogRecord(
            name="mylogger",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Temperature=%d",
            args=(42,),
            exc_info=None,
        )

        payload = json.loads(formatter.format(record))

        self.assertEqual(payload["message"], "Temperature=42")
