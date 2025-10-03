import unittest
from unittest.mock import patch
from datetime import datetime, timezone

from openfactory.assets.utils.time_methods import openfactory_timestamp, current_timestamp


class TestTimeMethods(unittest.TestCase):
    """
    Test class for time utility functions:
    - openfactory_timestamp
    - current_timestamp
    """

    def setUp(self):
        """ Set up a fixed datetime for deterministic tests. """
        self.fixed_dt = datetime(2025, 5, 4, 12, 34, 56, 789000, tzinfo=timezone.utc)
        self.expected_str = "2025-05-04T12:34:56.789Z"

    def test_openfactory_timestamp_valid(self):
        """ Test conversion of datetime to OpenFactory timestamp format. """
        result = openfactory_timestamp(self.fixed_dt)
        self.assertEqual(result, self.expected_str)

    def test_openfactory_timestamp_with_different_time(self):
        """ Test conversion works with different datetime values. """
        dt = datetime(2000, 1, 1, 0, 0, 0, 123000, tzinfo=timezone.utc)
        result = openfactory_timestamp(dt)
        self.assertEqual(result, "2000-01-01T00:00:00.123Z")

    @patch("openfactory.assets.utils.time_methods.datetime")
    def test_current_timestamp_mocked(self, mock_datetime):
        """ Test current_timestamp returns fixed mocked datetime in correct format. """
        mock_datetime.now.return_value = self.fixed_dt
        mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

        result = current_timestamp()
        self.assertEqual(result, self.expected_str)

    def test_current_timestamp_real(self):
        """ Test current_timestamp returns a string ending with Z and millisecond precision. """
        ts = current_timestamp()
        self.assertTrue(ts.endswith("Z"))
        self.assertRegex(ts, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
