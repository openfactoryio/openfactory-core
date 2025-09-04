import unittest
from datetime import datetime, timezone, timedelta
from openfactory.assets.utils import openfactory_timestamp


class TestOpenFactoryTimestamp(unittest.TestCase):
    """
    Test for openfactory_timestamp method
    """

    def test_basic_utc_timestamp(self):
        """ Check formatting of a standard UTC datetime with milliseconds. """
        dt = datetime(2025, 5, 4, 12, 34, 56, 789000, tzinfo=timezone.utc)
        result = openfactory_timestamp(dt)
        expected = "2025-05-04T12:34:56.789Z"
        self.assertEqual(result, expected)

    def test_truncate_microseconds_to_milliseconds(self):
        """ Ensure microseconds are correctly truncated to milliseconds. """
        dt = datetime(2025, 5, 4, 12, 34, 56, 789123, tzinfo=timezone.utc)
        result = openfactory_timestamp(dt)
        expected = "2025-05-04T12:34:56.789Z"
        self.assertEqual(result, expected)

    def test_naive_datetime_assumed_as_utc(self):
        """ Verify that naive datetime objects are formatted with 'Z' suffix. """
        dt = datetime(2025, 5, 4, 12, 34, 56, 123456)  # naive datetime
        result = openfactory_timestamp(dt)
        expected = "2025-05-04T12:34:56.123Z"
        self.assertEqual(result, expected)

    def test_different_timezone(self):
        """ Confirm that timezone-aware datetimes are formatted as-is with 'Z'. """
        dt = datetime(2025, 5, 4, 15, 34, 56, 987654, tzinfo=timezone(timedelta(hours=3)))
        result = openfactory_timestamp(dt)
        expected = "2025-05-04T15:34:56.987Z"
        self.assertEqual(result, expected)
