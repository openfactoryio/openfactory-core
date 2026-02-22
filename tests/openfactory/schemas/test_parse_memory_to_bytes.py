import unittest
from openfactory.schemas.common import parse_memory_to_bytes


class TestParseMemoryToBytes(unittest.TestCase):
    """
    Test class for the parse_memory_to_bytes function
    """

    def test_bytes_without_unit(self):
        """ Test that a numeric string without unit returns integer bytes """
        self.assertEqual(parse_memory_to_bytes("1024"), 1024)
        self.assertEqual(parse_memory_to_bytes("  2048  "), 2048)

    def test_bytes_lowercase_units(self):
        """ Test memory strings with lowercase units """
        self.assertEqual(parse_memory_to_bytes("1b"), 1)
        self.assertEqual(parse_memory_to_bytes("1k"), 1024)
        self.assertEqual(parse_memory_to_bytes("1kb"), 1024)
        self.assertEqual(parse_memory_to_bytes("1m"), 1024**2)
        self.assertEqual(parse_memory_to_bytes("1mi"), 1024**2)
        self.assertEqual(parse_memory_to_bytes("1g"), 1024**3)
        self.assertEqual(parse_memory_to_bytes("1gi"), 1024**3)

    def test_bytes_uppercase_units(self):
        """ Test memory strings with uppercase units """
        self.assertEqual(parse_memory_to_bytes("1K"), 1024)
        self.assertEqual(parse_memory_to_bytes("1MB"), 1024**2)
        self.assertEqual(parse_memory_to_bytes("1G"), 1024**3)
        self.assertEqual(parse_memory_to_bytes("1Gi"), 1024**3)

    def test_floating_point_values(self):
        """ Test floating point values with units """
        self.assertEqual(parse_memory_to_bytes("0.5Gi"), int(0.5 * 1024**3))
        self.assertEqual(parse_memory_to_bytes("1.5Mi"), int(1.5 * 1024**2))
        self.assertEqual(parse_memory_to_bytes("2.25Gb"), int(2.25 * 1024**3))

    def test_whitespace_and_case(self):
        """ Test memory strings with extra whitespace and mixed case """
        self.assertEqual(parse_memory_to_bytes(" 512Mi "), 512 * 1024**2)
        self.assertEqual(parse_memory_to_bytes(" 1Gi "), 1024**3)
        self.assertEqual(parse_memory_to_bytes("0.25Gi"), int(0.25 * 1024**3))

    def test_invalid_numeric_string_raises(self):
        """ Test invalid strings raise ValueError """
        with self.assertRaises(ValueError):
            parse_memory_to_bytes("abc")
        with self.assertRaises(ValueError):
            parse_memory_to_bytes("1XYZ")
