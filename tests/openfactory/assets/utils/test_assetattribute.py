import unittest
from unittest.mock import patch
from datetime import datetime
from openfactory.assets.utils import AssetAttribute


class TestAssetAttribute(unittest.TestCase):
    """
    Test class for AssetAttribute
    """

    def setUp(self):
        # Freeze datetime for deterministic AssetAttribute.timestamp
        self.fixed_ts = datetime(2023, 1, 1, 12, 0, 0)

        datetime_patcher = patch("openfactory.assets.utils.time_methods.datetime")
        self.mock_datetime = datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # Make datetime.now() return fixed timestamp
        self.mock_datetime.now.return_value = self.fixed_ts
        # Allow datetime(...) constructor to still work
        self.mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

    def test_valid_initialization_with_string_value(self):
        """ Test AssetAttribute initialization with string value """
        attr = AssetAttribute(
            id="attr1",
            value="ON",
            type="Samples",
            tag="status"
        )
        self.assertEqual(attr.id, "attr1")
        self.assertEqual(attr.value, "ON")
        self.assertEqual(attr.type, "Samples")
        self.assertEqual(attr.tag, "status")
        # Expect OpenFactory format with milliseconds + Z
        self.assertEqual(attr.timestamp, "2023-01-01T12:00:00.000Z")

    def test_valid_initialization_with_float_value(self):
        """ Test AssetAttribute initialization with float value """
        attr = AssetAttribute(
            id="attr2",
            value=42.0,
            type="Condition",
            tag="temperature"
        )
        self.assertEqual(attr.value, 42.0)
        self.assertEqual(attr.type, "Condition")
        self.assertEqual(attr.timestamp, "2023-01-01T12:00:00.000Z")

    def test_invalid_type_raises_value_error(self):
        """ Test invalid type raises ValueError """
        with self.assertRaises(ValueError) as ctx:
            AssetAttribute(
                id="attr3",
                value="bad",
                type="InvalidType",
                tag="test"
            )
        self.assertIn("Invalid type", str(ctx.exception))

    def test_custom_timestamp_is_preserved(self):
        """ Test custom timestamp overrides default """
        ts = "2025-10-01T12:34:56.000Z"
        attr = AssetAttribute(
            id="attr4",
            value="ok",
            type="Events",
            tag="event-tag",
            timestamp=ts
        )
        self.assertEqual(attr.timestamp, ts)
