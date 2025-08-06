import unittest
from unittest.mock import MagicMock
from openfactory.schemas.uns import AttachUNSMixin


class DummyAsset(AttachUNSMixin):
    def __init__(self, uuid, uns):
        self.uuid = uuid
        self.uns = uns


class TestAttachUNSMixin(unittest.TestCase):
    """
    Unit tests for AttachUNSMixin
    """
    def test_attach_uns_success_with_dict(self):
        """ Test attaching UNS when `uns` is a valid dict """
        asset = DummyAsset(uuid="1234", uns={"foo": "bar"})
        schema = MagicMock()
        expected_levels = {"level_a": "value"}
        schema.extract_uns_fields.return_value = expected_levels
        schema.generate_uns_path.return_value = "uns/path/1234"

        asset.attach_uns(schema)

        schema.extract_uns_fields.assert_called_once_with(asset_uuid="1234", uns_dict={"foo": "bar"})
        schema.generate_uns_path.assert_called_once_with(expected_levels)
        self.assertEqual(
            asset.uns,
            {
                "levels": expected_levels,
                "uns_id": "uns/path/1234"
            }
        )

    def test_attach_uns_with_non_dict_uns_converted_to_empty(self):
        """ Test attaching UNS when `uns` is None (should convert to empty dict) """
        asset = DummyAsset(uuid="uuid-xyz", uns=None)
        schema = MagicMock()
        expected_levels = {"x": 1}
        schema.extract_uns_fields.return_value = expected_levels
        schema.generate_uns_path.return_value = "some/path"

        asset.attach_uns(schema)

        schema.extract_uns_fields.assert_called_once_with(asset_uuid="uuid-xyz", uns_dict={})
        schema.generate_uns_path.assert_called_once_with(expected_levels)
        self.assertEqual(asset.uns["levels"], expected_levels)
        self.assertEqual(asset.uns["uns_id"], "some/path")

    def test_attach_uns_validation_error_propagates(self):
        """ Test that a ValueError from schema extraction is propagated """
        asset = DummyAsset(uuid="uuid-err", uns={"bad": "data"})
        schema = MagicMock()
        schema.extract_uns_fields.side_effect = ValueError("invalid uns")
        with self.assertRaises(ValueError) as ctx:
            asset.attach_uns(schema)
        self.assertIn("invalid uns", str(ctx.exception))
