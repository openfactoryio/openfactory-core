import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from openfactory.ofa.asset.methods import click_methods


class TestClickMethodsCommand(unittest.TestCase):
    """
    Test ofa asset methods command
    """

    @patch("openfactory.ofa.asset.methods.Asset")
    def test_click_methods_outputs_methods(self, MockAsset):
        """ Test outputs """
        mock_asset = MagicMock()
        mock_asset.uns_id.value = "DEM-001"
        mock_asset.methods.return_value = {
            "move_axis": {
                "description": "Move to a given (x, y) position with speed",
                "arguments": [
                    {"name": "x", "description": "X coordinate"},
                    {"name": "y", "description": "Y coordinate"},
                    {"name": "speed", "description": "Feed rate (optional; defaults to 100)"}
                ]
            },
            "stop_axis": {
                "description": "Stops all motion immediately.",
                "arguments": []
            }
        }

        MockAsset.return_value = mock_asset

        runner = CliRunner()
        result = runner.invoke(click_methods, ["asset-123"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("move_axis", result.output)
        self.assertIn("stop_axis", result.output)
        self.assertIn("X coordinate", result.output)
        self.assertIn("—", result.output)  # for no-argument method

    @patch("openfactory.ofa.asset.methods.Asset")
    def test_click_methods_empty(self, MockAsset):
        """ Test click_methods handle empty when no method is defined. """
        mock_asset = MagicMock()
        mock_asset.uns_id.value = "DEM-001"
        mock_asset.methods.return_value = {}
        MockAsset.return_value = mock_asset

        runner = CliRunner()
        result = runner.invoke(click_methods, ["asset-123"])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("DEM-001", result.output)
        self.assertNotIn("move_axis", result.output)
