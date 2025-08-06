import unittest
from unittest.mock import patch
from click.testing import CliRunner
from openfactory.ofa.asset.inspect import click_inspect


class TestClickAssetInspectCommand(unittest.TestCase):
    """
    Unit tests for ofa.asset.inspect.click_inspect command
    """

    @patch("openfactory.ofa.asset.inspect.Asset")
    def test_click_inspect_outputs_all_data(self, MockAsset):
        """
        Test click_inspect outputs samples, events, and conditions from asset
        """
        mock_instance = MockAsset.return_value
        mock_instance.uns_id.value = 'uns/path'
        mock_instance.samples.return_value = [
            {'ID': 'temp1', 'VALUE': '85', 'TAG': 'Temperature'}
        ]
        mock_instance.events.return_value = [
            {'ID': 'start1', 'VALUE': 'RUN', 'TAG': 'Execution'}
        ]
        mock_instance.conditions.return_value = [
            {'ID': 'cond1', 'VALUE': 'NORMAL', 'TAG': 'System'}
        ]

        runner = CliRunner()
        result = runner.invoke(click_inspect, ['asset-001'])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("asset-001", result.output)
        self.assertIn("uns/path", result.output)
        self.assertIn("temp1", result.output)
        self.assertIn("85", result.output)
        self.assertIn("Sample", result.output)

        self.assertIn("start1", result.output)
        self.assertIn("RUN", result.output)
        self.assertIn("Events", result.output)

        self.assertIn("cond1", result.output)
        self.assertIn("NORMAL", result.output)
        self.assertIn("Condition", result.output)

    @patch("openfactory.ofa.asset.inspect.Asset")
    def test_click_inspect_handles_empty_asset_data(self, MockAsset):
        """
        Test click_inspect handles asset with no samples, events, or conditions
        """
        mock_instance = MockAsset.return_value
        mock_instance.samples.return_value = []
        mock_instance.events.return_value = []
        mock_instance.conditions.return_value = []

        runner = CliRunner()
        result = runner.invoke(click_inspect, ['asset-002'])

        self.assertEqual(result.exit_code, 0)
        self.assertIn("asset-002", result.output)
        self.assertNotIn("Sample", result.output)
        self.assertNotIn("Events", result.output)
        self.assertNotIn("Condition", result.output)

    @patch("openfactory.ofa.asset.inspect.Asset")
    def test_click_inspect_asset_argument_required(self, MockAsset):
        """
        Test click_inspect errors when asset_uuid argument is missing
        """
        runner = CliRunner()
        result = runner.invoke(click_inspect, [])

        self.assertEqual(result.exit_code, 2)
        self.assertIn("Error: Missing argument 'ASSET_UUID'", result.output)
