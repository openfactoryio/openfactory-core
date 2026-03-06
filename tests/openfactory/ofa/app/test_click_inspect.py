import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from openfactory.ofa.asset.inspect import click_inspect


class TestClickInspectCommand(unittest.TestCase):
    """
    Test ofa asset inspect command
    """

    @patch("openfactory.ofa.asset.inspect.Asset")
    def test_click_inspect_outputs_asset_data(self, MockAsset):
        """ Test click_inspect prints samples, events and conditions """
        mock_asset = MagicMock()
        mock_asset.uns_id.value = "DEM-001"
        mock_asset.samples.return_value = [
            {"ID": "temperature", "VALUE": "21.5", "TAG": "Sensor.Temp"}
        ]
        mock_asset.events.return_value = [
            {"ID": "avail", "VALUE": "AVAILABLE", "TAG": "Availability"}
        ]
        mock_asset.conditions.return_value = [
            {"ID": "alarm", "VALUE": "FALSE", "TAG": "Alarm.State"}
        ]
        MockAsset.return_value = mock_asset

        runner = CliRunner()
        result = runner.invoke(click_inspect, ["asset-123"])

        self.assertEqual(result.exit_code, 0)

        # samples
        self.assertIn("temperature", result.output)
        self.assertIn("21.5", result.output)

        # events
        self.assertIn("AVAILABLE", result.output)
        self.assertIn("Availability", result.output)

        # conditions
        self.assertIn("alarm", result.output)
        self.assertIn("FALSE", result.output)

    @patch("openfactory.ofa.asset.inspect.Asset")
    def test_click_inspect_handles_empty_asset(self, MockAsset):
        """ Test click_inspect when asset has no samples/events/conditions """

        mock_asset = MagicMock()
        mock_asset.uns_id.value = "DEM-001"

        mock_asset.samples.return_value = []
        mock_asset.events.return_value = []
        mock_asset.conditions.return_value = []

        MockAsset.return_value = mock_asset

        runner = CliRunner()

        result = runner.invoke(click_inspect, ["asset-123"])

        self.assertEqual(result.exit_code, 0)

        # table header should still exist
        self.assertIn("DEM-001", result.output)

        # ensure no data rows
        self.assertNotIn("temperature", result.output)
        self.assertNotIn("AVAILABLE", result.output)
