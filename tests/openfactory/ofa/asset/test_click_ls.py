import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from openfactory.ofa.asset.ls import click_ls


class TestClickAssetLsCommand(unittest.TestCase):
    """
    Unit tests for ofa.asset.ls.click_ls command
    """

    @patch("openfactory.ofa.asset.ls.OpenFactory")
    def test_click_ls_outputs_asset(self, MockOpenFactory):
        """
        Test click_ls outputs
        """
        mock_asset = MagicMock()
        mock_asset.asset_uuid = "mtc-001"
        mock_asset.type = "MTConnectAgent"
        mock_asset.agent_avail.value = "AVAILABLE"
        mock_asset.DockerService.value = "mtc-container"

        instance = MockOpenFactory.return_value
        instance.assets.return_value = [mock_asset]

        runner = CliRunner()
        result = runner.invoke(click_ls)

        self.assertEqual(result.exit_code, 0)
        self.assertIn("mtc-001", result.output)
        self.assertIn("AVAILABLE", result.output)
        self.assertIn("MTConnectAgent", result.output)
        self.assertIn("mtc-container", result.output)

    @patch("openfactory.ofa.asset.ls.OpenFactory")
    def test_click_ls_handles_empty_assets(self, MockOpenFactory):
        """
        Test click_ls handles case when OpenFactory.assets returns empty list
        """
        instance = MockOpenFactory.return_value
        instance.assets.return_value = []

        runner = CliRunner()
        result = runner.invoke(click_ls)

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Deployed Assets", result.output)
        self.assertNotIn("AVAILABLE", result.output)
        self.assertNotIn("UNAVAILABLE", result.output)
