import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from openfactory.ofa.device.ls import click_ls


class TestClickDeviceLsCommand(unittest.TestCase):
    """
    Unit tests for ofa.device.ls.click_ls command
    """

    @patch("openfactory.ofa.device.ls.OpenFactory")
    def test_click_ls_outputs_device_info(self, MockOpenFactory):
        """
        Test click_ls outputs device info including availability and services
        """
        mock_device = MagicMock()
        mock_device.asset_uuid = "dev-123"
        mock_device.avail.value = "AVAILABLE"
        mock_device.references_below_uuid.return_value = {
            "dev-123-AGENT",
            "dev-123-PRODUCER",
            "dev-123-SUPERVISOR"
        }

        instance = MockOpenFactory.return_value
        instance.devices.return_value = [mock_device]

        runner = CliRunner()
        result = runner.invoke(click_ls)

        self.assertEqual(result.exit_code, 0)
        self.assertIn("dev-123", result.output)
        self.assertIn("AVAILABLE", result.output)
        self.assertIn("dev-123-AGENT", result.output)
        self.assertIn("dev-123-PROD", result.output)
        self.assertIn("dev-123-SUP", result.output)
        self.assertNotIn("NOT DEPLOYED", result.output)

    @patch("openfactory.ofa.device.ls.OpenFactory")
    def test_click_ls_handles_missing_services(self, MockOpenFactory):
        """
        Test click_ls handles devices with missing MTConnect, Kafka, and Supervisor
        """
        mock_device = MagicMock()
        mock_device.asset_uuid = "dev-456"
        mock_device.avail.value = "UNAVAILABLE"
        mock_device.references_below_uuid.return_value = set()  # none deployed

        instance = MockOpenFactory.return_value
        instance.devices.return_value = [mock_device]

        runner = CliRunner()
        result = runner.invoke(click_ls)

        self.assertEqual(result.exit_code, 0)
        self.assertIn("dev-456", result.output)
        self.assertIn("UNAVAILABLE", result.output)
        self.assertIn("NOT DEPLOYED", result.output)
        self.assertIn("Deployed Devices", result.output)

    @patch("openfactory.ofa.device.ls.OpenFactory")
    def test_click_ls_handles_empty_device_list(self, MockOpenFactory):
        """
        Test click_ls handles case when OpenFactory.devices returns empty list
        """
        instance = MockOpenFactory.return_value
        instance.devices.return_value = []

        runner = CliRunner()
        result = runner.invoke(click_ls)

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Deployed Devices", result.output)
        self.assertNotIn("AVAILABLE", result.output)
        self.assertNotIn("UNAVAILABLE", result.output)
