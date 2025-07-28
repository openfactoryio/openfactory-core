import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from openfactory.ofa.app.ls import click_ls


class TestClickLsCommand(unittest.TestCase):
    @patch("openfactory.ofa.app.ls.OpenFactory")
    def test_click_ls_outputs_applications(self, MockOpenFactory):
        """
        Test click_ls outputs deployed application data from OpenFactory
        """
        # Arrange: Create a mock app
        mock_app = MagicMock()
        mock_app.asset_uuid = "asset-123"
        mock_app.avail.value = "AVAILABLE"
        mock_app.application_manufacturer.value = "TestVendor"
        mock_app.application_version.value = "2.1.0"
        mock_app.application_license.value = "Apache-2.0"

        # Mock the return value of OpenFactory.applications()
        instance = MockOpenFactory.return_value
        instance.applications.return_value = [mock_app]

        # Act: Run the Click command
        runner = CliRunner()
        result = runner.invoke(click_ls)

        # Assert: Output and behavior
        self.assertEqual(result.exit_code, 0)
        self.assertIn("asset-123", result.output)
        self.assertIn("AVAILABLE", result.output)
        self.assertIn("TestVendor", result.output)
        self.assertIn("2.1.0", result.output)
        self.assertIn("Apache-2.0", result.output)

    @patch("openfactory.ofa.app.ls.OpenFactory")
    def test_click_ls_handles_empty_applications(self, MockOpenFactory):
        """
        Test click_ls handles case when OpenFactory.applications returns empty list
        """
        # Arrange: No apps returned
        instance = MockOpenFactory.return_value
        instance.applications.return_value = []

        # Act
        runner = CliRunner()
        result = runner.invoke(click_ls)

        # Assert: Should still succeed but no app rows
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Deployed Apps", result.output)
        # Possibly no data rows in output
        self.assertNotIn("AVAILABLE", result.output)
