import unittest
from unittest.mock import patch, MagicMock
from openfactory.utils import load_plugin


class TestLoadPlugin(unittest.TestCase):
    """
    Test cases for the load_plugin utility function
    """

    @patch("openfactory.utils.load_plugin.entry_points")
    def test_load_plugin_success(self, mock_entry_points):
        """ Test successfully loading a plugin by name from an entry point group """
        # Mock entry point
        mock_ep = MagicMock()
        mock_ep.name = "swarm"
        mock_loaded_class = MagicMock()
        mock_ep.load.return_value = mock_loaded_class

        # Mock entry_points().select(group=...) to return our mocked entry point
        mock_entry_points.return_value.select.return_value = [mock_ep]

        result = load_plugin("openfactory.deployment_platforms", "swarm")

        mock_ep.load.assert_called_once()
        self.assertEqual(result, mock_loaded_class)

    @patch("openfactory.utils.load_plugin.entry_points")
    def test_load_plugin_not_found(self, mock_entry_points):
        """ Test that ValueError is raised if the plugin name is not found in the group """
        # No matching entry point
        mock_ep = MagicMock()
        mock_ep.name = "docker"
        mock_entry_points.return_value.select.return_value = [mock_ep]

        with self.assertRaises(ValueError) as ctx:
            load_plugin("openfactory.deployment_platforms", "swarm")

        self.assertIn("No entry point named 'swarm'", str(ctx.exception))

    @patch("openfactory.utils.load_plugin.entry_points")
    def test_load_plugin_empty_group(self, mock_entry_points):
        """ Test that ValueError is raised if the group has no entry points. """
        mock_entry_points.return_value.select.return_value = []

        with self.assertRaises(ValueError):
            load_plugin("openfactory.deployment_platforms", "swarm")
