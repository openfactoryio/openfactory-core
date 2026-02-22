import unittest
from unittest.mock import patch
from openfactory.schemas.common import resources, Deploy, Resources, ResourcesDefinition, parse_memory_to_bytes


class TestResources(unittest.TestCase):
    """
    Test class for the `resources` helper function using Deploy objects.
    """

    def test_none_deploy_returns_none(self):
        """ Test that passing None deploy returns None. """
        self.assertIsNone(resources(None))

    def test_empty_resources_returns_none(self):
        """ Test Deploy with resources=None returns None. """
        deploy = Deploy(resources=None)
        self.assertIsNone(resources(deploy))

    def test_only_limits_cpus(self):
        """ Test Deploy with only CPU limits. """
        res = Resources(limits=ResourcesDefinition(cpus=0.5))
        deploy = Deploy(resources=res)
        expected = {"Limits": {"NanoCPUs": 500_000_000}, "Reservations": {}}
        self.assertEqual(resources(deploy), expected)

    @patch("openfactory.schemas.common.parse_memory_to_bytes", wraps=parse_memory_to_bytes)
    def test_only_limits_memory(self, mock_parse):
        """ Test Deploy with only memory limits and ensure parse_memory_to_bytes is called. """
        res = Resources(limits=ResourcesDefinition(memory="1Gi"))
        deploy = Deploy(resources=res)
        expected = {"Limits": {"MemoryBytes": 1024**3}, "Reservations": {}}
        result = resources(deploy)
        self.assertEqual(result, expected)
        mock_parse.assert_called_once_with("1Gi")

    @patch("openfactory.schemas.common.parse_memory_to_bytes", wraps=parse_memory_to_bytes)
    def test_only_reservations_memory(self, mock_parse):
        """ Test Deploy with only memory reservations and ensure parse_memory_to_bytes is called. """
        res = Resources(reservations=ResourcesDefinition(memory="512Mi"))
        deploy = Deploy(resources=res)
        expected = {"Limits": {}, "Reservations": {"MemoryBytes": 512*1024**2}}
        result = resources(deploy)
        self.assertEqual(result, expected)
        mock_parse.assert_called_once_with("512Mi")

    def test_only_reservations_cpus(self):
        """ Test Deploy with only CPU reservations. """
        res = Resources(reservations=ResourcesDefinition(cpus=0.25))
        deploy = Deploy(resources=res)
        expected = {"Limits": {}, "Reservations": {"NanoCPUs": 250_000_000}}
        self.assertEqual(resources(deploy), expected)

    @patch("openfactory.schemas.common.parse_memory_to_bytes", wraps=parse_memory_to_bytes)
    def test_limits_and_reservations(self, mock_parse):
        """ Test Deploy with both limits and reservations for CPU and memory. """
        res = Resources(
            limits=ResourcesDefinition(cpus=1.0, memory="1Gi"),
            reservations=ResourcesDefinition(cpus=0.5, memory="512Mi")
        )
        deploy = Deploy(resources=res)
        expected = {
            "Limits": {"NanoCPUs": 1_000_000_000, "MemoryBytes": 1024**3},
            "Reservations": {"NanoCPUs": 500_000_000, "MemoryBytes": 512*1024**2}
        }
        result = resources(deploy)
        self.assertEqual(result, expected)
        mock_parse.assert_any_call("1Gi")
        mock_parse.assert_any_call("512Mi")
        self.assertEqual(mock_parse.call_count, 2)

    @patch("openfactory.schemas.common.parse_memory_to_bytes", wraps=parse_memory_to_bytes)
    def test_partial_none_values(self, mock_parse):
        """ Test Deploy where some fields are None are ignored and parse_memory_to_bytes is only called for non-None memory. """
        res = Resources(
            limits=ResourcesDefinition(cpus=None, memory="1Gi"),
            reservations=ResourcesDefinition(cpus=0.5, memory=None)
        )
        deploy = Deploy(resources=res)
        expected = {
            "Limits": {"MemoryBytes": 1024**3},
            "Reservations": {"NanoCPUs": 500_000_000}
        }
        result = resources(deploy)
        self.assertEqual(result, expected)
        mock_parse.assert_called_once_with("1Gi")
