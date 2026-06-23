import unittest
from openfactory.schemas.common import open_files_limit, Deploy, Resources, ResourcesDefinition


class TestOpenFilesLimit(unittest.TestCase):
    """
    Test class for the open_files_limit function
    """

    def test_none_deploy_returns_default(self):
        """ Test that None deploy returns the default value """
        self.assertEqual(open_files_limit(None), None)
        self.assertEqual(open_files_limit(None, default=65535), 65535)

    def test_deploy_without_resources_returns_default(self):
        """ Test deploy with no resources returns the default """
        deploy = Deploy(resources=None)
        self.assertEqual(open_files_limit(deploy), None)

    def test_deploy_with_resources_without_limits_returns_default(self):
        """ Test deploy with resources but no limits returns the default """
        resources = Resources(reservations=None, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(open_files_limit(deploy), None)

    def test_deploy_with_limits_without_open_files_returns_default(self):
        """ Test deploy with limits but open_files is None returns the default """
        limits = ResourcesDefinition(open_files=None)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(open_files_limit(deploy), None)

    def test_deploy_with_limits_with_open_files_returns_value(self):
        """ Test deploy with limits and open_files returns the configured value """
        limits = ResourcesDefinition(open_files=65535)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(open_files_limit(deploy), 65535)

    def test_deploy_with_limits_without_open_files_and_custom_default(self):
        """ Test deploy with no open_files but a custom default returns that default """
        limits = ResourcesDefinition(open_files=None)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(open_files_limit(deploy, default=4096), 4096)
