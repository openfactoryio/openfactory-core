import unittest
from openfactory.schemas.common import cpus_limit, Deploy, Resources, ResourcesDefinition


class TestCpusLimit(unittest.TestCase):
    """
    Test class for the cpus_limit function
    """

    def test_none_deploy_returns_default(self):
        """ Test that None deploy returns the default value """
        self.assertEqual(cpus_limit(None), 1.0)
        self.assertEqual(cpus_limit(None, default=2.0), 2.0)

    def test_deploy_without_resources_returns_default(self):
        """ Test deploy with no resources returns the default """
        deploy = Deploy(resources=None)
        self.assertEqual(cpus_limit(deploy), 1.0)

    def test_deploy_with_resources_without_limits_returns_default(self):
        """ Test deploy with resources but no limits returns the default """
        resources = Resources(reservations=None, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_limit(deploy), 1.0)

    def test_deploy_with_limits_without_cpus_returns_default(self):
        """ Test deploy with limits but cpus is None returns the default """
        limits = ResourcesDefinition(cpus=None)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_limit(deploy), 1.0)

    def test_deploy_with_limits_with_cpus_returns_value(self):
        """ Test deploy with limits and cpus returns the cpus value """
        limits = ResourcesDefinition(cpus=1.25)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_limit(deploy), 1.25)

    def test_deploy_with_limits_with_cpus_and_custom_default(self):
        """ Test deploy with no cpus but a custom default returns that default """
        limits = ResourcesDefinition(cpus=None)
        resources = Resources(reservations=None, limits=limits)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_limit(deploy, default=2.5), 2.5)
