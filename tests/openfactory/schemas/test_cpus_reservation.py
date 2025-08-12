import unittest
from openfactory.schemas.common import cpus_reservation, Deploy, Resources, ResourcesDefinition


class TestCpusReservation(unittest.TestCase):
    """
    Test class for the cpus_reservation function
    """

    def test_none_deploy_returns_default(self):
        """ Test that None deploy returns the default value """
        self.assertEqual(cpus_reservation(None), 0.5)
        self.assertEqual(cpus_reservation(None, default=0.7), 0.7)

    def test_deploy_without_resources_returns_default(self):
        """ Test deploy with no resources returns the default """
        deploy = Deploy(resources=None)
        self.assertEqual(cpus_reservation(deploy), 0.5)

    def test_deploy_with_resources_without_reservations_returns_default(self):
        """ Test deploy with resources but no reservations returns the default """
        resources = Resources(reservations=None, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_reservation(deploy), 0.5)

    def test_deploy_with_reservations_without_cpus_returns_default(self):
        """ Test deploy with reservations but cpus is None returns the default """
        reservations = ResourcesDefinition(cpus=None)
        resources = Resources(reservations=reservations, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_reservation(deploy), 0.5)

    def test_deploy_with_reservations_with_cpus_returns_value(self):
        """ Test deploy with reservations and cpus returns the cpus value """
        reservations = ResourcesDefinition(cpus=0.75)
        resources = Resources(reservations=reservations, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_reservation(deploy), 0.75)

    def test_deploy_with_reservations_with_cpus_and_custom_default(self):
        """ Test deploy with no cpus but a custom default returns that default """
        reservations = ResourcesDefinition(cpus=None)
        resources = Resources(reservations=reservations, limits=None)
        deploy = Deploy(resources=resources)
        self.assertEqual(cpus_reservation(deploy, default=0.9), 0.9)
