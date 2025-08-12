import unittest
from openfactory.schemas.common import constraints, Deploy, Placement


class TestConstraints(unittest.TestCase):
    """
    Test class for the constraints function
    """

    def test_none_deploy_returns_none(self):
        """ Test that None deploy returns None """
        self.assertIsNone(constraints(None))

    def test_deploy_without_placement_returns_none(self):
        "" "Test deploy with no placement returns None """
        deploy = Deploy(placement=None)
        self.assertIsNone(constraints(deploy))

    def test_deploy_with_placement_without_constraints_returns_none(self):
        """ Test deploy with placement but no constraints returns None """
        placement = Placement(constraints=None)
        deploy = Deploy(placement=placement)
        self.assertIsNone(constraints(deploy))

    def test_deploy_with_placement_with_empty_constraints_returns_none(self):
        """ Test deploy with placement and empty constraints list returns None """
        placement = Placement(constraints=[])
        deploy = Deploy(placement=placement)
        self.assertIsNone(constraints(deploy))

    def test_deploy_with_constraints_replaces_single_equals(self):
        """ Test constraints replace single '=' with ' == ' """
        raw_constraints = ["node.labels.region=can-west", "node.role=manager"]
        placement = Placement(constraints=raw_constraints)
        deploy = Deploy(placement=placement)

        expected = ["node.labels.region == can-west", "node.role == manager"]
        self.assertEqual(constraints(deploy), expected)
