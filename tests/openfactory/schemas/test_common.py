import unittest
from pydantic import ValidationError
from openfactory.schemas.common import ResourcesDefinition, Resources, Placement, Deploy


class TestCommonSchemas(unittest.TestCase):
    """
    Unit tests for common schemas.
    """

    def test_resources_definition_valid(self):
        """ Test valid ResourcesDefinition with both CPU and memory. """
        data = {"cpus": 0.5, "memory": "1Gi"}
        model = ResourcesDefinition(**data)
        self.assertEqual(model.cpus, 0.5)
        self.assertEqual(model.memory, "1Gi")

    def test_resources_definition_partial(self):
        """ Test ResourcesDefinition with only one field defined. """
        model = ResourcesDefinition(cpus=1.0)
        self.assertEqual(model.cpus, 1.0)
        self.assertIsNone(model.memory)

    def test_resources_definition_invalid_cpu_type(self):
        """ Test ResourcesDefinition with invalid CPU type. """
        with self.assertRaises(ValidationError):
            ResourcesDefinition(cpus="not-a-float")

    def test_resources_valid(self):
        """ Test valid Resources with nested reservations and limits. """
        data = {
            "reservations": {"cpus": 0.2, "memory": "256Mi"},
            "limits": {"cpus": 0.5, "memory": "512Mi"}
        }
        model = Resources(**data)
        self.assertEqual(model.reservations.cpus, 0.2)
        self.assertEqual(model.limits.memory, "512Mi")

    def test_resources_optional_fields(self):
        """ Test Resources with only one of reservations/limits defined. """
        model = Resources(reservations=ResourcesDefinition(cpus=0.1))
        self.assertIsNotNone(model.reservations)
        self.assertIsNone(model.limits)

    def test_placement_constraints(self):
        """ Test valid placement constraints. """
        constraints = ["node.labels.zone == eu-west", "disk.type == ssd"]
        model = Placement(constraints=constraints)
        self.assertEqual(model.constraints, constraints)

    def test_placement_empty(self):
        """ Test Placement model with no constraints. """
        model = Placement()
        self.assertIsNone(model.constraints)

    def test_deploy_defaults(self):
        """ Test Deploy model with all defaults. """
        model = Deploy()
        self.assertEqual(model.replicas, 1)
        self.assertIsNone(model.resources)
        self.assertIsNone(model.placement)

    def test_deploy_with_all_fields(self):
        """ Test Deploy with full nested configuration. """
        data = {
            "replicas": 3,
            "resources": {
                "reservations": {"cpus": 0.2, "memory": "128Mi"},
                "limits": {"cpus": 0.5, "memory": "256Mi"}
            },
            "placement": {
                "constraints": ["node.labels.arch == amd64"]
            }
        }
        model = Deploy(**data)
        self.assertEqual(model.replicas, 3)
        self.assertEqual(model.resources.limits.cpus, 0.5)
        self.assertEqual(model.placement.constraints[0], "node.labels.arch == amd64")

    def test_deploy_invalid_replicas(self):
        """ Test Deploy with invalid replicas (non-integer). """
        with self.assertRaises(ValidationError):
            Deploy(replicas="not-an-int")
