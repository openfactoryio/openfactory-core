import unittest
from pydantic import ValidationError
from openfactory.schemas.common import Deploy, Resources, ResourcesDefinition, Placement
from openfactory.schemas.supervisors import Supervisor, SupervisorAdapter


class TestSupervisor(unittest.TestCase):
    """
    Unit tests for class Supervisor
    """

    def setUp(self):
        """ Set up test data """
        self.valid_adapter = SupervisorAdapter(
            ip="192.168.1.1",
            port=8080,
            environment=["ENV_VAR=value"],
            deploy=Deploy(
                replicas=2,
                resources=Resources(
                    reservations=ResourcesDefinition(cpus=1.5, memory="512Mi"),
                    limits=ResourcesDefinition(cpus=2.0, memory="1Gi")
                ),
                placement=Placement(constraints=["node.role==worker"])
            )
        )

    def test_supervisor_with_valid_data(self):
        """ Test that Supervisor initializes correctly with valid data """
        supervisor = Supervisor(
            image="supervisor-image:latest",
            adapter=self.valid_adapter,
            deploy=Deploy(
                replicas=3,
                resources=Resources(
                    reservations=ResourcesDefinition(cpus=2.0, memory="1Gi"),
                    limits=ResourcesDefinition(cpus=3.0, memory="2Gi")
                ),
                placement=Placement(constraints=["node.role==manager"])
            )
        )
        self.assertEqual(supervisor.image, "supervisor-image:latest")
        self.assertEqual(supervisor.adapter.ip, "192.168.1.1")
        self.assertEqual(supervisor.deploy.replicas, 3)

    def test_supervisor_missing_image(self):
        """ Test that Supervisor raises an error when 'image' is missing """
        with self.assertRaises(ValidationError):
            Supervisor(
                adapter=self.valid_adapter
            )

    def test_supervisor_missing_adapter(self):
        """ Test that Supervisor raises an error when 'adapter' is missing """
        with self.assertRaises(ValidationError):
            Supervisor(
                image="supervisor-image:latest"
            )

    def test_supervisor_optional_deploy(self):
        """ Test that Supervisor initializes correctly without 'deploy' """
        supervisor = Supervisor(
            image="supervisor-image:latest",
            adapter=self.valid_adapter
        )
        self.assertIsNone(supervisor.deploy)

    def test_supervisor_adapter_invalid_type(self):
        """ Test that invalid adapter type (e.g., str) raises a ValidationError """
        with self.assertRaises(TypeError) as cm:
            Supervisor(
                image="supervisor-image:latest",
                adapter="this_should_be_a_dict_not_str"
            )
        self.assertIn("adapter configuration must be a dictionary", str(cm.exception).lower())

    def test_supervisor_forbid_extra_fields(self):
        """ Test that extra fields are forbidden """
        with self.assertRaises(ValidationError) as cm:
            Supervisor(
                image="supervisor-image:latest",
                adapter=self.valid_adapter,
                unexpected="not_allowed"
            )
        self.assertIn("extra inputs are not permitted", str(cm.exception).lower())
