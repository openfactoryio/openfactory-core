import unittest
from pydantic import ValidationError
from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.types import MTConnectConnector
from openfactory.schemas.supervisors import Supervisor, SupervisorAdapter


class TestDevice(unittest.TestCase):
    """
    Unit tests for class Device
    """

    def setUp(self):
        # Minimal valid MTConnectConnector with required 'type' and 'agent' (external agent)
        self.valid_connector_data = {
            "type": "mtconnect",
            "agent": {
                "ip": "10.0.0.1",
                "port": 5000
            }
        }
        self.valid_connector = MTConnectConnector(**self.valid_connector_data)

        # Optional supervisor config
        self.valid_supervisor = Supervisor(
            image="supervisor-image",
            adapter=SupervisorAdapter(ip="192.168.1.1", port=8080),
            deploy=None
        )

    def test_device_valid_minimal(self):
        """ Minimal Device with connector only. """
        device = Device(
            uuid="device-123",
            connector=self.valid_connector
        )
        self.assertEqual(device.uuid, "device-123")
        self.assertIsNone(device.supervisor)
        self.assertIsNone(device.uns)
        self.assertIsNone(device.ksql_tables)
        self.assertIsNotNone(device.connector.agent.deploy)
        self.assertEqual(device.connector.agent.deploy.replicas, 1)

    def test_device_valid_full(self):
        """ Full Device with connector, supervisor, ksql_tables, and UNS metadata. """
        device = Device(
            uuid="device-123",
            connector=self.valid_connector,
            supervisor=self.valid_supervisor,
            ksql_tables=["device", "agent"],
            uns={"some": "metadata"}
        )
        self.assertEqual(device.ksql_tables, ["device", "agent"])
        self.assertEqual(device.uns, {"some": "metadata"})
        self.assertIsNotNone(device.connector.agent.deploy)
        self.assertEqual(device.connector.agent.deploy.replicas, 1)

    def test_ksql_tables_invalid_raises(self):
        """ Invalid ksql_tables entries should raise ValidationError. """
        with self.assertRaises(ValidationError) as cm:
            Device(
                uuid="device-789",
                connector=self.valid_connector,
                ksql_tables=["invalid_table"]
            )
        self.assertIn("Invalid entries in ksql_tables", str(cm.exception))

    def test_extra_fields_forbidden(self):
        """ Providing extra undefined fields should raise ValidationError. """
        with self.assertRaises(ValidationError) as cm:
            Device(
                uuid="device-123",
                connector=self.valid_connector,
                foo="bar"
            )
        self.assertIn("extra", str(cm.exception).lower())
        self.assertIn("permitted", str(cm.exception).lower())
