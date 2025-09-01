import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema


class TestOPCUAConnectorSchema(unittest.TestCase):
    """ Unit tests for OPCUAConnectorSchema """

    def test_valid_connector_schema_with_path(self):
        """ Valid connector schema with device path """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"path": "Sensors/TemperatureSensor_1"}
        }
        schema = OPCUAConnectorSchema(**data)
        self.assertEqual(schema.type, "opcua")
        self.assertEqual(schema.server.uri, "opc.tcp://127.0.0.1:4840")
        self.assertEqual(schema.device.path, "Sensors/TemperatureSensor_1")

    def test_valid_connector_schema_with_node_id(self):
        """ Valid connector schema with device node_id """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"node_id": "ns=2;i=42"}
        }
        schema = OPCUAConnectorSchema(**data)
        self.assertEqual(schema.device.node_id, "ns=2;i=42")
        self.assertEqual(schema.device.namespace_index, 2)
        self.assertEqual(schema.device.identifier_type, "i")
        self.assertEqual(schema.device.identifier, "42")

    def test_missing_type_field_raises(self):
        """ Missing 'type' field should raise ValidationError """
        data = {
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"path": "Sensors/TemperatureSensor_1"}
        }
        with self.assertRaises(ValidationError):
            OPCUAConnectorSchema(**data)

    def test_invalid_device_nested_validation(self):
        """ Nested device validation errors propagate """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"node_id": "invalid_format"}
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        err_str = str(cm.exception)
        self.assertIn("Invalid node_id format", err_str)

    def test_extra_fields_forbid(self):
        """ Unknown fields in the top-level schema should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"path": "Sensors/TemperatureSensor_1"},
            "foo": "bar"
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("foo", str(cm.exception))
