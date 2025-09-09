import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema, OPCUAVariableConfig


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

    def test_variables_normalization_with_server_defaults(self):
        """ Variables are normalized to OPCUAVariableConfig and inherit server defaults """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840",
                "namespace_uri": "http://example.com",
                "subscription": {"queue_size": 10, "sampling_interval": 100}
            },
            "device": {
                "path": "Sensors/TemperatureSensor",
                "variables": {
                    "temp": "Temperature",  # simple string
                    "hum": {                # explicit override
                        "browse_name": "Humidity",
                        "queue_size": 5,
                        "sampling_interval": 50
                    }
                }
            }
        }
        schema = OPCUAConnectorSchema(**data)

        temp_var = schema.device.variables["temp"]
        hum_var = schema.device.variables["hum"]

        # All variables normalized
        self.assertIsInstance(temp_var, OPCUAVariableConfig)
        self.assertIsInstance(hum_var, OPCUAVariableConfig)

        # Server defaults applied
        self.assertEqual(temp_var.queue_size, 10)
        self.assertEqual(temp_var.sampling_interval, 100)

        # Explicit overrides preserved
        self.assertEqual(hum_var.queue_size, 5)
        self.assertEqual(hum_var.sampling_interval, 50)

    def test_mixed_variable_config(self):
        """ Mix of string and OPCUAVariableConfig in device.variables works """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {
                "path": "Sensors/TemperatureSensor",
                "variables": {
                    "sensor_model": "SensorModel",          # string
                    "temperature": {"browse_name": "Temp"}  # dict override
                }
            }
        }
        schema = OPCUAConnectorSchema(**data)

        self.assertIsInstance(schema.device.variables["sensor_model"], OPCUAVariableConfig)
        self.assertEqual(schema.device.variables["sensor_model"].browse_name, "SensorModel")
        self.assertIsInstance(schema.device.variables["temperature"], OPCUAVariableConfig)
        self.assertEqual(schema.device.variables["temperature"].browse_name, "Temp")
