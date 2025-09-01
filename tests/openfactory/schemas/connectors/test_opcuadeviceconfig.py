import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUADeviceConfig


class TestOPCUADeviceConfig(unittest.TestCase):
    """ Unit tests for OPCUADeviceConfig """

    def test_valid_device_with_path_only(self):
        """ Device with only path should be valid. """
        data = {
            "path": "Sensors/TemperatureSensor_1",
            "variables": {"temp": "Temperature"},
            "methods": {"calibrate": "Calibrate"}
        }
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.path, "Sensors/TemperatureSensor_1")
        self.assertIsNone(device.node_id)
        self.assertEqual(device.variables, {"temp": "Temperature"})
        self.assertEqual(device.methods, {"calibrate": "Calibrate"})
        self.assertIsNone(device.namespace_index)
        self.assertIsNone(device.identifier_type)
        self.assertIsNone(device.identifier)

    def test_valid_device_with_node_id_numeric(self):
        """ Device with numeric node_id should parse namespace_index and identifier. """
        data = {"node_id": "ns=2;i=42", "variables": {"temp": "Temperature"}}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.node_id, "ns=2;i=42")
        self.assertIsNone(device.path)
        self.assertEqual(device.variables, {"temp": "Temperature"})
        self.assertEqual(device.namespace_index, 2)
        self.assertEqual(device.identifier_type, "i")
        self.assertEqual(device.identifier, "42")

    def test_valid_device_with_node_id_string(self):
        """ Device with string identifier node_id should parse correctly. """
        data = {"node_id": "ns=3;s=TemperatureSensor_1"}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.node_id, "ns=3;s=TemperatureSensor_1")
        self.assertIsNone(device.path)
        self.assertEqual(device.namespace_index, 3)
        self.assertEqual(device.identifier_type, "s")
        self.assertEqual(device.identifier, "TemperatureSensor_1")

    def test_both_path_and_node_id_defined_raises(self):
        """ Defining both path and node_id should raise ValueError. """
        data = {"path": "Sensors/TemperatureSensor_1", "node_id": "ns=2;i=42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Exactly one of 'path' or 'node_id' must be specified", str(cm.exception))

    def test_neither_path_nor_node_id_defined_raises(self):
        """ Defining neither path nor node_id should raise ValueError. """
        data = {}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Exactly one of 'path' or 'node_id' must be specified", str(cm.exception))

    def test_extra_fields_forbid_raises_validation_error(self):
        """ Providing unknown fields should raise ValidationError. """
        data = {"path": "Sensors/TemperatureSensor_1", "foo": "bar"}
        with self.assertRaises(ValidationError) as cm:
            OPCUADeviceConfig(**data)
        err_str = str(cm.exception)
        self.assertIn("foo", err_str)
        self.assertIn("extra", err_str.lower())

    def test_variables_and_methods_optional(self):
        """ Variables and methods can be omitted. """
        data = {"node_id": "ns=2;i=42"}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.node_id, "ns=2;i=42")
        self.assertIsNone(device.path)
        self.assertIsNone(device.variables)
        self.assertIsNone(device.methods)

    def test_invalid_node_id_missing_ns(self):
        """ node_id missing 'ns=' prefix should raise ValueError """
        data = {"node_id": "2;i=42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_invalid_node_id_missing_type_separator(self):
        """ node_id missing type separator ('i' or 's') should raise ValueError """
        data = {"node_id": "ns=2;42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_invalid_node_id_invalid_namespace_index(self):
        """ node_id with non-integer namespace_index should raise ValueError """
        data = {"node_id": "ns=abc;i=42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_invalid_node_id_invalid_identifier_type(self):
        """ node_id with invalid identifier type should raise ValueError """
        data = {"node_id": "ns=2;x=42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_invalid_node_id_empty_identifier(self):
        """ node_id with empty identifier should raise ValueError """
        data = {"node_id": "ns=2;i="}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_node_id_with_special_characters(self):
        """ node_id with special characters in identifier should parse correctly """
        data = {"node_id": "ns=2;s=Temp_Sensor-01"}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.namespace_index, 2)
        self.assertEqual(device.identifier_type, "s")
        self.assertEqual(device.identifier, "Temp_Sensor-01")

    def test_node_id_with_leading_trailing_whitespace(self):
        """ node_id with leading/trailing spaces should raise ValueError """
        data = {"node_id": " ns=2;i=42 "}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_path_with_leading_trailing_whitespace(self):
        """ path with leading/trailing spaces is preserved """
        data = {"path": "  Sensors/TemperatureSensor_1  "}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.path, "  Sensors/TemperatureSensor_1  ")

    def test_empty_variables_and_methods_dict(self):
        """ Empty dictionaries for variables or methods are allowed """
        data = {"node_id": "ns=2;i=42", "variables": {}, "methods": {}}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.variables, {})
        self.assertEqual(device.methods, {})

    def test_serialization_excludes_parsed_fields(self):
        """ Parsed fields (namespace_index, identifier_type, identifier) are excluded by default in model_dump() """
        data = {"node_id": "ns=2;i=42"}
        device = OPCUADeviceConfig(**data)
        d = device.model_dump()
        self.assertNotIn("namespace_index", d)
        self.assertNotIn("identifier_type", d)
        self.assertNotIn("identifier", d)

    def test_identifier_with_unicode_characters(self):
        """ Unicode characters in string identifier are handled correctly """
        data = {"node_id": "ns=1;s=TempµSensor"}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.identifier, "TempµSensor")

    def test_long_identifier_string(self):
        """ Very long string identifiers are handled correctly """
        long_id = "A" * 500
        data = {"node_id": f"ns=1;s={long_id}"}
        device = OPCUADeviceConfig(**data)
        self.assertEqual(device.identifier, long_id)

    def test_node_id_uppercase_type_invalid(self):
        """ Uppercase type character (I or S) should fail validation if regex is strict """
        data = {"node_id": "ns=2;I=42"}
        with self.assertRaises(ValueError) as cm:
            OPCUADeviceConfig(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))
