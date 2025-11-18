import unittest
from openfactory.schemas.connectors.opcua import OPCUAVariableConfig


class TestNodeIdParsing(unittest.TestCase):
    """ Tests for node_id parsing and validation. """

    def test_valid_node_id_numeric(self):
        var = OPCUAVariableConfig(node_id="ns=2;i=42", tag="Temp")
        self.assertEqual(var.namespace_index, 2)
        self.assertEqual(var.identifier_type, "i")
        self.assertEqual(var.identifier, "42")

    def test_valid_node_id_string(self):
        var = OPCUAVariableConfig(node_id="ns=3;s=Sensor_1", tag="Temp")
        self.assertEqual(var.namespace_index, 3)
        self.assertEqual(var.identifier_type, "s")
        self.assertEqual(var.identifier, "Sensor_1")

    def test_invalid_node_id_missing_ns(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id="2;i=42", tag="Temp")

    def test_invalid_node_id_missing_type_separator(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id="ns=2;42", tag="Temp")

    def test_invalid_node_id_invalid_namespace_index(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id="ns=abc;i=42", tag="Temp")

    def test_invalid_node_id_invalid_identifier_type(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id="ns=2;x=42", tag="Temp")

    def test_invalid_node_id_empty_identifier(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id="ns=2;i=", tag="Temp")

    def test_node_id_with_special_characters(self):
        var = OPCUAVariableConfig(node_id="ns=2;s=Temp_Sensor-01", tag="T")
        self.assertEqual(var.identifier, "Temp_Sensor-01")

    def test_node_id_with_unicode_identifier(self):
        var = OPCUAVariableConfig(node_id="ns=1;s=TempµSensor", tag="T")
        self.assertEqual(var.identifier, "TempµSensor")

    def test_long_identifier_string(self):
        long_id = "A" * 500
        var = OPCUAVariableConfig(node_id=f"ns=1;s={long_id}", tag="T")
        self.assertEqual(var.identifier, long_id)

    def test_node_id_with_whitespace_rejected(self):
        with self.assertRaises(ValueError):
            OPCUAVariableConfig(node_id=" ns=2;i=42 ", tag="Temp")


class TestOPCUAVariableConfig(unittest.TestCase):
    """ Tests for standalone variable config behavior. """

    def test_serialization_excludes_parsed_fields(self):
        var = OPCUAVariableConfig(node_id="ns=2;i=42", tag="T")
        dumped = var.model_dump()
        self.assertNotIn("namespace_index", dumped)
        self.assertNotIn("identifier_type", dumped)
        self.assertNotIn("identifier", dumped)

        # Deadband must exist and default to 0.0
        self.assertIn("deadband", dumped)
        self.assertEqual(dumped["deadband"], 0.0)
