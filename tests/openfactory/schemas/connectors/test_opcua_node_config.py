import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUANodeConfig


class TestOPCUANodeConfig(unittest.TestCase):
    """ Unit tests for OPCUANodeConfig """

    # ---------------- NodeId Tests ----------------

    def test_valid_node_id_numeric(self):
        var = OPCUANodeConfig(node_id="ns=2;i=42")
        self.assertEqual(var.namespace_index, 2)
        self.assertEqual(var.identifier_type, "i")
        self.assertEqual(var.identifier, "42")

    def test_valid_node_id_string(self):
        var = OPCUANodeConfig(node_id="ns=3;s=Sensor_1")
        self.assertEqual(var.namespace_index, 3)
        self.assertEqual(var.identifier_type, "s")
        self.assertEqual(var.identifier, "Sensor_1")

    def test_invalid_node_id_missing_ns(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id="2;i=42")

    def test_invalid_node_id_missing_type_separator(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id="ns=2;42")

    def test_invalid_node_id_invalid_namespace_index(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id="ns=abc;i=42")

    def test_invalid_node_id_invalid_identifier_type(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id="ns=2;x=42")

    def test_invalid_node_id_empty_identifier(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id="ns=2;i=")

    def test_node_id_with_special_characters(self):
        var = OPCUANodeConfig(node_id="ns=2;s=Temp_Sensor-01")
        self.assertEqual(var.identifier, "Temp_Sensor-01")

    def test_node_id_with_unicode_identifier(self):
        var = OPCUANodeConfig(node_id="ns=1;s=TempµSensor")
        self.assertEqual(var.identifier, "TempµSensor")

    def test_long_identifier_string(self):
        long_id = "A" * 500
        var = OPCUANodeConfig(node_id=f"ns=1;s={long_id}")
        self.assertEqual(var.identifier, long_id)

    def test_node_id_with_whitespace_rejected(self):
        with self.assertRaises(ValueError):
            OPCUANodeConfig(node_id=" ns=2;i=42 ")

    # ---------------- Path Tests ----------------

    def test_valid_single_segment_path(self):
        node = OPCUANodeConfig(browse_path="0:Root")
        self.assertEqual(node.browse_path, "0:Root")

    def test_valid_multi_segment_path(self):
        path = "0:Root/0:Objects/2:DeviceSet/4:SIG350-0005AP100/2:Manufacturer"
        node = OPCUANodeConfig(browse_path=path)
        self.assertEqual(node.browse_path, path)

    def test_path_with_non_numeric_ns_index(self):
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(path="x:Root,0:Objects")

    def test_path_with_empty_identifier(self):
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(path="0:")

    def test_path_missing_colon_separator(self):
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(path="0Root,1:Objects")

    def test_path_and_node_id_together_invalid(self):
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(node_id="ns=1;i=1", path="0:Root")

    def test_missing_node_id_and_path_invalid(self):
        with self.assertRaises(ValidationError):
            OPCUANodeConfig()

    def test_path_with_unicode_identifier(self):
        path = "0:Root/1:Objects/2:Device_µSensor"
        node = OPCUANodeConfig(browse_path=path)
        self.assertEqual(node.browse_path, path)

    def test_path_with_special_characters(self):
        path = "0:Root/1:Objects/2:Device-001"
        node = OPCUANodeConfig(browse_path=path)
        self.assertEqual(node.browse_path, path)

    def test_path_with_spaces_inside_identifier(self):
        path = "0:Root/1:Objects/2:Device Set/4:SIG350 0005AP100/2:Manufacturer"
        node = OPCUANodeConfig(browse_path=path)
        self.assertEqual(node.browse_path, path)

    def test_path_with_leading_trailing_spaces_rejected(self):
        # Leading/trailing spaces in ns_index or overall path should fail
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(browse_path="0:Root/ 1:Objects")
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(browse_path="0:Root/1:Objects ")

    def test_path_with_spaces_around_colon_rejected(self):
        # Spaces around the colon separating ns_index and identifier should fail
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(path="0:Root/1 :Objects")
        with self.assertRaises(ValidationError):
            OPCUANodeConfig(path="0:Root/1: Objects")

    def test_path_must_start_with_root(self):
        # valid
        node = OPCUANodeConfig(browse_path="0:Root/0:Objects")
        self.assertEqual(node.browse_path, "0:Root/0:Objects")

        # invalid: does not start with 0:Root
        with self.assertRaises(ValidationError) as cm:
            OPCUANodeConfig(browse_path="1:Root/0:Objects")
        self.assertIn("BrowsePath must start with '0:Root'", str(cm.exception))

        with self.assertRaises(ValidationError) as cm:
            OPCUANodeConfig(browse_path="0:Objects/0:Root")
        self.assertIn("BrowsePath must start with '0:Root'", str(cm.exception))
