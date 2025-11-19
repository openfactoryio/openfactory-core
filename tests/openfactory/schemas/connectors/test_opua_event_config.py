import unittest
from openfactory.schemas.connectors.opcua import OPCUAEventConfig


class TestOPCUAEventConfig(unittest.TestCase):
    """ Tests for OPCUAEventConfig node_id parsing and validation. """

    def test_valid_node_id_numeric(self):
        evt = OPCUAEventConfig(node_id="ns=6;i=10")
        self.assertEqual(evt.namespace_index, 6)
        self.assertEqual(evt.identifier_type, "i")
        self.assertEqual(evt.identifier, "10")

    def test_valid_node_id_string(self):
        evt = OPCUAEventConfig(node_id="ns=3;s=SensorA")
        self.assertEqual(evt.namespace_index, 3)
        self.assertEqual(evt.identifier_type, "s")
        self.assertEqual(evt.identifier, "SensorA")

    def test_invalid_node_id_format(self):
        with self.assertRaises(ValueError):
            OPCUAEventConfig(node_id="3;i=10")

    def test_invalid_node_id_empty_identifier(self):
        with self.assertRaises(ValueError):
            OPCUAEventConfig(node_id="ns=2;i=")

    def test_node_id_special_characters(self):
        evt = OPCUAEventConfig(node_id="ns=1;s=Alarm_01-µ")
        self.assertEqual(evt.identifier, "Alarm_01-µ")


class TestOPCUAConnectorSchemaEvents(unittest.TestCase):
    """ Tests normalization and uniqueness checks for events in the connector schema. """

    def test_event_serialization_excludes_parsed_fields(self):
        evt = OPCUAEventConfig(node_id="ns=6;i=10")
        dumped = evt.model_dump()
        self.assertNotIn("namespace_index", dumped)
        self.assertNotIn("identifier_type", dumped)
        self.assertNotIn("identifier", dumped)
