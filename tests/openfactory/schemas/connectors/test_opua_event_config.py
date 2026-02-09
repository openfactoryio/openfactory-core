import unittest
from openfactory.schemas.connectors.opcua import OPCUAEventConfig


class TestOPCUAConnectorSchemaEvents(unittest.TestCase):
    """ Tests normalization and uniqueness checks for events in the connector schema. """

    def test_event_serialization_excludes_parsed_fields(self):
        evt = OPCUAEventConfig(node_id="ns=6;i=10")
        dumped = evt.model_dump()
        self.assertNotIn("namespace_index", dumped)
        self.assertNotIn("identifier_type", dumped)
        self.assertNotIn("identifier", dumped)

        # browse_path should not be present when not set
        self.assertNotIn("browse_path", dumped)

        # node_id must remain
        self.assertIn("node_id", dumped)
        self.assertEqual(dumped["node_id"], "ns=6;i=10")
