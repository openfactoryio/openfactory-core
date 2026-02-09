import unittest
from openfactory.schemas.connectors.opcua import OPCUAMethodConfig


class TestOPCUAMethodSourceConfig(unittest.TestCase):
    """ Tests for standalone method source config behavior. """

    def test_serialization_excludes_parsed_fields(self):
        method_src = OPCUAMethodConfig(node_id="ns=2;i=42")

        dumped = method_src.model_dump()

        # Parsed fields must not be serialized
        self.assertNotIn("namespace_index", dumped)
        self.assertNotIn("identifier_type", dumped)
        self.assertNotIn("identifier", dumped)

        # Only the original addressing field should remain
        self.assertIn("node_id", dumped)
        self.assertEqual(dumped["node_id"], "ns=2;i=42")

        # browse_path should not be present when not set
        self.assertNotIn("browse_path", dumped)
