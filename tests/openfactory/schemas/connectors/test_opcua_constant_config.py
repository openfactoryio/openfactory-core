import unittest
from openfactory.schemas.connectors.opcua import OPCUAConstantConfig


class TestOPCUAConstantConfig(unittest.TestCase):
    """ Tests for standalone constant config behavior. """

    def test_serialization_excludes_parsed_fields(self):
        const = OPCUAConstantConfig(node_id="ns=2;i=42", tag="T")
        dumped = const.model_dump()

        # Parsed fields must not be serialized
        self.assertNotIn("namespace_index", dumped)
        self.assertNotIn("identifier_type", dumped)
        self.assertNotIn("identifier", dumped)

        # Tag must exist
        self.assertIn("tag", dumped)
        self.assertEqual(dumped["tag"], "T")

        # browse_path should not be present when not set
        self.assertNotIn("browse_path", dumped)

        # Ensure variable-only fields are NOT present
        self.assertNotIn("deadband", dumped)
        self.assertNotIn("access_level", dumped)
        self.assertNotIn("queue_size", dumped)
        self.assertNotIn("sampling_interval", dumped)
