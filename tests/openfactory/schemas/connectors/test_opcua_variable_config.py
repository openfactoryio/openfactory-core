import unittest
from openfactory.schemas.connectors.opcua import OPCUAVariableConfig


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

        # Access level must exist and default to 'ro'
        self.assertIn("access_level", dumped)
        self.assertEqual(dumped["access_level"], "ro")

        # browse_path should not be present when not set
        self.assertNotIn("browse_path", dumped)
