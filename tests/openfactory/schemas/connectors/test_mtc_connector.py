import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema


class TestMTConnectConnector(unittest.TestCase):
    """
    Unit tests for the MTConnectConnector schema
    """

    def test_valid_mtconnect_connector(self):
        """ Test valid MTConnectConnector with proper type and agent. """
        data = {
            "type": "mtconnect",
            "agent": {
                "ip": "10.0.0.1",
                "port": 5000,
            }
        }
        connector = MTConnectConnectorSchema(**data)
        self.assertEqual(connector.type, "mtconnect")
        self.assertEqual(connector.agent.ip, "10.0.0.1")
        self.assertEqual(connector.agent.port, 5000)

    def test_missing_type_field_raises(self):
        """ Test validation error when 'type' field is missing. """
        data = {
            "agent": {
                "ip": "10.0.0.1",
                "port": 5000,
            }
        }
        with self.assertRaises(ValidationError) as cm:
            MTConnectConnectorSchema(**data)
        err_str = str(cm.exception)
        self.assertIn("Field required", err_str)
        self.assertIn("type", err_str)

    def test_invalid_type_field_raises(self):
        """ Test validation error when 'type' field is invalid. """
        data = {
            "type": "invalid_type",
            "agent": {
                "ip": "10.0.0.1",
                "port": 5000,
            }
        }
        with self.assertRaises(ValidationError) as cm:
            MTConnectConnectorSchema(**data)
        err_str = str(cm.exception)
        self.assertIn("mtconnect", err_str)
        self.assertIn("invalid_type", err_str)

    def test_missing_agent_field_raises(self):
        """ Test validation error when 'agent' field is missing. """
        data = {
            "type": "mtconnect",
        }
        with self.assertRaises(ValidationError) as cm:
            MTConnectConnectorSchema(**data)
        err_str = str(cm.exception)
        self.assertIn("Field required", err_str)
        self.assertIn("agent", err_str)
