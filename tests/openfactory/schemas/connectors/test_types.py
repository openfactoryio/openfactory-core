import unittest
from pydantic import ValidationError, BaseModel, TypeAdapter
from openfactory.schemas.connectors.types import Connector
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema, OPCUAVariableConfig


class TestConnectorUnion(unittest.TestCase):
    """
    Unit tests for Connector Union
    """

    def test_valid_mtconnect_connector(self):
        """ Test valid MTConnect connector configuration is parsed correctly. """
        data = {
            "type": "mtconnect",
            "agent": {
                "port": 5000,
                "device_xml": "some/path/device.xml",
                "adapter": {
                    "ip": "192.168.1.10",
                    "port": 7878
                }
            }
        }
        connector = TypeAdapter(Connector).validate_python(data)
        self.assertIsInstance(connector, MTConnectConnectorSchema)
        self.assertEqual(connector.type, "mtconnect")
        self.assertEqual(connector.agent.port, 5000)

    def test_valid_opcua_connector(self):
        """ Test valid OPC UA connector configuration is parsed correctly. """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
                # subscription omitted → defaults applied
                "variables": {
                    "temp": {
                        "node_id": "ns=3;i=1050",
                        "tag": "Temperature"
                    },
                    "hum": {
                        "node_id": "ns=2;i=10",
                        "tag": "Humidity"
                    }
                }
            }
        }

        connector = OPCUAConnectorSchema(**data)

        # Type checks
        self.assertIsInstance(connector, OPCUAConnectorSchema)
        self.assertEqual(connector.type, "opcua")
        self.assertEqual(connector.server.uri, "opc.tcp://127.0.0.1:4840/freeopcua/server/")

        # Variables should be normalized into OPCUAVariableConfig
        self.assertIn("temp", connector.server.variables)
        temp_var = connector.server.variables["temp"]
        self.assertIsInstance(temp_var, OPCUAVariableConfig)
        self.assertEqual(temp_var.tag, "Temperature")
        # Defaults applied
        self.assertEqual(temp_var.queue_size, 1)
        self.assertEqual(temp_var.sampling_interval, 0)

        self.assertIn("hum", connector.server.variables)
        hum_var = connector.server.variables["hum"]
        self.assertIsInstance(hum_var, OPCUAVariableConfig)
        self.assertEqual(hum_var.tag, "Humidity")
        # Defaults applied
        self.assertEqual(hum_var.queue_size, 1)
        self.assertEqual(hum_var.sampling_interval, 0)

    def test_unknown_type_discriminator(self):
        """ Test validation error when unknown connector type is used. """
        data = {
            "type": "none-existent",
            "agent": {
                "port": 1234
            }
        }
        with self.assertRaises(ValidationError):
            TypeAdapter(Connector).validate_python(data)

    def test_missing_type_field(self):
        """ Test validation error when 'type' field is missing. """

        # Simulates how Connector is actually used in a model field (e.g., Device.connector)
        class ConnectorWrapper(BaseModel):
            connector: Connector

        data = {
            "connector": {
                "agent": {
                    "port": 5000,
                    "device_xml": "x",
                    "adapter": {
                        "ip": "1.1.1.1",
                        "port": 7878
                    }
                }
            }
        }

        with self.assertRaises(ValidationError) as context:
            ConnectorWrapper.model_validate(data)

        self.assertIn("type", str(context.exception))
