import unittest
from unittest.mock import MagicMock, patch
from openfactory.connectors.opcua.opcua_connector import OPCUAConnector
from openfactory.schemas.devices import Device
from openfactory.exceptions import OFAException
from openfactory.connectors.registry import CONNECTOR_REGISTRY
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema


class TestOPCUAConnector(unittest.TestCase):
    """
    Tests for OPCUAConnector class
    """

    def setUp(self):
        # Mock deployment strategy and KSQL client
        self.ksql_mock = MagicMock()
        self.deploy_strategy_mock = MagicMock()

        # Create OPCUAConnector instance
        self.connector = OPCUAConnector(
            deployment_strategy=self.deploy_strategy_mock,
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        # Create mock Device
        self.device = MagicMock(spec=Device)
        self.device.uuid = "DEVICE-123"
        self.device.uns = {"meta": "data"}
        self.device.ksql_tables = ["device"]

        # Connector type
        connector_cfg = MagicMock()
        connector_cfg.type = "opcua"
        self.device.connector = connector_cfg

        # Properly mock model_dump to return a real dict
        self.device.model_dump = MagicMock(return_value={"field": "value"})

    def test_connector_is_registered(self):
        """ OPCUAConnectorSchema should map to OPCUAConnector in CONNECTOR_REGISTRY. """
        self.assertIn(OPCUAConnectorSchema, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[OPCUAConnectorSchema], OPCUAConnector)

    def test_deploy_wrong_connector_type_raises(self):
        """ Deploy should raise if connector type is not opcua. """
        self.device.connector.type = "mtconnect"
        with self.assertRaises(OFAException):
            self.connector.deploy(self.device, "config.yaml")

    @patch("openfactory.connectors.opcua.opcua_connector.config.OPCUA_CONNECTOR_COORDINATOR",
           "http://mock-coordinator:8000")
    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.requests.post")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_deploy(self, mock_notify, mock_post, mock_register_asset, mock_asset):
        """ Test deployfull flow """
        # Mock post return values
        mock_post.return_value.json.return_value = {"assigned_gateway": "gw-1"}
        mock_post.return_value.raise_for_status.return_value = None

        # Mocks for device and producer
        mock_device_asset = MagicMock()
        mock_producer_asset = MagicMock()
        mock_asset.side_effect = [mock_device_asset, mock_producer_asset]

        self.connector.deploy(self.device, 'some_file.yml')

        # Assert HTTP POST
        mock_post.assert_called_once_with(
            "http://mock-coordinator:8000/register_device",
            json={'device': {'field': 'value'}}
        )

        # Assert register_asset calls
        expected_calls = [
            unittest.mock.call(
                self.device.uuid,
                uns=self.device.uns,
                asset_type="Device",
                ksqlClient=self.ksql_mock,
                bootstrap_servers="kafka:9092"
            ),
            unittest.mock.call(
                self.device.uuid.upper() + "-PRODUCER",
                uns=None,
                asset_type="KafkaProducer",
                ksqlClient=self.ksql_mock,
                bootstrap_servers="kafka:9092"
            )
        ]
        mock_register_asset.assert_has_calls(expected_calls, any_order=False)

        # Assert Asset constructor call order
        expected_asset_calls = [
            unittest.mock.call(self.device.uuid, ksqlClient=self.ksql_mock, bootstrap_servers="kafka:9092"),
            unittest.mock.call(self.device.uuid.upper() + "-PRODUCER", ksqlClient=self.ksql_mock, bootstrap_servers="kafka:9092")
        ]
        self.assertEqual(mock_asset.call_args_list, expected_asset_calls)

        # Assert references added correctly
        mock_device_asset.add_reference_below.assert_called_once_with(self.device.uuid.upper() + "-PRODUCER")
        mock_producer_asset.add_reference_above.assert_called_once_with(self.device.uuid)

        # Assert success notification
        mock_notify.success.assert_called_once_with(
            f"OPC UA producer for device {self.device.uuid} registerd succesfully with gateway gw-1"
        )

    @patch("openfactory.connectors.opcua.opcua_connector.requests.post")
    def test_deploy_opcua_producer_docker_api_error_raises(self, mock_post):
        """ deploy_opcua_producer should raise OFAException on RequestException. """
        from requests.exceptions import RequestException
        mock_post.side_effect = RequestException("Mocked exception")

        with self.assertRaises(OFAException) as cm:
            self.connector.deploy_opcua_producer(self.device)
        self.assertIn("Mocked exception", str(cm.exception))

    @patch("openfactory.connectors.opcua.opcua_connector.requests.post")
    def test_deploy_opcua_producer_connection_error_raises(self, mock_post):
        """ deploy_opcua_producer should raise OFAException if coordinator not reachable """
        from requests.exceptions import ConnectionError
        mock_post.side_effect = ConnectionError("Cannot connect")

        with self.assertRaises(OFAException) as cm:
            self.connector.deploy_opcua_producer(self.device)

        self.assertIn("No OPC UA Coordinator running at URL", str(cm.exception))

    @patch("openfactory.connectors.opcua.opcua_connector.config.OPCUA_CONNECTOR_COORDINATOR", "http://mock-coordinator")
    @patch("openfactory.connectors.opcua.opcua_connector.requests.delete")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_success(self, mock_notify, mock_deregister, mock_delete):
        """ Test successful tear_down """
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_delete.return_value = mock_response

        self.connector.tear_down(self.device.uuid)

        mock_delete.assert_called_once_with(
            "http://mock-coordinator/unregister_device/DEVICE-123"
        )
        mock_deregister.assert_called_once_with(
            "DEVICE-123-PRODUCER",
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )
        mock_notify.success.assert_called_once_with(
            "OPC UA producer for device DEVICE-123 shut down successfully"
        )

    @patch("openfactory.connectors.opcua.opcua_connector.requests.delete")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_connection_error_raises(self, mock_notify, mock_deregister, mock_delete):
        """ Tear down should raise OFAException if coordinator not reachable """
        from requests.exceptions import ConnectionError
        mock_delete.side_effect = ConnectionError("Cannot connect")

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down(self.device.uuid)

        self.assertIn("No OPC UA Coordinator running at URL", str(cm.exception))
        mock_deregister.assert_not_called()
        mock_notify.success.assert_not_called()

    @patch("openfactory.connectors.opcua.opcua_connector.config.OPCUA_CONNECTOR_COORDINATOR", "http://mock-coordinator")
    @patch("openfactory.connectors.opcua.opcua_connector.requests.delete")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_http_error_with_detail(self, mock_notify, mock_deregister, mock_delete):
        """ Tear down should surface FastAPI detail message on HTTPError """
        from requests.exceptions import HTTPError

        # Mock response with JSON detail
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("404 Client Error")
        mock_response.json.return_value = {"detail": "Device DEVICE-123 not found"}
        mock_delete.return_value = mock_response

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down(self.device.uuid)

        # Assert the detail text is included in the raised exception
        self.assertIn("Device DEVICE-123 not found", str(cm.exception))

        mock_notify.success.assert_not_called()

    @patch("openfactory.connectors.opcua.opcua_connector.requests.delete")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_request_exception_raises(self, mock_notify, mock_deregister, mock_delete):
        """ Tear down should raise OFAException on other RequestException """
        from requests.exceptions import RequestException
        mock_delete.side_effect = RequestException("HTTP error")

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down(self.device.uuid)

        self.assertIn("HTTP error", str(cm.exception))
        mock_deregister.assert_not_called()
        mock_notify.success.assert_not_called()
