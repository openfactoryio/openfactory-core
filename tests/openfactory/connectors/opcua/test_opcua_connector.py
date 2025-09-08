import unittest
import json
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

        # Create mock connector data (similar to MTConnect example)
        connector_cfg = MagicMock()
        connector_cfg.type = "opcua"
        connector_cfg.model_dump.return_value = {"field": "value"}

        # Create mock Device
        self.device = MagicMock(spec=Device)
        self.device.uuid = "DEVICE-123"
        self.device.uns = {"meta": "data"}
        self.device.connector = connector_cfg
        self.device.ksql_tables = ["device"]

    def test_connector_is_registered(self):
        """ OPCUAConnectorSchema should map to OPCUAConnector in CONNECTOR_REGISTRY. """
        self.assertIn(OPCUAConnectorSchema, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[OPCUAConnectorSchema], OPCUAConnector)

    def test_deploy_wrong_connector_type_raises(self):
        """ Deploy should raise if connector type is not opcua. """
        self.device.connector.type = "mtconnect"
        with self.assertRaises(OFAException):
            self.connector.deploy(self.device, "config.yaml")

    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    @patch.object(OPCUAConnector, "deploy_opcua_producer")
    def test_deploy_calls_register_and_deploy(self, mock_deploy_producer, mock_register_asset):
        """ Deploy should call register_asset and deploy_opcua_producer for valid device. """
        self.connector.deploy(self.device, "config.yaml")
        mock_register_asset.assert_called_once_with(
            self.device.uuid,
            uns=self.device.uns,
            asset_type="Device",
            ksqlClient=self.ksql_mock,
            docker_service=""
        )
        mock_deploy_producer.assert_called_once_with(self.device)

    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    def test_deploy_opcua_producer_calls_deployment_strategy_and_registers_producer(self, mock_register_asset, mock_asset_cls):
        """ deploy_opcua_producer should call deployment strategy deploy and register_asset. """

        # Mock Asset instance
        mock_asset_instance = MagicMock()
        mock_asset_cls.return_value = mock_asset_instance

        self.connector.deploy_opcua_producer(self.device)

        # Deployment strategy should be called
        self.deploy_strategy_mock.deploy.assert_called()

        # register_asset should be called
        mock_register_asset.assert_called_once()

        # Add reference calls should happen on mocked Asset
        self.assertTrue(mock_asset_instance.add_reference_below.called)
        self.assertTrue(mock_asset_instance.add_reference_above.called)

    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.config.OPCUA_PRODUCER_LOG_LEVEL", "mocked_level")
    @patch("openfactory.connectors.opcua.opcua_connector.config.KAFKA_BROKER", "mocked_kafka:9092")
    def test_deploy_opcua_producer_sets_correct_env(self, mock_register_asset, mock_asset_cls):
        """ deploy_opcua_producer should set correct environment variables. """

        mock_asset_cls.return_value = MagicMock()

        self.connector.deploy_opcua_producer(self.device)

        # Capture arguments passed to deployment_strategy.deploy
        _, kwargs = self.deploy_strategy_mock.deploy.call_args
        env_vars = kwargs["env"]

        # Assertions for key env variables
        self.assertIn("KAFKA_BROKER=mocked_kafka:9092", env_vars)
        self.assertIn(f"KSQLDB_URL={self.ksql_mock.ksqldb_url}", env_vars)
        self.assertIn(f"OPCUA_CONNECTOR={json.dumps(self.device.connector.model_dump())}", env_vars)
        self.assertIn("OPCUA_PRODUCER_UUID=DEVICE-123-PRODUCER", env_vars)
        self.assertIn("OPCUA_PRODUCER_LOG_LEVEL=mocked_level", env_vars)
        self.assertIn("DOCKER_SERVICE=device-123-producer", env_vars)
        self.assertIn("DEVICE_UUID=DEVICE-123", env_vars)
        self.assertIn("APPLICATION_MANUFACTURER=OpenFactory", env_vars)
        self.assertIn("APPLICATION_LICENSE=Polyform Noncommercial License 1.0.0", env_vars)

    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    def test_deploy_opcua_producer_docker_api_error_raises(self, mock_register_asset):
        """ deploy_opcua_producer should raise OFAException on Docker APIError. """
        from docker.errors import APIError
        self.deploy_strategy_mock.deploy.side_effect = APIError("Docker error")
        with self.assertRaises(OFAException):
            self.connector.deploy_opcua_producer(self.device)

    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_success(self, mock_user_notify, mock_deregister):
        """ tear_down should remove producer and deregister assets on success. """
        self.connector.tear_down("test-device")
        self.deploy_strategy_mock.remove.assert_called_once_with("test-device-producer")
        mock_deregister.assert_called_once_with(
            "test-device-PRODUCER",
            ksqlClient=self.ksql_mock,
            bootstrap_servers=self.connector.bootstrap_servers
        )
        mock_user_notify.success.assert_called_once()

    @patch("openfactory.connectors.opcua.opcua_connector.docker.errors.NotFound", new=Exception)
    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    def test_tear_down_not_running(self, mock_user_notify):
        """ Handle case where producer was not running. """
        # Simulate NotFound error
        self.deploy_strategy_mock.remove.side_effect = Exception
        self.connector.tear_down("test-device")
        mock_user_notify.info.assert_called()

    @patch("openfactory.connectors.opcua.opcua_connector.docker.errors.APIError", new=Exception)
    def test_tear_down_api_error_raises(self):
        """ tear_down should raise OFAException on Docker APIError. """
        self.deploy_strategy_mock.remove.side_effect = Exception("API error")
        with self.assertRaises(OFAException):
            self.connector.tear_down("test-device")
