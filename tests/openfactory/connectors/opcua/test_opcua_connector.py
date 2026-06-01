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
        self.device.model_dump_json = MagicMock(
            return_value='{"field":"value"}'
        )

    def test_connector_is_registered(self):
        """ OPCUAConnectorSchema should map to OPCUAConnector in CONNECTOR_REGISTRY. """
        self.assertIn(OPCUAConnectorSchema, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[OPCUAConnectorSchema], OPCUAConnector)

    def test_deploy_wrong_connector_type_raises(self):
        """ Deploy should raise if connector type is not opcua. """
        self.device.connector.type = "mtconnect"
        with self.assertRaises(OFAException):
            self.connector.deploy(self.device, "config.yaml")

    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    def test_get_coordinator(self, mock_asset):
        """ _get_coordinator should return coordinator when available. """
        self.ksql_mock.query.return_value = [
            {"ASSET_UUID": "COORD-123"}
        ]

        coordinator = MagicMock()
        coordinator.avail.value = "AVAILABLE"
        mock_asset.return_value = coordinator

        result = self.connector._get_coordinator()

        self.ksql_mock.query.assert_called_once_with(
            "select ASSET_UUID FROM ASSETS_TYPE WHERE TYPE='OPCUA.Coordinator';"
        )

        mock_asset.assert_called_once_with(
            asset_uuid="COORD-123",
            ksqlClient=self.ksql_mock
        )

        self.assertIs(result, coordinator)

    def test_get_coordinator_not_deployed_raises(self):
        """ _get_coordinator should raise if no coordinator is found. """
        self.ksql_mock.query.return_value = []

        with self.assertRaises(OFAException) as cm:
            self.connector._get_coordinator()

        self.assertEqual(str(cm.exception), "OPC UA Coordinator is not deployed")

    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    def test_get_coordinator_not_available_raises(self, mock_asset):
        """ _get_coordinator should raise if coordinator is not AVAILABLE. """

        self.ksql_mock.query.return_value = [
            {"ASSET_UUID": "COORD-123"}
        ]

        coordinator = MagicMock()
        coordinator.avail.value = "UNAVAILABLE"
        mock_asset.return_value = coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector._get_coordinator()

        self.assertEqual(str(cm.exception), "OPC UA Coordinator is not AVAILABLE")

    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    @patch.object(OPCUAConnector, "_get_coordinator")
    def test_deploy(self, mock_get_coordinator, mock_register_asset, mock_notify):
        """ Test successful deployment """
        coordinator = MagicMock()
        mock_get_coordinator.return_value = coordinator

        self.connector.deploy(self.device, "some_file.yml")

        mock_get_coordinator.assert_called_once_with()

        coordinator.register_device.assert_called_once_with(
            sender_uuid="opcua-connector",
            device_config=str(self.device.model_dump_json())
        )

        mock_register_asset.assert_called_once_with(
            asset_uuid=self.device.uuid,
            uns=self.device.uns,
            asset_type="Device",
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        mock_notify.success.assert_called_once_with(
            f"OPC UA device {self.device.uuid} registered successfully"
        )

    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch.object(OPCUAConnector, "_get_coordinator")
    def test_tear_down_success(self, mock_get_coordinator, mock_deregister, mock_notify):
        """ Test successful tear down. """
        coordinator = MagicMock()
        mock_get_coordinator.return_value = coordinator

        self.connector.tear_down(self.device.uuid)

        mock_get_coordinator.assert_called_once_with()

        coordinator.deregister_device.assert_called_once_with(
            sender_uuid="opcua-connector",
            device_uuid=self.device.uuid
        )

        mock_deregister.assert_called_once_with(
            asset_uuid=self.device.uuid,
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        mock_notify.success.assert_called_once_with(
            f"SHDR device {self.device.uuid} deregistered successfully"
        )

    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    @patch("openfactory.connectors.opcua.opcua_connector.register_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    def test_deploy_invalid_coordinator_asset(self, mock_asset, mock_register_asset, mock_notify):

        coordinator = MagicMock()
        coordinator.avail.value = "AVAILABLE"
        coordinator.register_device.side_effect = TypeError()
        mock_asset.return_value = coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector.deploy(self.device, "some_file.yml")

        self.assertIn(
            "does not appear to be a valid OPC UA coordinator",
            str(cm.exception)
        )

        mock_register_asset.assert_not_called()
        mock_notify.success.assert_not_called()

    @patch("openfactory.connectors.opcua.opcua_connector.user_notify")
    @patch("openfactory.connectors.opcua.opcua_connector.deregister_asset")
    @patch("openfactory.connectors.opcua.opcua_connector.Asset")
    def test_tear_down_invalid_coordinator_asset(self, mock_asset, mock_deregister, mock_notify):
        """ Deploy should raise if coordinator asset is invalid """
        coordinator = MagicMock()
        coordinator.avail.value = "AVAILABLE"
        coordinator.deregister_device.side_effect = TypeError()
        mock_asset.return_value = coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down(self.device.uuid)

        self.assertIn(
            "does not appear to be a valid SHDR coordinator",
            str(cm.exception)
        )

        mock_deregister.assert_not_called()
        mock_notify.success.assert_not_called()
