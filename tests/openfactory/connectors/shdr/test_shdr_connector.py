import unittest
from unittest.mock import MagicMock, patch
from openfactory.connectors.shdr.shdr_connector import SHDRConnector
from openfactory.schemas.devices import Device
from openfactory.exceptions import OFAException
from openfactory.connectors.registry import CONNECTOR_REGISTRY
from openfactory.schemas.connectors.shdr import SHDRConnectorSchema


class TestSHDRConnector(unittest.TestCase):
    """
    Tests for SHDRConnector class
    """

    def setUp(self):

        # Mock deployment strategy and KSQL client
        self.ksql_mock = MagicMock()
        self.deploy_strategy_mock = MagicMock()

        # Create SHDRConnector instance
        self.connector = SHDRConnector(
            deployment_strategy=self.deploy_strategy_mock,
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        # Create mock Device
        self.device = MagicMock(spec=Device)
        self.device.uuid = "DEVICE-123"
        self.device.uns = {"meta": "data"}

        # Connector type
        connector_cfg = MagicMock()
        connector_cfg.type = "shdr"
        self.device.connector = connector_cfg

        # Properly mock model_dump_json
        self.device.model_dump_json = MagicMock(
            return_value='{"field":"value"}'
        )

    def test_connector_is_registered(self):
        """ SHDRConnectorSchema should map to SHDRConnector in CONNECTOR_REGISTRY. """

        self.assertIn(SHDRConnectorSchema, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[SHDRConnectorSchema], SHDRConnector)

    def test_deploy_wrong_connector_type_raises(self):
        """ Deploy should raise if connector type is not shdr. """

        self.device.connector.type = "opcua"
        with self.assertRaises(OFAException):
            self.connector.deploy(self.device, "config.yaml")

    @patch("openfactory.connectors.shdr.shdr_connector.Asset")
    def test_get_coordinator(self, mock_asset):
        """ _get_coordinator should return coordinator when available. """
        self.ksql_mock.query.return_value = [
            {"ASSET_UUID": "COORD-123"}
        ]

        mock_coordinator = MagicMock()
        mock_coordinator.avail.value = "AVAILABLE"
        mock_asset.return_value = mock_coordinator

        coordinator = self.connector._get_coordinator()

        self.ksql_mock.query.assert_called_once_with(
            "select ASSET_UUID FROM ASSETS_TYPE WHERE TYPE='SHDR.Coordinator';"
        )

        mock_asset.assert_called_once_with(
            asset_uuid="COORD-123",
            ksqlClient=self.ksql_mock
        )

        self.assertIs(coordinator, mock_coordinator)

    def test_get_coordinator_not_deployed_raises(self):
        """ _get_coordinator should raise if no coordinator is found. """
        self.ksql_mock.query.return_value = []

        with self.assertRaises(OFAException) as cm:
            self.connector._get_coordinator()

        self.assertEqual(str(cm.exception), "SHDR Coordinator is not deployed")

    @patch("openfactory.connectors.shdr.shdr_connector.register_asset")
    @patch("openfactory.connectors.shdr.shdr_connector.user_notify")
    @patch.object(SHDRConnector, "_get_coordinator")
    def test_deploy(self, mock_get_coordinator, mock_notify, mock_register_asset):
        """ Test deploy flow. """
        mock_coordinator = MagicMock()
        mock_get_coordinator.return_value = mock_coordinator

        self.connector.deploy(self.device, "config.yaml")

        mock_get_coordinator.assert_called_once_with()

        mock_coordinator.register_device.assert_called_once_with(
            sender_uuid="ofa-cli",
            device_config='{"field":"value"}'
        )

        mock_register_asset.assert_called_once_with(
            asset_uuid=self.device.uuid,
            uns=self.device.uns,
            asset_type="Device",
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        mock_notify.success.assert_called_once_with(
            f"SHDR device {self.device.uuid} registered successfully"
        )

    @patch("openfactory.connectors.shdr.shdr_connector.Asset")
    def test_get_coordinator_not_available_raises(self, mock_asset):
        """ _get_coordinator should raise if coordinator is not AVAILABLE """

        self.ksql_mock.query.return_value = [
            {"ASSET_UUID": "SHDR-COORDINATOR"}
        ]

        mock_coordinator = MagicMock()
        mock_coordinator.avail.value = "UNAVAILABLE"
        mock_asset.return_value = mock_coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector._get_coordinator()

        self.ksql_mock.query.assert_called_once_with(
            "select ASSET_UUID FROM ASSETS_TYPE WHERE TYPE='SHDR.Coordinator';"
        )

        mock_asset.assert_called_once_with(
            asset_uuid="SHDR-COORDINATOR",
            ksqlClient=self.ksql_mock
        )

        self.assertIn("is not AVAILABLE", str(cm.exception))

    @patch.object(SHDRConnector, "_get_coordinator")
    def test_deploy_invalid_coordinator_raises(self, mock_get_coordinator):
        """ Deploy should raise if coordinator does not expose register_device """
        mock_coordinator = MagicMock()
        mock_coordinator.register_device.side_effect = TypeError("Invalid coordinator")
        mock_get_coordinator.return_value = mock_coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector.deploy(self.device, "config.yaml")

        mock_get_coordinator.assert_called_once_with()

        self.assertIn("does not appear to be a valid SHDR coordinator", str(cm.exception))

    @patch("openfactory.connectors.shdr.shdr_connector.deregister_asset")
    @patch("openfactory.connectors.shdr.shdr_connector.user_notify")
    @patch.object(SHDRConnector, "_get_coordinator")
    def test_tear_down(self, mock_get_coordinator, mock_notify, mock_deregister_asset):
        """ Test tear_down flow. """
        mock_coordinator = MagicMock()
        mock_get_coordinator.return_value = mock_coordinator

        self.connector.tear_down(self.device.uuid)

        # Assert coordinator lookup
        mock_get_coordinator.assert_called_once_with()

        # Assert deregistration request
        mock_coordinator.deregister_device.assert_called_once_with(
            sender_uuid='shdr-connector',
            device_uuid=self.device.uuid
        )

        # Assert asset deregistration
        mock_deregister_asset.assert_called_once_with(
            asset_uuid=self.device.uuid,
            ksqlClient=self.ksql_mock,
            bootstrap_servers="kafka:9092"
        )

        # Assert success notification
        mock_notify.success.assert_called_once_with(
            f"SHDR device {self.device.uuid} deregistered successfully"
        )

    @patch.object(SHDRConnector, "_get_coordinator")
    def test_tear_down_invalid_coordinator_raises(self, mock_get_coordinator):
        """ tear_down should raise if coordinator does not expose deregister_device. """
        mock_coordinator = MagicMock()
        mock_coordinator.deregister_device.side_effect = TypeError(
            "Invalid coordinator"
        )

        mock_get_coordinator.return_value = mock_coordinator

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down(self.device.uuid)

        mock_get_coordinator.assert_called_once_with()
        self.assertIn("does not appear to be a valid SHDR coordinator", str(cm.exception))
