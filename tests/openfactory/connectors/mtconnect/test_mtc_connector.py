import unittest
from unittest.mock import MagicMock, patch
from docker.errors import APIError
from openfactory.schemas.devices import Device
from openfactory.exceptions import OFAException
from openfactory.connectors.registry import CONNECTOR_REGISTRY
from openfactory.connectors.mtconnect.mtc_connector import MTConnectConnector
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema


class TestMTConnectConnector(unittest.TestCase):
    """
    Tests for class MTConnectConnector
    """

    def setUp(self):
        # Fake connector data
        agent_cfg = MagicMock()
        agent_cfg.ip = "192.168.1.10"
        agent_cfg.port = 5000
        agent_cfg.deploy = {"dummy": "cfg"}

        connector_cfg = MagicMock()
        connector_cfg.type = "mtconnect"
        connector_cfg.agent = agent_cfg

        # Create Device object (Pydantic)
        self.device = MagicMock(spec=Device)
        self.device.uuid = "DEVICE-123"
        self.device.uns = {"meta": "data"}
        self.device.connector = connector_cfg
        self.device.ksql_tables = ["device"]

        # Mock deployment strategy
        self.deployment_strategy = MagicMock()

        # Instance of connector
        self.connector = MTConnectConnector(
            deployment_strategy=self.deployment_strategy,
            ksqlClient=MagicMock(),
            bootstrap_servers="kafka:9092"
        )

    def test_connector_is_registered(self):
        """ MTConnectConnectorSchema should map to MTConnectConnector in CONNECTOR_REGISTRY. """
        self.assertIn(MTConnectConnectorSchema, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[MTConnectConnectorSchema], MTConnectConnector)

    @patch("openfactory.connectors.mtconnect.mtc_connector.register_asset")
    @patch("openfactory.connectors.mtconnect.mtc_connector.MTConnectAgentDeployer")
    def test_deploy(self, mock_agent_deployer_cls, mock_register_asset):
        """ Deploy should register asset, deploy agent and deploy Kafka producer. """
        mock_agent_instance = MagicMock()
        mock_agent_deployer_cls.return_value = mock_agent_instance

        # Call deploy from MTConnectConnector
        with patch.object(self.connector, "deploy_kafka_producer") as mock_kafka:
            self.connector.deploy(self.device, "config.yaml")

        # Check register_asset called once with correct args
        mock_register_asset.assert_called_once_with(
            self.device.uuid,
            uns=self.device.uns,
            asset_type="Device",
            ksqlClient=self.connector.ksql,
            docker_service=""
        )

        # Check MTConnectAgentDeployer called once with correct arguments
        mock_agent_deployer_cls.assert_called_once_with(
            self.device,
            "config.yaml",
            self.deployment_strategy,
            self.connector.ksql,
            self.connector.bootstrap_servers
        )

        # Check the agent deploy method is called
        mock_agent_instance.deploy.assert_called_once()

        # Check kafka producer deployment is triggered
        mock_kafka.assert_called_once_with(self.device)

    def test_deploy_rejects_non_mtconnect(self):
        """ Deploy should raise if connector type is not mtconnect. """
        self.device.connector.type = "opcua"
        with self.assertRaises(OFAException):
            self.connector.deploy(self.device, "config.yaml")

    def test_get_mtc_agent_url(self):
        """ Test _get_mtc_agent_url logic """
        # Case 1: IP set, port 443 → HTTPS
        self.device.connector.agent.ip = "192.168.1.10"
        self.device.connector.agent.port = 443
        url = self.connector._get_mtc_agent_url(self.device)
        self.assertEqual(url, "https://192.168.1.10:443")

        # Case 2: IP set, port != 443 → HTTP
        self.device.connector.agent.port = 5000
        url = self.connector._get_mtc_agent_url(self.device)
        self.assertEqual(url, "http://192.168.1.10:5000")

        # Case 3: IP None → service URL
        self.device.connector.agent.ip = None
        url = self.connector._get_mtc_agent_url(self.device)
        self.assertEqual(url, "http://device-123-agent:5000")

    @patch('openfactory.connectors.mtconnect.mtc_connector.config')
    @patch("openfactory.connectors.mtconnect.mtc_connector.register_asset")
    @patch("openfactory.connectors.mtconnect.mtc_connector.Asset")
    @patch("openfactory.connectors.mtconnect.mtc_connector.constraints")
    @patch("openfactory.connectors.mtconnect.mtc_connector.cpus_limit")
    @patch("openfactory.connectors.mtconnect.mtc_connector.cpus_reservation")
    def test_deploy_kafka_producer_calls_deploy(
        self,
        mock_cpus_reservation,
        mock_cpus_limit,
        mock_constraints,
        mock_asset_cls,
        mock_register_asset,
        mock_config,
    ):
        """ Test deploy_kafka_producer calls deploy correctly """
        # Setup config attributes
        mock_config.MTCONNECT_PRODUCER_IMAGE = "test_producer_image:latest"
        mock_config.KAFKA_BROKER = "test.kafka:9092"
        mock_config.OPENFACTORY_NETWORK = "test_network"

        # Setup CPU and constraints mocks
        mock_constraints.return_value = ["node.role == worker"]
        mock_cpus_limit.return_value = 1.0
        mock_cpus_reservation.return_value = 0.5

        # Setup Asset mock instance so no real Kafka produce happens
        mock_asset_instance = MagicMock()
        mock_asset_instance.add_reference_below.return_value = None
        mock_asset_instance.add_reference_above.return_value = None
        mock_asset_cls.return_value = mock_asset_instance

        mock_register_asset.return_value = None

        # Call method under test
        self.connector.deploy_kafka_producer(self.device)

        # Assert deploy call
        self.deployment_strategy.deploy.assert_called_once()
        call_kwargs = self.deployment_strategy.deploy.call_args[1]

        expected_service_name = "device-123-producer"
        expected_producer_uuid = "DEVICE-123-PRODUCER"
        expected_mtc_agent = "http://192.168.1.10:5000"

        self.assertEqual(call_kwargs["image"], "test_producer_image:latest")
        self.assertEqual(call_kwargs["name"], expected_service_name)
        self.assertEqual(call_kwargs["mode"], {"Replicated": {"Replicas": 1}})

        self.assertIn("KAFKA_BROKER=test.kafka:9092", call_kwargs["env"])
        self.assertIn(f"KAFKA_PRODUCER_UUID={expected_producer_uuid}", call_kwargs["env"])
        self.assertIn(f"MTC_AGENT={expected_mtc_agent}", call_kwargs["env"])

        self.assertEqual(call_kwargs["constraints"], ["node.role == worker"])

        expected_resources = {
            "Limits": {"NanoCPUs": int(1_000_000_000 * 1.0)},
            "Reservations": {"NanoCPUs": int(1_000_000_000 * 0.5)},
        }
        self.assertEqual(call_kwargs["resources"], expected_resources)

        self.assertEqual(call_kwargs["networks"], ["test_network"])

    @patch('openfactory.connectors.mtconnect.mtc_connector.config')
    def test_deploy_kafka_producer_deploy_raises_api_error(self, mock_config):
        """ Test deploy_kafka_producer raises APIError """
        # Make deploy raise docker APIError
        self.deployment_strategy.deploy.side_effect = APIError("Deploy failed")

        # Deploying should raise OFAException wrapping the APIError
        with self.assertRaises(OFAException) as cm:
            self.connector.deploy_kafka_producer(self.device)

        expected_service_name = self.device.uuid.lower() + '-producer'
        self.assertIn(f"Producer {expected_service_name} could not be created", str(cm.exception))
        self.assertIn("Deploy failed", str(cm.exception))

    @patch('openfactory.connectors.mtconnect.mtc_connector.register_asset')
    @patch('openfactory.connectors.mtconnect.mtc_connector.Asset')
    def test_deploy_kafka_producer_registers_asset(self, mock_asset_cls, mock_register_asset):
        """ Test deploy_kafka_producer calls register_asset correctly """

        # Call method
        self.connector.deploy_kafka_producer(self.device)

        expected_producer_uuid = self.device.uuid.upper() + '-PRODUCER'
        expected_service_name = self.device.uuid.lower() + '-producer'

        # Assert register_asset called exactly once with correct args
        mock_register_asset.assert_called_once_with(
            expected_producer_uuid,
            uns=None,
            asset_type="KafkaProducer",
            ksqlClient=self.connector.ksql,
            bootstrap_servers=self.connector.bootstrap_servers,
            docker_service=expected_service_name,
        )

    @patch("openfactory.connectors.mtconnect.mtc_connector.register_asset")
    @patch('openfactory.connectors.mtconnect.mtc_connector.Asset')
    def test_deploy_kafka_producer_adds_asset_references(self, mock_asset_cls, mock_register_asset):
        """ Test deploy_kafka_producer adds references above and below correctly """
        mock_asset_instance = MagicMock()
        mock_asset_cls.return_value = mock_asset_instance

        self.connector.deploy_kafka_producer(self.device)

        mock_asset_instance.add_reference_below.assert_called_once_with(self.device.uuid.upper() + '-PRODUCER')
        mock_asset_instance.add_reference_above.assert_called_once_with(self.device.uuid)

    @patch("openfactory.connectors.mtconnect.mtc_connector.register_asset")
    @patch('openfactory.connectors.mtconnect.mtc_connector.Asset')
    def test_deploy_kafka_producer_sends_success_notification(self, mock_asset_cls, mock_register_asset):
        """ Test deploy_kafka_producer sends a success notification with the producer UUID """
        self.deployment_strategy.deploy.return_value = None

        with patch("openfactory.connectors.mtconnect.mtc_connector.user_notify") as mock_notify:
            self.connector.deploy_kafka_producer(self.device)

            mock_notify.success.assert_called_once()
            msg = mock_notify.success.call_args[0][0]
            self.assertIn(self.device.uuid.upper() + '-PRODUCER', msg)

    @patch("openfactory.connectors.mtconnect.mtc_connector.deregister_asset")
    def test_tear_down(self, mock_deregister_asset):
        """ Test tear_down removes adapter, producer and agent """
        with patch("openfactory.connectors.mtconnect.mtc_connector.user_notify") as mock_notify:
            self.connector.tear_down("DEVICE-123")

        # Should call remove 3 times (adapter, producer, agent)
        self.assertEqual(self.deployment_strategy.remove.call_count, 3)
        mock_deregister_asset.assert_any_call("DEVICE-123-PRODUCER", ksqlClient=self.connector.ksql, bootstrap_servers=self.connector.bootstrap_servers)
        mock_deregister_asset.assert_any_call("DEVICE-123-AGENT", ksqlClient=self.connector.ksql, bootstrap_servers=self.connector.bootstrap_servers)
        mock_notify.success.assert_any_call("Kafka producer for device DEVICE-123 shut down successfully")

    @patch("openfactory.connectors.mtconnect.mtc_connector.deregister_asset")
    def test_tear_down_adapter_api_error_raises_ofaexception(self, mock_deregister_asset):
        """ Test tear_down raise OFAException on APIError when adapter is removed """

        # Raise APIError on adapter removal
        def remove_side_effect(name):
            if name.endswith("-adapter"):
                raise APIError("Adapter API error")
            return None
        self.deployment_strategy.remove.side_effect = remove_side_effect

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down("DEVICE-123")

        self.assertIn("Adapter API error", str(cm.exception))

    @patch("openfactory.connectors.mtconnect.mtc_connector.deregister_asset")
    def test_tear_down_producer_api_error_raises_ofaexception(self, mock_deregister_asset):
        """ Test tear_down raise OFAException on APIError when producer is removed """
        def side_effect(name):
            if name.endswith("-producer"):
                raise APIError("Producer API error")
            return None
        self.deployment_strategy.remove.side_effect = side_effect

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down("DEVICE-123")

        self.assertIn("Producer API error", str(cm.exception))

    @patch("openfactory.connectors.mtconnect.mtc_connector.deregister_asset")
    def test_tear_down_agent_api_error_raises_ofaexception(self, mock_deregister_asset):
        """ Test tear_down raise OFAException on APIError when agent is removed """
        def side_effect(name):
            if name.endswith("-agent"):
                raise APIError("Agent API error")
            return None
        self.deployment_strategy.remove.side_effect = side_effect

        with self.assertRaises(OFAException) as cm:
            self.connector.tear_down("DEVICE-123")

        self.assertIn("Agent API error", str(cm.exception))
