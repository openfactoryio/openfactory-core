import unittest
from unittest.mock import patch, MagicMock, mock_open
from openfactory.exceptions import OFAException
from openfactory.connectors.mtconnect.mtcagent_deployer import MTConnectAgentDeployer
from openfactory.schemas.common import Deploy
from docker.errors import APIError


class TestMTConnectAgentDeployer(unittest.TestCase):
    """
    Tests for class MTConnectAgentDeployer
    """

    def setUp(self):
        # Minimal device mock with nested attributes expected by the class
        self.device = MagicMock()
        self.device.uuid = "DEVICE-123"
        self.device.connector.agent.ip = None
        self.device.connector.agent.port = 5000
        self.device.connector.agent.device_xml = "device.xml"
        self.device.connector.agent.deploy = {}
        self.device.connector.agent.adapter = None

        self.yaml_file = "/path/to/config.yaml"
        self.ksql_client = MagicMock()
        self.bootstrap_servers = "kafka:9092"
        self.deployment_strategy = MagicMock()

        self.deployer = MTConnectAgentDeployer(
            device=self.device,
            yaml_config_file=self.yaml_file,
            deployment_strategy=self.deployment_strategy,
            ksqlClient=self.ksql_client,
            bootstrap_servers=self.bootstrap_servers,
        )

    @patch("builtins.open", new_callable=mock_open, read_data="agent config content")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.config")
    def test_load_agent_config(self, mock_config, mock_file):
        """ Test load_agent_config """
        mock_config.MTCONNECT_AGENT_CFG_FILE = "/fake/path/agent.cfg"
        content = self.deployer.load_agent_config()
        self.assertEqual(content, "agent config content")
        mock_file.assert_called_once_with("/fake/path/agent.cfg", 'r')

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.config")
    def test_load_agent_config_file_not_found_raises(self, mock_config):
        """ Test load_agent_config raises when config file does not exist """
        mock_config.MTCONNECT_AGENT_CFG_FILE = "/nonexistent/path.cfg"
        with self.assertRaises(OFAException) as cm:
            self.deployer.load_agent_config()
        self.assertIn("Could not find MTConnect agent config file", str(cm.exception))

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.open_ofa")
    @patch("os.path.isabs", return_value=False)
    @patch("os.path.dirname", return_value="/base/dir")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.split_protocol", return_value=(None, ""))
    def test_load_device_xml_relative_path(self, mock_split, mock_dirname, mock_isabs, mock_open_ofa):
        """ Test load_device_xml when no Agent IP """
        # Setup device connector agent.ip = None to trigger XML load
        self.device.connector.agent.ip = None
        self.device.connector.agent.device_xml = "relpath/device.xml"
        mock_file_handle = MagicMock()
        mock_file_handle.read.return_value = "<xml></xml>"
        mock_open_ofa.return_value.__enter__.return_value = mock_file_handle

        content = self.deployer.load_device_xml()

        expected_path = "/base/dir/relpath/device.xml"
        mock_open_ofa.assert_called_once_with(expected_path)
        self.assertEqual(content, "<xml></xml>")

    def test_load_device_xml_agent_ip_set_returns_empty(self):
        """ Test load_device_xml when Agent has an IP """
        self.device.connector.agent.ip = "192.168.1.10"
        content = self.deployer.load_device_xml()
        self.assertEqual(content, "")

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.open_ofa")
    def test_load_device_xml_open_ofa_raises(self, mock_open_ofa):
        """ Test load_device_xml raises OFAException when device_xml does not exist """
        self.device.connector.agent.ip = None
        self.device.connector.agent.device_xml = "device.xml"
        mock_open_ofa.side_effect = Exception("Failed to open")

        with self.assertRaises(OFAException) as cm:
            self.deployer.load_device_xml()
        self.assertIn("Failed to load device XML", str(cm.exception))

    @patch.object(MTConnectAgentDeployer, "load_agent_config", return_value="agent cfg content")
    @patch.object(MTConnectAgentDeployer, "load_device_xml", return_value="<xml></xml>")
    def test_prepare_env(self, mock_load_xml, mock_load_cfg):
        """ Test prepare_env when Agent has an IP """
        self.device.connector.agent.adapter = MagicMock()
        self.device.connector.agent.adapter.ip = "1.2.3.4"
        self.device.connector.agent.adapter.port = 8080

        env = self.deployer.prepare_env()

        self.assertIn("MTC_AGENT_UUID=DEVICE-123-AGENT", env)
        self.assertIn("ADAPTER_UUID=DEVICE-123", env)
        self.assertIn("ADAPTER_IP=1.2.3.4", env)
        self.assertIn("ADAPTER_PORT=8080", env)
        self.assertIn("XML_MODEL=<xml></xml>", env)
        self.assertIn("AGENT_CFG_FILE=agent cfg content", env)

    @patch.object(MTConnectAgentDeployer, "load_agent_config", return_value="agent cfg content")
    @patch.object(MTConnectAgentDeployer, "load_device_xml", return_value="<xml></xml>")
    def test_prepare_env_adapter_ip_default(self, mock_load_xml, mock_load_cfg):
        """" Test prepare_env when agent has no IP """
        # Adapter.ip missing → defaults to device uuid + '-adapter'
        self.device.connector.agent.adapter = MagicMock()
        self.device.connector.agent.adapter.ip = None
        self.device.connector.agent.adapter.port = 1234

        env = self.deployer.prepare_env()

        self.assertIn("MTC_AGENT_UUID=DEVICE-123-AGENT", env)
        self.assertIn("ADAPTER_UUID=DEVICE-123", env)
        self.assertIn(f"ADAPTER_IP={self.device.uuid.lower()}-adapter", env)
        self.assertIn("ADAPTER_PORT=1234", env)
        self.assertIn("XML_MODEL=<xml></xml>", env)
        self.assertIn("AGENT_CFG_FILE=agent cfg content", env)

    @patch.object(MTConnectAgentDeployer, "deploy_adapter")
    def test_deploy_adapter_if_needed_with_image(self, mock_deploy_adapter):
        """ Test deploy_adapter_if_needed with an adapter image """
        adapter = MagicMock()
        adapter.image = "adapter-image"
        self.device.connector.agent.adapter = adapter

        self.deployer.deploy_adapter_if_needed()

        mock_deploy_adapter.assert_called_once()

    @patch.object(MTConnectAgentDeployer, "deploy_adapter")
    def test_deploy_adapter_if_needed_without_image(self, mock_deploy_adapter):
        """ Test deploy_adapter_if_needed without adapter image """
        adapter = MagicMock()
        adapter.image = None
        self.device.connector.agent.adapter = adapter

        self.deployer.deploy_adapter_if_needed()

        mock_deploy_adapter.assert_not_called()

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.user_notify")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.constraints", return_value=["node.role == worker"])
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.cpus_limit", return_value=1.0)
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.cpus_reservation", return_value=0.5)
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.config")
    def test_deploy_adapter_success(self, mock_config, mock_cpus_reservation, mock_cpus_limit, mock_constraints, mock_notify):
        """ Test deploy_adapter """
        adapter = MagicMock()
        adapter.image = "adapter-image"
        adapter.environment = ["FOO=BAR", "BAZ=QUX"]
        adapter.deploy = {}

        self.device.connector.agent.adapter = adapter
        mock_config.OPENFACTORY_NETWORK = "test_network"

        self.deployer.deploy_adapter()

        self.deployer.deployment_strategy.deploy.assert_called_once()
        call_args, call_kwargs = self.deployer.deployment_strategy.deploy.call_args

        # Check main arguments
        assert call_kwargs["image"] == "adapter-image"
        assert call_kwargs["name"] == self.device.uuid.lower() + "-adapter"
        assert call_kwargs["mode"] == {"Replicated": {"Replicas": 1}}
        assert call_kwargs["env"] == ["FOO=BAR", "BAZ=QUX"]
        assert call_kwargs["constraints"] == ["node.role == worker"]
        assert call_kwargs["networks"] == ["test_network"]

        expected_resources = {
            "Limits": {"NanoCPUs": int(1e9 * 1.0)},
            "Reservations": {"NanoCPUs": int(1e9 * 0.5)},
        }
        assert call_kwargs["resources"] == expected_resources

        mock_notify.success.assert_called_once_with(f"Adapter {self.device.uuid.lower()}-adapter deployed successfully")

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.user_notify")
    def test_deploy_adapter_api_error(self, mock_notify):
        """ Test deploy_adapter when Docker APIError """
        adapter = MagicMock()
        adapter.image = "adapter-image"
        adapter.environment = []
        adapter.deploy = Deploy()
        self.device.connector.agent.adapter = adapter

        self.deployer.deployment_strategy.deploy.side_effect = APIError("Docker error")

        self.deployer.deploy_adapter()

        mock_notify.fail.assert_called_once()
        mock_notify.success.assert_not_called()

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.config")
    def test_deploy_service(self, mock_config):
        """ Test deploy_service """
        mock_config.MTCONNECT_AGENT_IMAGE = "agent-image"
        mock_config.OPENFACTORY_NETWORK = "ofa-network"
        mock_config.OPENFACTORY_DOMAIN = "openfactory.local"

        self.device.connector.agent.port = 5000
        self.device.connector.agent.deploy = Deploy()

        fake_env = [
            f"MTC_AGENT_UUID={self.device.uuid.upper()}-AGENT",
            "SOME_OTHER_VAR=some_value"
        ]

        with patch.object(self.deployer, "prepare_env", return_value=fake_env):
            self.deployer.deploy_service()

        self.deployment_strategy.deploy.assert_called_once()
        _, kwargs = self.deployment_strategy.deploy.call_args

        self.assertEqual(kwargs["image"], "agent-image")
        self.assertIn(f"{self.device.uuid.lower()}.agent.openfactory.local", kwargs["labels"].get(f"traefik.http.routers.{self.device.uuid.lower()}-agent.rule"))
        self.assertIn(f"{self.device.uuid.lower()}-agent", kwargs["name"])
        self.assertIn("Replicated", kwargs["mode"])
        self.assertEqual(kwargs["env"], fake_env)

    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.register_asset")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.Asset")
    def test_register_assets(self, mock_asset_cls, mock_register_asset):
        """ Test register_assets """
        mock_asset_instance = MagicMock()
        mock_asset_cls.return_value = mock_asset_instance

        self.deployer.register_assets()

        expected_agent_uuid = self.device.uuid.upper() + "-AGENT"
        expected_service_name = self.device.uuid.lower() + "-agent"

        mock_register_asset.assert_called_once_with(
            expected_agent_uuid, uns=None, asset_type="MTConnectAgent",
            ksqlClient=self.ksql_client, bootstrap_servers=self.bootstrap_servers, docker_service=expected_service_name
        )
        mock_asset_cls.assert_any_call(self.device.uuid, ksqlClient=self.ksql_client, bootstrap_servers=self.bootstrap_servers)
        mock_asset_cls.assert_any_call(expected_agent_uuid, ksqlClient=self.ksql_client, bootstrap_servers=self.bootstrap_servers)

        mock_asset_instance.add_reference_below.assert_called_once_with(expected_agent_uuid)
        mock_asset_instance.add_reference_above.assert_called_once_with(self.device.uuid)
        mock_asset_instance.add_attribute.assert_called_once()
        attr = mock_asset_instance.add_attribute.call_args[0][0]
        self.assertEqual(attr.id, "agent_port")
        self.assertEqual(attr.type, "Events")
        self.assertEqual(attr.tag, "NetworkPort")
        self.assertEqual(attr.value, self.device.connector.agent.port)

    @patch.object(MTConnectAgentDeployer, "deploy_adapter_if_needed")
    @patch.object(MTConnectAgentDeployer, "deploy_service")
    @patch.object(MTConnectAgentDeployer, "register_assets")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.user_notify")
    def test_deploy_no_agent_ip(self, mock_notify, mock_register_assets, mock_deploy_service, mock_deploy_adapter_if_needed):
        """ Test deploy when Agent has no IP """
        self.device.connector.agent.ip = None

        self.deployer.deploy()

        mock_deploy_adapter_if_needed.assert_called_once()
        mock_deploy_service.assert_called_once()
        mock_register_assets.assert_called_once()
        mock_notify.success.assert_called_once_with(f"MTConnect Agent {self.device.uuid.upper()}-AGENT deployed successfully")

    @patch.object(MTConnectAgentDeployer, "register_assets")
    @patch("openfactory.connectors.mtconnect.mtcagent_deployer.user_notify")
    def test_deploy_when_agent_has_ip(self, mock_notify, mock_register_assets):
        """ Test deploy when Agent has an IP """
        self.device.connector.agent.ip = "192.168.1.1"

        self.deployer.deploy()

        mock_register_assets.assert_called_once()
        self.deployment_strategy.deploy.assert_not_called()
        mock_notify.success.assert_called_once_with(f"MTConnect Agent {self.device.uuid.upper()}-AGENT deployed successfully")
