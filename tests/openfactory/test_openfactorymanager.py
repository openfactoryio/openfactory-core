import unittest
import docker
from unittest.mock import patch, MagicMock, call
from openfactory.schemas.supervisors import Supervisor, SupervisorAdapter
from openfactory.schemas.common import Deploy, Resources, ResourcesDefinition, Placement
from openfactory.schemas.apps import OpenFactoryAppSchema
from openfactory.schemas.filelayer.nfs_backend import NFSBackendConfig
from openfactory.schemas.filelayer.local_backend import LocalBackendConfig
from openfactory.openfactory_manager import OpenFactoryManager
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy, SwarmDeploymentStrategy
from openfactory.exceptions import OFAException


class DummyDeploymentStrategy(OpenFactoryServiceDeploymentStrategy):

    def __init__(self):
        super().__init__()

    def deploy(self, *args, **kwargs):
        pass

    def remove(self, *args, **kwargs):
        pass


def create_mock_device(uuid="device-uuid-1", uns=None, connector_type="mocked_connector", supervisor=None):
    """ Mock a Device """
    device = MagicMock()
    device.uuid = uuid
    device.uns = uns or {}

    connector = MagicMock()
    connector.type = connector_type
    device.connector = connector

    device.supervisor = supervisor

    return device


class TestOpenFactoryManager(unittest.TestCase):
    """
    Tests for class OpenFactoryManager
    """

    def setUp(self):
        self.manager = OpenFactoryManager.__new__(OpenFactoryManager)
        self.manager.ksql = MagicMock()
        self.manager.ksql.ksqldb_url = "http://mocked_ksql:8088"
        self.manager.bootstrap_servers = "mocked_bootstrap_servers"
        self.manager.deployment_strategy = MagicMock()

    @patch("openfactory.openfactory_manager.load_plugin")
    @patch("openfactory.openfactory_manager.config")
    def test_init_success(self, mock_config, mock_load_plugin):
        """ Test construction of OpenFactoryManager instance """
        # Setup config mock
        mock_config.DEPLOYMENT_PLATFORM = "dummy_platform"

        # Make load_plugin return our dummy class
        mock_load_plugin.return_value = DummyDeploymentStrategy

        # Create a mock KSQLDBClient
        mock_ksql = MagicMock()

        # Instantiate OpenFactoryManager
        manager = OpenFactoryManager(mock_ksql, "mocked_broker")

        # Assertions
        self.assertIsInstance(manager.deployment_strategy, DummyDeploymentStrategy)
        self.assertIsNotNone(manager.bootstrap_servers)
        self.assertEqual(manager.bootstrap_servers, "mocked_broker")
        self.assertIs(manager.ksql, mock_ksql)

        # deployment_strategy should be constructed twice (per current code)
        self.assertEqual(mock_load_plugin.call_count, 1)

    @patch("openfactory.openfactory_manager.load_plugin")
    @patch("openfactory.openfactory_manager.config")
    def test_init_invalid_plugin(self, mock_config, mock_load_plugin):
        """ Test construction of OpenFactoryManager instance raises TypeError when invalid deployment platform """
        # Setup config mock
        mock_config.DEPLOYMENT_PLATFORM = "bad_platform"

        # Plugin that does NOT inherit from OpenFactoryServiceDeploymentStrategy
        class NotADeploymentStrategy:
            pass

        mock_load_plugin.return_value = NotADeploymentStrategy

        with self.assertRaises(TypeError) as ctx:
            OpenFactoryManager(ksqlClient=MagicMock(), bootstrap_servers="mocked_broker")

        self.assertIn("must inherit from OpenFactoryServiceDeploymentStrategy", str(ctx.exception))

    @patch("openfactory.openfactory_manager.config")
    @patch("openfactory.openfactory_manager.register_asset")
    @patch("openfactory.openfactory_manager.Asset")
    def test_deploy_device_supervisor(self, mock_asset, mock_register_asset, mock_config):
        """ Test deploy_device_supervisor """

        # Setup config constants
        mock_config.KSQLDB_LOG_LEVEL = "INFO"
        mock_config.OPENFACTORY_NETWORK = "openfactory_net"

        # Create two distinct mocks to be returned on Asset() calls
        mock_dev_asset = MagicMock()
        mock_sup_asset = MagicMock()

        def asset_side_effect(uuid, **kwargs):
            if uuid == device.uuid:
                return mock_dev_asset
            elif uuid == f"{device.uuid.upper()}-SUPERVISOR":
                return mock_sup_asset
            else:
                return MagicMock()

        mock_asset.side_effect = asset_side_effect

        supervisor = Supervisor(
            image="mocked_supervisor_image",
            adapter=SupervisorAdapter(
                ip="127.0.0.1",
                port="5000",
                environment=["FOO=bar", "BAZ=qux"]
            ),
            deploy=Deploy(
                replicas=1,
                resources=Resources(
                    limits=ResourcesDefinition(cpus=2.0),
                    reservations=ResourcesDefinition(cpus=1.5)
                ),
                placement=Placement(
                    constraints=["node=worker"]
                )
            )
        )

        # Mock Device and nested supervisor/adapter
        device = MagicMock()
        device.uuid = "dev123"
        device.supervisor = supervisor

        # Mock Deploy object with constraints and resources
        resources = Resources(
            limits=ResourcesDefinition(cpus=2.0),
            reservations=ResourcesDefinition(cpus=1.5)
        )
        placement = Placement(constraints=["node.role=worker"])

        device.supervisor.deploy = Deploy(
            replicas=1,
            resources=resources,
            placement=placement
        )

        self.manager.deploy_device_supervisor(device)

        expected_env = [
            "SUPERVISOR_UUID=DEV123-SUPERVISOR",
            "DEVICE_UUID=dev123",
            f"KAFKA_BROKER={self.manager.bootstrap_servers}",
            f"KSQLDB_URL={self.manager.ksql.ksqldb_url}",
            "ADAPTER_IP=127.0.0.1",
            "ADAPTER_PORT=5000",
            "KSQLDB_LOG_LEVEL=INFO",
            "FOO=bar",
            "BAZ=qux"
        ]

        # Assert deployment_strategy.deploy called correctly
        self.manager.deployment_strategy.deploy.assert_called_once_with(
            image="mocked_supervisor_image",
            name="dev123-supervisor",
            mode={"Replicated": {"Replicas": 1}},
            env=expected_env,
            resources={
                "Limits": {"NanoCPUs": 2000000000},       # 2.0 * 1_000_000_000
                "Reservations": {"NanoCPUs": 1500000000}  # 1.5 * 1_000_000_000
            },
            constraints=["node.role == worker"]
        )

        # Assert register_asset called with correct supervisor uuid and params
        mock_register_asset.assert_called_once_with(
            "DEV123-SUPERVISOR",
            uns=None,
            asset_type="Supervisor",
            ksqlClient=self.manager.ksql,
            bootstrap_servers=self.manager.bootstrap_servers,
            docker_service="dev123-supervisor"
        )

        # Check that add_reference_below was called correctly on dev Asset instance
        mock_dev_asset.add_reference_below.assert_called_once_with("DEV123-SUPERVISOR")

        # Check that add_reference_above was called correctly on sup Asset instance
        mock_sup_asset.add_reference_above.assert_called_once_with("dev123")

    @patch("openfactory.openfactory_manager.config")
    @patch("openfactory.openfactory_manager.register_asset")
    @patch("openfactory.openfactory_manager.user_notify")
    def test_deploy_openfactory_application_success(self, mock_user_notify, mock_register_asset, mock_config):
        """ Test deploy_openfactory_application """

        # Setup config constants
        mock_config.KSQLDB_LOG_LEVEL = "INFO"
        mock_config.OPENFACTORY_NETWORK = "openfactory_net"

        # Application dict without explicit KSQLDB_LOG_LEVEL in environment
        app = OpenFactoryAppSchema(
            uuid="APP123",
            image="app_image",
            environment=["FOO=bar", "BAZ=qux"],
            uns={"some": "metadata"},
        )

        self.manager.deploy_openfactory_application(app)

        expected_env = [
            "APP_UUID=APP123",
            f"KAFKA_BROKER={self.manager.bootstrap_servers}",
            f"KSQLDB_URL={self.manager.ksql.ksqldb_url}",
            "DOCKER_SERVICE=app123",
            "FOO=bar",
            "BAZ=qux",
            "KSQLDB_LOG_LEVEL=INFO"
        ]

        # deployment_strategy.deploy call check
        self.manager.deployment_strategy.deploy.assert_called_once_with(
            image="app_image",
            name="app123",
            mode={"Replicated": {"Replicas": 1}},
            env=expected_env,
            mounts=[]
        )

        # register_asset call check
        mock_register_asset.assert_called_once_with(
            "APP123",
            uns={"some": "metadata"},
            asset_type="OpenFactoryApp",
            ksqlClient=self.manager.ksql,
            bootstrap_servers=self.manager.bootstrap_servers,
            docker_service="app123"
        )

        # user_notify.success called
        mock_user_notify.success.assert_called_once_with("Application APP123 deployed successfully")

    @patch("openfactory.openfactory_manager.config")
    @patch("openfactory.openfactory_manager.user_notify")
    @patch("openfactory.openfactory_manager.register_asset")
    def test_deploy_openfactory_application_includes_storage_env(self, mock_register_asset, mock_user_notify, mock_config):
        """ Test that deploy_openfactory_application includes STORAGE in env when storage is provided """

        mock_config.KSQLDB_LOG_LEVEL = "INFO"
        mock_config.OPENFACTORY_NETWORK = "openfactory_net"

        nfs_config = NFSBackendConfig(
            type="nfs",
            server="nfs.example.com",
            remote_path="/data/share",
            mount_point="/mnt/data",
            mount_options=["rw"]
        )

        app = OpenFactoryAppSchema(
            uuid="APP_WITH_STORAGE",
            image="app_image",
            environment=["FOO=bar"],
            uns=None,
            storage=nfs_config,
        )

        # Patch backend class so we don't try to mount anything
        with patch("openfactory.filelayer.nfs_backend.NFSBackend") as MockBackend:
            backend_instance = MockBackend.return_value
            backend_instance.get_mount_spec.return_value = None

            self.manager.deploy_openfactory_application(app)

        # Extract env from the call to deploy
        deploy_call = self.manager.deployment_strategy.deploy.call_args
        env_list = deploy_call.kwargs["env"]

        # STORAGE env var should be present and contain serialized config
        storage_env = next((e for e in env_list if e.startswith("STORAGE=")), None)
        self.assertIsNotNone(storage_env, "Expected STORAGE env var to be included")
        self.assertIn('"type": "nfs"', storage_env)
        self.assertIn('"server": "nfs.example.com"', storage_env)
        self.assertIn('"/mnt/data"', storage_env)

        # Other env vars should still be present
        self.assertIn("APP_UUID=APP_WITH_STORAGE", env_list)
        self.assertIn("FOO=bar", env_list)

        mock_user_notify.success.assert_called_once_with(
            "Application APP_WITH_STORAGE deployed successfully"
        )

    @patch("openfactory.openfactory_manager.config")
    @patch("openfactory.openfactory_manager.user_notify")
    @patch("openfactory.openfactory_manager.register_asset")
    def test_deploy_with_nfs_storage_calls_backend(self, mock_register_asset, mock_user_notify, mock_config):
        """ Test that deploying an app with NFS storage calls the backend's get_mount_spec """

        mock_config.KSQLDB_LOG_LEVEL = "INFO"
        mock_config.OPENFACTORY_NETWORK = "openfactory_net"

        nfs_config = NFSBackendConfig(
            type="nfs",
            server="nfs.example.com",
            remote_path="/data/share",
            mount_point="/mnt/data",
            mount_options=["rw"]
        )

        app = OpenFactoryAppSchema(
            uuid="APP123",
            image="app_image",
            environment=None,
            uns=None,
            storage=nfs_config
        )

        with patch("openfactory.filelayer.nfs_backend.NFSBackend") as MockBackend:
            backend_instance = MockBackend.return_value
            mount_spec = {"source": "nfs.example.com:/data/share", "target": "/mnt/data", "type": "volume"}
            backend_instance.get_mount_spec.return_value = mount_spec

            self.manager.deploy_openfactory_application(app)

            # Backend instantiation and get_mount_spec called
            MockBackend.assert_called_once_with(nfs_config)
            backend_instance.get_mount_spec.assert_called_once()

            # Deployment strategy received the mount spec
            self.manager.deployment_strategy.deploy.assert_called_once()
            deploy_kwargs = self.manager.deployment_strategy.deploy.call_args.kwargs
            self.assertIn("mounts", deploy_kwargs)
            self.assertIn(mount_spec, deploy_kwargs["mounts"])

    @patch("openfactory.openfactory_manager.config")
    @patch("openfactory.openfactory_manager.user_notify")
    def test_deploy_openfactory_application_deploy_fails(self, mock_user_notify, mock_config):
        """ Test deploy_openfactory_application raises on Docker APIError """
        mock_config.KSQLDB_LOG_LEVEL = "INFO"
        mock_config.OPENFACTORY_NETWORK = "openfactory_net"

        # Simulate deployment_strategy.deploy raising docker.errors.APIError
        class DummyAPIError(docker.errors.APIError):
            pass

        self.manager.deployment_strategy.deploy.side_effect = DummyAPIError("deployment failed")

        app = OpenFactoryAppSchema(
            uuid="APP123",
            image="app_image",
            environment=None,
            uns=None,
            storage=None
        )

        self.manager.deploy_openfactory_application(app)

        # Check user_notify.fail called with correct message
        mock_user_notify.fail.assert_called_once()
        args, _ = mock_user_notify.fail.call_args
        self.assertIn("Application APP123 could not be deployed", args[0])

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    def test_local_backend_cannot_be_used_with_swarm(self, mock_isdir, mock_exists):
        """ LocalBackend should raise an error when deployed with SwarmDeploymentStrategy """

        # Create a mocked OpenFactoryApp with aLocalBackendConfig
        app = MagicMock()
        app.uuid = "APP1"
        app.environment = []
        app.storage = LocalBackendConfig(type="local",
                                         local_path="/tmp/host",
                                         mount_point="/mnt/data")

        # Assign a SwarmDeploymentStrategy instance to the manager
        self.manager.deployment_strategy = SwarmDeploymentStrategy()

        # Expect ValueError because LocalBackend is incompatible with Swarm
        with self.assertRaises(ValueError) as cm:
            self.manager.deploy_openfactory_application(app)

        self.assertIn("LocalBackend cannot be used with SwarmDeploymentStrategy", str(cm.exception))

    @patch('openfactory.openfactory_manager.get_apps_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_apps_from_config_file(self, mock_uns_schema_class, mock_user_notify, mock_get_apps_from_config_file):
        """ Test deploy_apps_from_config_file """
        # Mock UNS schema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock the OpenFactoryManager instance
        manager = OpenFactoryManager(ksqlClient=MagicMock(), bootstrap_servers='mocked_bootstrap_servers')
        manager.applications_uuid = MagicMock(return_value=['existing-app-uuid'])
        manager.deploy_openfactory_application = MagicMock()

        mock_get_apps_from_config_file.return_value = {
            'App1': OpenFactoryAppSchema(uuid='new-app-uuid', image='app1-image', environment=None, uns=None),
            'App2': OpenFactoryAppSchema(uuid='existing-app-uuid', image='app2-image', environment=None, uns=None)
        }

        # Call the method to test
        manager.deploy_apps_from_config_file('dummy_config.yaml')

        # Assertions
        mock_get_apps_from_config_file.assert_called_once_with('dummy_config.yaml', mock_uns_instance)
        mock_user_notify.info.assert_any_call('App1:')
        mock_user_notify.info.assert_any_call('App2:')
        mock_user_notify.info.assert_any_call('Application existing-app-uuid exists already and was not deployed')
        manager.deploy_openfactory_application.assert_called_once_with(
            mock_get_apps_from_config_file.return_value['App1']
        )

    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_connector_type_unknown(self, mock_uns_schema_class, mock_user_notify, mock_get_devices_from_config_file):
        """ Test deploy_apps_from_config_file with an unknown connector """

        # Mock UNS schema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Configure OpenFactoryManager instance
        self.manager.connectors = {
            "known_connector": MagicMock()
        }
        self.manager.devices_uuid = MagicMock(return_value=[])  # no devices deployed yet
        self.manager.deploy_device_supervisor = MagicMock()

        # Mock device with unknown connector type
        device_mock = MagicMock()
        device_mock.uuid = "dev001"
        device_mock.connector.type = "unknown_connector"

        mock_get_devices_from_config_file.return_value = {
            "Device1": device_mock
        }

        # Call the method under test
        self.manager.deploy_devices_from_config_file("dummy_devices.yaml")

        # Check that warning is raised for unknown connector type
        mock_user_notify.warning.assert_called_once_with(
            "Device dev001 has an unknown connector unknown_connector"
        )

        # Ensure that no deployment or supervisor deploy is called
        self.manager.connectors["known_connector"].deploy.assert_not_called()
        self.manager.deploy_device_supervisor.assert_not_called()
        mock_user_notify.success.assert_not_called()

    @patch('openfactory.openfactory_manager.register_device_connector')
    @patch('openfactory.openfactory_manager.build_connector')
    @patch('openfactory.openfactory_manager.config')
    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_from_config_file_success(
        self,
        mock_uns_schema_class,
        mock_user_notify,
        mock_get_devices,
        mock_config,
        mock_build_connector,
        mock_register_device_connector
    ):
        """ Test deploy_devices_from_config_file """

        mock_config.OPENFACTORY_UNS_SCHEMA = "mocked_schema"
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock devices
        devices_dict = {
            "Device1": create_mock_device(
                uuid="device-uuid-1",
                connector_type="mocked_connector",
                supervisor=MagicMock(),
                uns={"uns_id": "some/mocked/path"}),
            "Device2": create_mock_device(
                uuid="device-uuid-2",
                connector_type="mocked_connector",
                supervisor=None,
                uns={"uns_id": "some/other/path"})
        }
        mock_get_devices.return_value = devices_dict

        # Mock build_connector to return a connector with a deploy method
        mock_connector_instance = MagicMock()
        mock_build_connector.return_value = mock_connector_instance

        self.manager.devices_uuid = MagicMock(return_value=[])
        self.manager.deploy_device_supervisor = MagicMock()

        # Call method under test
        self.manager.deploy_devices_from_config_file("mock_devices.yaml")

        # Check UNS schema created correctly
        mock_uns_schema_class.assert_called_once_with(schema_yaml_file="mocked_schema")
        mock_get_devices.assert_called_once_with("mock_devices.yaml", mock_uns_instance)

        # Check each device deployment
        for device_key, device_obj in devices_dict.items():
            mock_user_notify.info.assert_any_call(f"{device_key} - {device_obj.uuid}:")
            mock_connector_instance.deploy.assert_any_call(device_obj, "mock_devices.yaml")
            self.manager.deploy_device_supervisor.assert_any_call(device_obj)
            mock_register_device_connector.assert_any_call(device_obj, self.manager.ksql)
            mock_user_notify.success.assert_any_call(f"Device {device_obj.uuid} deployed successfully")

    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_from_config_file_no_devices(self, mock_uns_schema_class, mock_user_notify, mock_get_devices):
        """ Test deploy_devices_from_config_file when no devices are found in the config file """
        # Mock dependencies
        mock_get_devices.return_value = None
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Call the method
        self.manager.deploy_devices_from_config_file("mock_config.yaml")

        # Assertions
        mock_get_devices.assert_called_once_with("mock_config.yaml", mock_uns_instance)
        mock_user_notify.info.assert_not_called()

    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_from_config_file_device_exists(self, mock_uns_schema_class, mock_user_notify, mock_get_devices):
        """ Test deploy_devices_from_config_file when device already exists """

        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance
        device_mock = create_mock_device(uuid="device-uuid-1")
        mock_get_devices.return_value = {"Device1": device_mock}
        self.manager.devices_uuid = MagicMock(return_value=["device-uuid-1"])

        # Call the method
        self.manager.deploy_devices_from_config_file("mock_config.yaml")

        # Assertions
        mock_get_devices.assert_called_once_with("mock_config.yaml", mock_uns_instance)
        mock_user_notify.info.assert_any_call("Device device-uuid-1 exists already and was not deployed")

    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_from_config_file_invalid_uns_schema(self, mock_uns_schema_class, mock_user_notify, mock_get_devices):
        """ Test deploy_devices_from_config_file aborts when UNSSchema raises ValueError """

        # Setup the UNSSchema constructor to raise ValueError (simulate invalid schema)
        mock_uns_schema_class.side_effect = ValueError("Mocked error")

        # Call the method under test
        self.manager.deploy_devices_from_config_file("mock_config.yaml")

        # Assertions
        mock_uns_schema_class.assert_called_once_with(schema_yaml_file=unittest.mock.ANY)
        mock_user_notify.fail.assert_called_once()
        fail_msg = mock_user_notify.fail.call_args[0][0]
        assert "Mocked error" in fail_msg
        mock_get_devices.assert_not_called()

    @patch('openfactory.openfactory_manager.register_device_connector')
    @patch('openfactory.openfactory_manager.build_connector')
    @patch('openfactory.openfactory_manager.config')
    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_devices_from_config_file_with_ofa_exception(
        self,
        mock_uns_schema_class,
        mock_user_notify,
        mock_get_devices,
        mock_config,
        mock_build_connector,
        mock_register_device_connector
    ):
        """ Test deploy_devices_from_config_file when connector.deploy raises OFAException """
        mock_config.OPENFACTORY_UNS_SCHEMA = "mocked_schema"
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Create a device mock
        device = create_mock_device(uuid="device-uuid-err", connector_type="mocked_connector")
        mock_get_devices.return_value = {"DeviceErr": device}

        # Mock connector instance that raises OFAException
        mock_connector_instance = MagicMock()
        mock_connector_instance.deploy.side_effect = OFAException("Simulated deployment failure")
        mock_build_connector.return_value = mock_connector_instance

        self.manager.devices_uuid = MagicMock(return_value=[])
        self.manager.deploy_device_supervisor = MagicMock()

        # Run method
        self.manager.deploy_devices_from_config_file("mock_devices.yaml")

        # Ensure connector.deploy was attempted
        mock_connector_instance.deploy.assert_called_once_with(device, "mock_devices.yaml")

        # Ensure user_notify.fail was called with OFAException message
        mock_user_notify.fail.assert_called_once()
        fail_msg = mock_user_notify.fail.call_args[0][0]
        self.assertIn("Device device-uuid-err not deployed", fail_msg)
        self.assertIn("Simulated deployment failure", fail_msg)

        # Ensure no success message and no supervisor deploy
        mock_user_notify.success.assert_not_called()
        self.manager.deploy_device_supervisor.assert_not_called()
        mock_register_device_connector.assert_not_called()

    @patch('openfactory.openfactory_manager.get_apps_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_apps_from_config_file_no_apps(self, mock_uns_schema_class, mock_user_notify, mock_get_apps_from_config_file):
        """ Test deploy_apps_from_config_file when no apps are found in the config file """
        # Mock UNS schema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock the YAML configuration file loading to return None
        mock_get_apps_from_config_file.return_value = None

        # Call the method to test
        self.manager.deploy_apps_from_config_file('dummy_config.yaml')

        # Assertions
        mock_get_apps_from_config_file.assert_called_once_with('dummy_config.yaml', mock_uns_instance)
        mock_user_notify.info.assert_not_called()

    @patch('openfactory.openfactory_manager.get_apps_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_deploy_apps_from_config_file_invalid_uns_schema(self, mock_uns_schema_class, mock_user_notify, mock_get_apps):
        """ Test deploy_apps_from_config_file aborts when UNSSchema raises ValueError """

        # Simulate UNSSchema constructor raising ValueError due to invalid schema
        mock_uns_schema_class.side_effect = ValueError("Mocked error")

        # Call the method under test
        self.manager.deploy_apps_from_config_file("mock_apps_config.yaml")

        # Assertions
        mock_uns_schema_class.assert_called_once_with(schema_yaml_file=unittest.mock.ANY)
        mock_user_notify.fail.assert_called_once()
        fail_msg = mock_user_notify.fail.call_args[0][0]
        assert "Mocked error" in fail_msg
        mock_get_apps.assert_not_called()

    @patch('openfactory.openfactory_manager.deregister_device_connector')
    @patch('openfactory.openfactory_manager.build_connector')
    @patch('openfactory.openfactory_manager.deregister_asset')
    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_shut_down_devices_from_config_file(
        self,
        mock_uns_schema_class, mock_user_notify,
        mock_get_devices_from_config_file,
        mock_deregister_asset,
        mock_build_connector,
        mock_deregister_device_connector
    ):
        """ Test shut_down_devices_from_config_file """

        # Mock UNSSchema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock devices returned by config file
        devices_dict = {
            "Device1": create_mock_device(uuid="device-uuid-1", connector_type="mocked_connector"),
            "Device2": create_mock_device(uuid="device-uuid-3", connector_type="mocked_connector")
        }
        mock_get_devices_from_config_file.return_value = devices_dict

        # Mock build_connector to return a connector with a tear_down method
        mock_connector_instance = MagicMock()
        mock_build_connector.return_value = mock_connector_instance

        # Mock devices already deployed in manager
        deployed_devices = [
            MagicMock(asset_uuid="device-uuid-1"),
            MagicMock(asset_uuid="device-uuid-2")
        ]
        self.manager.devices = MagicMock(return_value=deployed_devices)

        # Mock deployment_strategy
        self.manager.deployment_strategy = MagicMock()
        self.manager.ksql = MagicMock()
        self.manager.bootstrap_servers = "dummy-bootstrap"

        # Call method under test
        self.manager.shut_down_devices_from_config_file("dummy_config.yaml")

        # Config was loaded
        mock_get_devices_from_config_file.assert_called_once_with("dummy_config.yaml", uns_schema=mock_uns_instance)

        # Info messages
        mock_user_notify.info.assert_any_call("Device1:")
        mock_user_notify.info.assert_any_call("Device2:")
        mock_user_notify.info.assert_any_call("No device device-uuid-3 deployed in OpenFactory")

        # Connector teardown called for deployed device only
        mock_connector_instance.tear_down.assert_called_once_with("device-uuid-1")

        # Deployment strategy remove should be called for the supervisor of device-uuid-1
        self.manager.deployment_strategy.remove.assert_called_once_with("device-uuid-1-supervisor")

        # Assets should be deregistered exactly twice (supervisor + device itself)
        expected_calls = [
            call("DEVICE-UUID-1-SUPERVISOR",
                 ksqlClient=self.manager.ksql,
                 bootstrap_servers=self.manager.bootstrap_servers),
            call("device-uuid-1",
                 ksqlClient=self.manager.ksql,
                 bootstrap_servers=self.manager.bootstrap_servers),
        ]
        mock_deregister_asset.assert_has_calls(expected_calls, any_order=False)
        assert mock_deregister_asset.call_count == 2  # strict check

        # Check deregister_device_connector
        expected_calls = [
            call("device-uuid-1",
                 bootstrap_servers=self.manager.bootstrap_servers),
        ]
        mock_deregister_device_connector.assert_has_calls(expected_calls, any_order=True)
        assert mock_deregister_device_connector.call_count == 1

        # Success messages
        mock_user_notify.success.assert_any_call("Supervisor for device device-uuid-1 shut down successfully")
        mock_user_notify.success.assert_any_call("Device device-uuid-1 shut down successfully")

    @patch('openfactory.openfactory_manager.get_devices_from_config_file')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_shut_down_devices_from_config_file_no_devices(self, mock_uns_schema_class, mock_get_devices_from_config_file):
        """ Test shut_down_devices_from_config_file when no devices are found in the config file """
        # Mock UNSSchema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Configure the OpenFactoryManager instance
        self.manager.devices = MagicMock(return_value=[])
        self.manager.tear_down_device = MagicMock()

        # Mock the devices returned by the config file
        mock_get_devices_from_config_file.return_value = None

        # Call the method
        self.manager.shut_down_devices_from_config_file("dummy_config.yaml")

        # Assertions
        mock_get_devices_from_config_file.assert_called_once_with("dummy_config.yaml", uns_schema=mock_uns_instance)
        self.manager.tear_down_device.assert_not_called()

    @patch("openfactory.openfactory_manager.Asset")
    @patch("openfactory.openfactory_manager.user_notify")
    @patch("openfactory.openfactory_manager.deregister_asset")
    def test_tear_down_application(self, mock_deregister_asset, mock_user_notify, MockAsset):
        """ Test tear_down_application """

        # Mock Asset instance and its DockerService value
        mock_app_instance = MagicMock()
        mock_app_instance.DockerService.value = "mock-service-name"
        MockAsset.return_value = mock_app_instance

        # Call the method under test
        app_uuid = 'app-uuid-123'
        self.manager.tear_down_application(app_uuid)

        # Check that the correct services were removed
        self.manager.deployment_strategy.remove.assert_called_once_with("mock-service-name")

        # Check that the correct notifications were sent
        mock_user_notify.success.assert_any_call(f"OpenFactory application {app_uuid} shut down successfully")

        # Ensure deregister_asset was called
        mock_deregister_asset.assert_any_call(app_uuid,
                                              ksqlClient=self.manager.ksql,
                                              bootstrap_servers=self.manager.bootstrap_servers)

    @patch("openfactory.openfactory_manager.Asset")
    @patch("openfactory.openfactory_manager.user_notify")
    @patch("openfactory.openfactory_manager.deregister_asset")
    def test_tear_down_application_no_docker_service(self, mock_deregister_asset, mock_user_notify, MockAsset):
        """ Test tear_down_application when application is not deployed as a Docker service """

        # Mock Asset instance and its DockerService value
        mock_app_instance = MagicMock()
        mock_app_instance.DockerService.value = "mock-service-name"
        MockAsset.return_value = mock_app_instance

        # Set remove() to raise NotFound error
        self.manager.deployment_strategy.remove.side_effect = docker.errors.NotFound("Service not found")

        app_uuid = 'app-uuid-123'
        self.manager.tear_down_application(app_uuid)

        # Ensure deregister_asset was called
        mock_deregister_asset.assert_any_call(app_uuid,
                                              ksqlClient=self.manager.ksql,
                                              bootstrap_servers=self.manager.bootstrap_servers)

        # No success message
        mock_user_notify.assert_not_called()

    @patch("openfactory.openfactory_manager.Asset")
    @patch("openfactory.openfactory_manager.user_notify")
    @patch("openfactory.openfactory_manager.deregister_asset")
    def test_tear_down_application_docker_api_error(self, mock_deregister_asset, mock_user_notify, MockAsset):
        """ Test tear_down_application handels Docker API errors """

        # Mock Asset instance and its DockerService value
        mock_app_instance = MagicMock()
        mock_app_instance.DockerService.value = "mock-service-name"
        MockAsset.return_value = mock_app_instance

        # Setup deployment_strategy.remove to raise APIError
        self.manager.deployment_strategy.remove.side_effect = docker.errors.APIError("Docker error")

        # Call the method to test
        with self.assertRaises(OFAException):
            self.manager.tear_down_application('app-uuid-123')

        # Ensure deregister_asset was not called
        mock_deregister_asset.assert_not_called()

        # No success message
        mock_user_notify.assert_not_called()

    @patch('openfactory.openfactory_manager.get_apps_from_config_file')
    @patch('openfactory.openfactory_manager.user_notify')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_shut_down_apps_from_config_file(self, mock_uns_schema_class, mock_user_notify, mock_get_apps_from_config_file):
        """ Test shut_down_apps_from_config_file """
        # Mock UNS schema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock the OpenFactoryManager instance
        ksqlMock = MagicMock()
        ksqlMock.ksqldb_url = "mock_ksqldb_url"
        manager = OpenFactoryManager(ksqlClient=ksqlMock, bootstrap_servers='mokded_bootstrap_servers')
        manager.applications_uuid = MagicMock(return_value=['app-uuid-1', 'app-uuid-2'])
        manager.tear_down_application = MagicMock()

        # Mock the YAML config file content
        mock_get_apps_from_config_file.return_value = {
            'App1': OpenFactoryAppSchema(uuid='app-uuid-1', image='app1-image', environment=None, uns=None),
            'App2': OpenFactoryAppSchema(uuid='app-uuid-3', image='app2-image', environment=None, uns=None)
        }

        # Call the method
        manager.shut_down_apps_from_config_file('dummy_config.yaml')

        # Assertions
        mock_get_apps_from_config_file.assert_called_once_with('dummy_config.yaml', mock_uns_instance)
        mock_user_notify.info.assert_any_call('App1:')
        mock_user_notify.info.assert_any_call('App2:')
        mock_user_notify.info.assert_any_call('No application app-uuid-3 deployed in OpenFactory')
        manager.tear_down_application.assert_called_once_with('app-uuid-1')

    @patch('openfactory.openfactory_manager.get_apps_from_config_file')
    @patch('openfactory.openfactory_manager.UNSSchema')
    def test_shut_down_apps_from_config_file_no_apps(self, mock_uns_schema_class, mock_get_apps_from_config_file):
        """ Test shut_down_apps_from_config_file when no apps are found in the config file """
        # Mocke UNS schema
        mock_uns_instance = MagicMock()
        mock_uns_schema_class.return_value = mock_uns_instance

        # Mock the OpenFactoryManager instance
        ksqlMock = MagicMock()
        ksqlMock.ksqldb_url = "mock_ksqldb_url"
        manager = OpenFactoryManager(ksqlClient=ksqlMock, bootstrap_servers='mokded_bootstrap_servers')

        # Mock the YAML config file returning None
        mock_get_apps_from_config_file.return_value = None

        # Call the method
        manager.shut_down_apps_from_config_file('dummy_config.yaml')

        # Assertions
        mock_get_apps_from_config_file.assert_called_once_with('dummy_config.yaml', mock_uns_instance)
