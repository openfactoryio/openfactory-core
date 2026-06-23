import unittest
from unittest.mock import patch, MagicMock
from docker.types import DriverConfig
from openfactory.openfactory_deploy_strategy import SwarmDeploymentStrategy, LocalDockerDeploymentStrategy


class TestSwarmDeploymentStrategy(unittest.TestCase):
    """
    Unit tests for class SwarmDeploymentStrategy
    """

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy(self, mock_docker_client):
        """ Test Docker Swarm deploy method """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            labels={"role": "web"},
            command="run",
            ports={8080: 80},
            networks=["net1"],
            constraints=["node.role==manager"],
            resources={"Limits": {"NanoCPUs": 500000000}},
            mode={"Replicated": {"Replicas": 2}}
        )

        mock_docker_client.api.create_service.assert_called_once()

        args, kwargs = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertEqual(task["ContainerSpec"]["Image"], "test-image")
        self.assertEqual(task["ContainerSpec"]["Env"], ["ENV=prod"])
        self.assertEqual(task["ContainerSpec"]["Labels"], {"role": "web"})
        self.assertEqual(task["ContainerSpec"]["Command"], ["run"])
        self.assertEqual(task["Resources"]["Limits"]["NanoCPUs"], 500000000)
        self.assertEqual(task["Placement"]["Constraints"], ["node.role==manager"])
        self.assertEqual(kwargs["name"], "test-service")
        self.assertEqual(kwargs["networks"], ["net1"])
        self.assertEqual(kwargs["mode"], {"Replicated": {"Replicas": 2}})
        self.assertEqual(kwargs["labels"], {"role": "web"})
        self.assertEqual(
            kwargs["endpoint_spec"],
            {
                "Ports": [
                    {
                        "Protocol": "tcp",
                        "PublishedPort": 8080,
                        "TargetPort": 80
                    }
                ]
            }
        )

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_accepts_image_pull_policy(self, mock_docker_client):
        """ Test Docker Swarm deploy accepts image_pull_policy parameter """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            image_pull_policy="always",
            name="test-service",
            env=["ENV=prod"]
        )

        mock_docker_client.api.create_service.assert_called_once()

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_with_runtime_user(self, mock_docker_client):
        """ Test Docker Swarm deploy with runtime user """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            user="1234:5678"
        )

        mock_docker_client.api.create_service.assert_called_once()

        args, _ = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertEqual(task["ContainerSpec"]["User"], "1234:5678")

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_with_mounts(self, mock_docker_client):
        """ Test Docker Swarm deploy with mounts """

        strategy = SwarmDeploymentStrategy()

        mounts = [{
            "Target": "/mnt/data",
            "Source": "nfs-prover3018-desk",
            "Type": "volume",
            "VolumeOptions": {
                "DriverConfig": {
                    "Name": "local",
                    "Options": {
                        "type": "nfs",
                        "o": "addr=192.168.0.100,rw",
                        "device": ":/nfs/desk"
                    }
                }
            }
        }]

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            mounts=mounts
        )

        mock_docker_client.api.create_service.assert_called_once()

        args, _ = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertEqual(task["ContainerSpec"]["Mounts"], mounts)

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_with_open_files(self, mock_docker_client):
        """ Test Docker Swarm deploy with open_files limit """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            open_files=5000
        )

        args, _ = mock_docker_client.api.create_service.call_args

        task = args[0]

        self.assertEqual(
            task["ContainerSpec"]["Ulimits"],
            [{
                "Name": "nofile",
                "Soft": 5000,
                "Hard": 5000
            }]
        )

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_without_open_files(self, mock_docker_client):
        """ Test Docker Swarm deploy without open_files limit """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"]
        )

        args, _ = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertNotIn("Ulimits", task["ContainerSpec"])

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_remove(self, mock_docker_client):
        """ Test Docker Swarm remove method """
        mock_service = MagicMock()
        mock_docker_client.services.get.return_value = mock_service

        strategy = SwarmDeploymentStrategy()
        strategy.remove("test-service")

        mock_docker_client.services.get.assert_called_once_with("test-service")
        mock_service.remove.assert_called_once_with()

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_without_resources(self, mock_docker_client):
        """ Test Docker Swarm deploy without resources """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            resources=None
        )

        mock_docker_client.api.create_service.assert_called_once()

        args, _ = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertNotIn("Resources", task)

    @patch("openfactory.openfactory_deploy_strategy.dal.docker_client")
    def test_swarm_deploy_without_constraints(self, mock_docker_client):
        """ Test Docker Swarm deploy without constraints """

        strategy = SwarmDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-service",
            env=["ENV=prod"],
            constraints=None
        )

        mock_docker_client.api.create_service.assert_called_once()

        args, _ = mock_docker_client.api.create_service.call_args
        task = args[0]

        self.assertNotIn("Placement", task)


class TestLocalDockerDeploymentStrategy(unittest.TestCase):
    """
    Unit tests for class LocalDockerDeploymentStrategy
    """

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy(self, mock_from_env):
        """ Test local Docker deploy method using direct docker.from_env() """
        mock_docker_client = MagicMock()
        mock_run = MagicMock()
        mock_docker_client.containers.run = mock_run
        mock_from_env.return_value = mock_docker_client

        strategy = LocalDockerDeploymentStrategy()
        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"],
            labels={"type": "api"},
            command="start",
            ports={8080: 80},
            networks=["bridge"],
            resources={"Limits": {"NanoCPUs": 1000000000}}
        )

        mock_run.assert_called_once()
        args, kwargs = mock_run.call_args
        self.assertEqual(kwargs["image"], "test-image")
        self.assertEqual(kwargs["name"], "test-container")
        self.assertEqual(kwargs["environment"], ["ENV=dev"])
        self.assertEqual(kwargs["command"], "start")
        self.assertTrue(kwargs["detach"])
        self.assertEqual(kwargs["ports"], {"80/tcp": 8080})
        self.assertEqual(kwargs["network"], "bridge")
        self.assertEqual(kwargs["labels"], {"type": "api"})
        self.assertEqual(kwargs["nano_cpus"], 1000000000)
        self.assertIn("ulimits", kwargs)
        self.assertIsNone(kwargs["ulimits"])

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_image_pull_policy_always(self, mock_from_env):
        """ Test local Docker deploy always pulls image before deployment """

        mock_client = MagicMock()
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            image_pull_policy="always",
            name="test-container",
            env=["ENV=dev"]
        )

        mock_client.images.pull.assert_called_once_with("test-image")
        mock_client.containers.run.assert_called_once()

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_image_pull_policy_missing(self, mock_from_env):
        """ Test local Docker deploy does not explicitly pull image when policy is missing """

        mock_client = MagicMock()
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            image_pull_policy="missing",
            name="test-container",
            env=["ENV=dev"]
        )

        mock_client.images.pull.assert_not_called()
        mock_client.containers.run.assert_called_once()

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_with_runtime_user(self, mock_from_env):
        """ Test local Docker deploy with runtime user """

        mock_client = MagicMock()
        mock_run = MagicMock()

        mock_client.containers.run = mock_run
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"],
            user="1234:5678"
        )

        mock_run.assert_called_once()

        _, kwargs = mock_run.call_args

        self.assertIn("user", kwargs)
        self.assertEqual(kwargs["user"], "1234:5678")

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_remove(self, mock_from_env):
        """ Test local Docker remove performs a graceful shutdown before removal. """
        mock_docker_client = MagicMock()
        mock_container = MagicMock()
        mock_docker_client.containers.get.return_value = mock_container
        mock_from_env.return_value = mock_docker_client

        strategy = LocalDockerDeploymentStrategy()
        strategy.remove("test-container")

        mock_docker_client.containers.get.assert_called_once_with("test-container")

        # Verify graceful shutdown sequence
        self.assertEqual(
            mock_container.method_calls,
            [
                unittest.mock.call.stop(),
                unittest.mock.call.remove(),
            ]
        )

        mock_container.stop.assert_called_once_with()
        mock_container.remove.assert_called_once_with()

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    @patch("openfactory.openfactory_deploy_strategy.Mount")
    def test_local_deploy_with_mounts(self, mock_mount_class, mock_from_env):
        """ Test local Docker deploy with mounts """
        # Mock Docker client
        mock_client = MagicMock()
        mock_run = MagicMock()
        mock_client.containers.run = mock_run
        mock_from_env.return_value = mock_client

        # Initialize strategy
        strategy = LocalDockerDeploymentStrategy()

        # Example mount spec
        mounts = [
            {"Target": "/mnt/data", "Source": "/host/data", "Type": "bind", "ReadOnly": False}
        ]

        # Deploy container
        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"],
            networks=None,
            mounts=mounts
        )

        # Check that Mount objects were created correctly
        mock_mount_class.assert_called_once_with(
            target="/mnt/data",
            source="/host/data",
            type="bind",
            read_only=False
        )

        # Check that mounts passed to containers.run
        _, kwargs = mock_run.call_args
        self.assertIn("mounts", kwargs)
        self.assertEqual(len(kwargs["mounts"]), 1)
        self.assertEqual(kwargs["mounts"][0], mock_mount_class.return_value)

    @patch("openfactory.openfactory_deploy_strategy.Mount")
    def test_swarm_mount_to_container_mount_volume(self, mock_mount_class):
        """ Test that swarm_mount_to_container_mount converts a volume dict to Mount correctly. """
        strategy = LocalDockerDeploymentStrategy()

        mount_dict = {
            "Type": "volume",
            "Source": "my_volume",
            "Target": "/mnt",
            "ReadOnly": True,
            "VolumeOptions": {
                "DriverConfig": {
                    "Name": "local",
                    "Options": {
                        "type": "nfs",
                        "o": "addr=10.0.0.1,rw,nfsvers=4",
                        "device": ":/data"
                    }
                }
            }
        }

        mount_obj = strategy.swarm_mount_to_container_mount(mount_dict)

        # Assert Mount constructor was called with correct arguments
        mock_mount_class.assert_called_once_with(
            target="/mnt",
            source="my_volume",
            type="volume",
            read_only=True,
            driver_config=DriverConfig(
                name="local",
                options={
                    "type": "nfs",
                    "o": "addr=10.0.0.1,rw,nfsvers=4",
                    "device": ":/data"
                }
            )
        )

        # The returned object is the mocked Mount
        self.assertEqual(mount_obj, mock_mount_class.return_value)

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_additional_networks(self, mock_from_env):
        """ Test that additional networks are connected after container deployment """
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_network = MagicMock()
        mock_client.containers.run.return_value = mock_container
        # networks.get() returns the mocked network
        mock_client.networks.get.return_value = mock_network
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        # Case: multiple networks
        networks = ["first-net", "second-net", "third-net"]

        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"],
            networks=networks
        )

        # The first network is used in containers.run
        _, kwargs = mock_client.containers.run.call_args
        self.assertEqual(kwargs["network"], "first-net")

        # networks.get should be called for additional networks only
        mock_client.networks.get.assert_any_call("second-net")
        mock_client.networks.get.assert_any_call("third-net")
        self.assertEqual(mock_client.networks.get.call_count, 2)

        # The container should be connected to each additional network
        mock_network.connect.assert_any_call(mock_container)
        self.assertEqual(mock_network.connect.call_count, 2)

        # Case: single network
        strategy.deploy(
            image="test-image",
            name="test-container-2",
            env=["ENV=dev"],
            networks=["only-net"]
        )
        # containers.run should use the single network
        _, kwargs = mock_client.containers.run.call_args
        self.assertEqual(kwargs["network"], "only-net")
        # networks.get and connect should NOT be called again for single network
        self.assertEqual(mock_client.networks.get.call_count, 2)  # unchanged

        # Case: networks=None
        strategy.deploy(
            image="test-image",
            name="test-container-3",
            env=["ENV=dev"],
            networks=None
        )
        _, kwargs = mock_client.containers.run.call_args
        self.assertIsNone(kwargs["network"])
        # networks.get should still only have been called twice total
        self.assertEqual(mock_client.networks.get.call_count, 2)

    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_single_network(self, mock_from_env):
        """ Test deploy when there is exactly one network """
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_client.containers.run.return_value = mock_container
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        networks = ["single-net"]

        strategy.deploy(
            image="test-image",
            name="test-container-single-net",
            env=["ENV=dev"],
            networks=networks
        )

        # The first (and only) network should be used in containers.run
        _, kwargs = mock_client.containers.run.call_args
        self.assertEqual(kwargs["network"], "single-net")

        # networks.get should NOT be called, because there are no additional networks
        self.assertFalse(mock_client.networks.get.called)
        # container.connect should also NOT be called
        self.assertFalse(mock_client.networks.get.return_value.connect.called)

    @patch("openfactory.openfactory_deploy_strategy.Ulimit")
    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_with_open_files(self, mock_from_env, mock_ulimit):
        """ Test local Docker deploy with open_files limit """

        mock_client = MagicMock()
        mock_run = MagicMock()

        mock_client.containers.run = mock_run
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"],
            open_files=5000
        )

        mock_ulimit.assert_called_once_with(name="nofile", soft=5000, hard=5000)

        _, kwargs = mock_run.call_args
        self.assertIn("ulimits", kwargs)
        self.assertEqual(kwargs["ulimits"], [mock_ulimit.return_value])

    @patch("openfactory.openfactory_deploy_strategy.Ulimit")
    @patch("openfactory.openfactory_deploy_strategy.docker.from_env")
    def test_local_deploy_without_open_files(self, mock_from_env, mock_ulimit):
        """ Test local Docker deploy without open_files limit """

        mock_client = MagicMock()
        mock_client.containers.run = MagicMock()
        mock_from_env.return_value = mock_client

        strategy = LocalDockerDeploymentStrategy()

        strategy.deploy(
            image="test-image",
            name="test-container",
            env=["ENV=dev"]
        )

        mock_ulimit.assert_not_called()
        _, kwargs = mock_client.containers.run.call_args
        self.assertIn("ulimits", kwargs)
        self.assertIsNone(kwargs["ulimits"])
