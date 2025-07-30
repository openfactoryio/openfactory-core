import unittest
import docker
from unittest.mock import patch, MagicMock
from openfactory.docker.docker_access_layer import DockerAccesLayer


class TestDockerAccessLayer(unittest.TestCase):
    """
    Test cases for DockerAccesLayer class
    """

    @patch('openfactory.docker.docker_access_layer.config')
    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_connect_success(self, mock_docker_client_class, mock_config):
        """ Test successful connection to Docker engine in Swarm mode """
        mock_config.OPENFACTORY_MANAGER_NODE_DOCKER_URL = 'tcp://127.0.0.1:2375'

        # Prepare mock DockerClient instance
        mock_docker_client = MagicMock()
        mock_docker_client_class.return_value = mock_docker_client

        # Set swarm.attrs to return tokens
        mock_docker_client.swarm.attrs = {
            'JoinTokens': {
                'Worker': 'worker-token',
                'Manager': 'manager-token'
            }
        }

        # Mock a single node marked as manager and leader
        mock_node = MagicMock()
        mock_node.attrs = {
            'Spec': {'Role': 'manager'},
            'ManagerStatus': {'Leader': True},
            'Status': {'Addr': '192.168.1.100'}
        }
        mock_docker_client.nodes.list.return_value = [mock_node]

        dal = DockerAccesLayer()
        dal.connect()

        self.assertEqual(dal.docker_url, 'tcp://127.0.0.1:2375')
        self.assertEqual(dal.worker_token, 'worker-token')
        self.assertEqual(dal.manager_token, 'manager-token')
        self.assertEqual(dal.leader_ip, '192.168.1.100')

    @patch('openfactory.docker.docker_access_layer.user_notify')
    @patch('openfactory.docker.docker_access_layer.config')
    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_docker_client_creation_fails(self, mock_docker_client_class, mock_config, mock_user_notify):
        """ Test Docker client creation failure """

        mock_config.OPENFACTORY_MANAGER_NODE_DOCKER_URL = 'ssh://user@192.168.1.5'

        # Simulate failure at DockerClient(...) instantiation
        mock_docker_client_class.side_effect = docker.errors.DockerException("SSH failure or daemon not reachable")

        dal = DockerAccesLayer()
        dal.connect()

        self.assertEqual(dal.worker_token, 'UNAVAILABLE')
        self.assertEqual(dal.manager_token, 'UNAVAILABLE')
        mock_user_notify.warning.assert_called_once()
        self.assertIn("Could not connect to Docker", mock_user_notify.warning.call_args[0][0])

    @patch('openfactory.docker.docker_access_layer.user_notify')
    @patch('openfactory.docker.docker_access_layer.config')
    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_connect_docker_exception(self, mock_docker_client_class, mock_config, mock_user_notify):
        """ Test connection failure due to DockerException when accessing swarm.attrs """

        mock_config.OPENFACTORY_MANAGER_NODE_DOCKER_URL = 'tcp://127.0.0.1:2375'

        # Raise DockerException when accessing swarm.attrs
        mock_docker_client = MagicMock()
        type(mock_docker_client.swarm).attrs = property(lambda self: (_ for _ in ()).throw(docker.errors.DockerException("Docker daemon unreachable")))
        mock_docker_client_class.return_value = mock_docker_client

        dal = DockerAccesLayer()
        dal.connect()

        self.assertEqual(dal.worker_token, 'UNAVAILABLE')
        self.assertEqual(dal.manager_token, 'UNAVAILABLE')
        mock_user_notify.warning.assert_called_once()
        self.assertIn("Could not connect to Docker", mock_user_notify.warning.call_args[0][0])

    @patch('openfactory.docker.docker_access_layer.config')
    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_connect_swarm_mode_exception(self, mock_docker_client, mock_config):
        """ Test connection to Docker engine in Swarm mode with exception """
        mock_config.OPENFACTORY_MANAGER_NODE_DOCKER_URL = 'tcp://127.0.0.1:2375'
        mock_config.OPENFACTORY_MANAGER_NODE = '127.0.0.1'
        mock_client_instance = mock_docker_client.return_value
        mock_client_instance.swarm.attrs = {}  # Simulate not in swarm mode

        dal = DockerAccesLayer()
        dal.connect()

        # Expect join tokens to be marked as unavailable
        self.assertEqual(dal.worker_token, 'UNAVAILABLE')
        self.assertEqual(dal.manager_token, 'UNAVAILABLE')

    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_get_node_name_labels(self, mock_docker_client):
        """ Test getting node name labels """
        mock_nodes = [
            MagicMock(attrs={'Spec': {'Labels': {'name': 'node1'}}}),
            MagicMock(attrs={'Spec': {'Labels': {'name': 'node2'}}}),
            MagicMock(attrs={'Spec': {'Labels': {}}}),
        ]
        mock_docker_client.return_value.nodes.list.return_value = mock_nodes

        dal = DockerAccesLayer()
        dal.docker_client = mock_docker_client.return_value
        name_labels = dal.get_node_name_labels()

        self.assertEqual(name_labels, ['node1', 'node2'])

    @patch('openfactory.docker.docker_access_layer.docker.DockerClient')
    def test_get_node_ip_addresses(self, mock_docker_client):
        """ Test getting node IP addresses """
        mock_nodes = [
            MagicMock(attrs={'Status': {'Addr': '192.168.1.1'}}),
            MagicMock(attrs={'Status': {'Addr': '192.168.1.2'}}),
            MagicMock(attrs={'Status': {}}),
        ]
        mock_docker_client.return_value.nodes.list.return_value = mock_nodes

        dal = DockerAccesLayer()
        dal.docker_client = mock_docker_client.return_value
        ip_addresses = dal.get_node_ip_addresses()

        self.assertEqual(ip_addresses, ['192.168.1.1', '192.168.1.2'])
