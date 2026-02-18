""" Docker Access Layer. """

import docker
import openfactory.config as config
from openfactory.models.user_notifications import user_notify
from typing import List, Optional


class DockerAccesLayer:
    """
    Docker Access Layer (DAL) for interfacing with the Docker Swarm cluster.

    Provides methods to connect to the Docker daemon, retrieve swarm join tokens,
    and fetch information such as node labels and IP addresses.
    """

    def __init__(self) -> None:
        """
        Initializes the DockerAccesLayer with empty connection state.

        Attributes:
            docker_client (Optional[docker.DockerClient]): The Docker client used to communicate with the manager node.
            docker_url (Optional[str]): The URL used to connect to the Docker daemon.
            worker_token (Optional[str]): The token used for joining workers to the Swarm.
            manager_token (Optional[str]): The token used for joining managers to the Swarm.
            leader_ip (Optional[str]): IP address of the leader manager node.
        """
        self.docker_client: Optional[docker.DockerClient] = None
        self.docker_url: Optional[str] = None
        self.worker_token: Optional[str] = None
        self.manager_token: Optional[str] = None
        self.ip: Optional[str] = None

    def connect(self) -> None:
        """
        Connects to the Docker engine via the OpenFactory Manager Node.

        Initializes the Docker client using the manager node's Docker URL.
        If the manager node is not in Swarm mode, join tokens are marked as unavailable,
        and a warning is issued. Otherwise, retrieves and stores the Swarm join tokens
        for worker and manager nodes.

        Note:
            Logs a warning if the manager node is not in Swarm mode.

        Sets:
            self.docker_client: Initialized Docker client instance.
            self.worker_token: Worker join token or 'UNAVAILABLE'.
            self.manager_token: Manager join token or 'UNAVAILABLE'.
            self.leader_ip: IP address of the leader manager node.
        """
        self.docker_url = config.OPENFACTORY_MANAGER_NODE_DOCKER_URL

        try:
            self.docker_client = docker.DockerClient(base_url=self.docker_url)
            swarm_attrs = self.docker_client.swarm.attrs
        except docker.errors.DockerException as e:
            user_notify.warning(f"ERROR: Could not connect to Docker at {self.docker_url}: {e}")
            self.worker_token = 'UNAVAILABLE'
            self.manager_token = 'UNAVAILABLE'
            return

        if 'JoinTokens' not in swarm_attrs:
            if config.DEPLOYMENT_PLATFORM == 'swarm':
                user_notify.warning(f'WARNING: Docker running on {config.OPENFACTORY_MANAGER_NODE_DOCKER_URL} is not a Swarm manager')
            self.worker_token = 'UNAVAILABLE'
            self.manager_token = 'UNAVAILABLE'
            return
        self.worker_token = self.docker_client.swarm.attrs['JoinTokens']['Worker']
        self.manager_token = self.docker_client.swarm.attrs['JoinTokens']['Manager']

        self.leader_ip = None
        for node in self.docker_client.nodes.list():
            if node.attrs['Spec']['Role'] == 'manager':
                manager_status = node.attrs.get('ManagerStatus', {})
                if manager_status.get('Leader'):
                    self.leader_ip = node.attrs['Status']['Addr']
                    break

        if not self.leader_ip:
            user_notify.warning("WARNING: Could not determine the leader manager node IP.")

    def get_node_name_labels(self) -> List[str]:
        """
        Retrieves the 'name' labels for all nodes in the swarm cluster.

        Returns:
            List[str]: A list of node names from the swarm.
        """
        name_labels = []
        for node in self.docker_client.nodes.list():
            labels = node.attrs.get('Spec', {}).get('Labels', {})
            if 'name' in labels:
                name_labels.append(labels['name'])
        return name_labels

    def get_node_ip_addresses(self) -> List[str]:
        """
        Retrieves the IP addresses of all nodes in the swarm cluster.

        Returns:
            List[str]: A list of IP addresses from the swarm nodes.
        """
        ip_addresses = []
        for node in self.docker_client.nodes.list():
            status = node.attrs.get('Status', {})
            ip_address = status.get('Addr')
            if ip_address:
                ip_addresses.append(ip_address)
        return ip_addresses


dal = DockerAccesLayer()
