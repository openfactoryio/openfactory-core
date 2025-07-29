"""
Initialize the OpenFactory cluster infrastructure.

This script must be run on one of the OpenFactory manager nodes defined in the
<cluster_config_file>. It performs the following tasks:

  - Initializes a Docker Swarm manager node on the current machine
  - Replaces the default Docker Swarm ingress network (if specified)
  - Creates the OpenFactory overlay network
  - Optionally creates named Docker volumes

Usage:
    python init_openfactory_cluster.py <cluster_config_file>

The <cluster_config_file> must define the OpenFactory network and,
optionally, a custom ingress network. Below is an example structure:

Example:
    networks:
      openfactory-network:
        ipam:
          config:
            - subnet: 10.2.0.0/24
              gateway: 10.2.0.1
              ip_range: 10.2.0.128/25

      docker-ingress-network:
        name: ofa_ingress
        ipam:
          config:
            - subnet: 10.1.0.0/24

Note:
    The current host must be listed under the 'managers' section of the
    cluster configuration, with a valid IP address that matches one of its
    local interfaces.
"""

import socket
import sys
import docker
import docker.types
import openfactory.config as config
from typing import Dict, Optional
from openfactory.schemas.infra import get_infrastructure_from_config_file


def is_ip_local(ip_str: str) -> bool:
    """
    Check if the given IP address is bound to a local network interface.

    This function attempts to bind a UDP socket to the provided IP address.
    If the bind succeeds, it indicates that the IP is assigned to an interface
    on the local machine.

    Args:
        ip_str (str): The IP address to check.

    Returns:
        bool: True if the IP address is local, False otherwise.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((ip_str, 0))  # If it fails, IP is not local
        s.close()
        return True
    except OSError:
        return False


def find_matching_manager_ip(cfg: Dict) -> Optional[str]:
    """
    Find and return the manager IP address that matches a local network interface.

    This function iterates over the manager nodes defined in the infrastructure
    configuration and checks if any of their IP addresses are bound to a local
    interface on the current machine.

    Args:
        cfg (dict): The infrastructure configuration dictionary. It must include
                    a 'nodes' -> 'managers' section, where each manager entry has
                    an 'ip' field.

    Returns:
        Optional[str]: The matching local manager IP address as a string if found;
                       otherwise, returns None.
    """
    managers = cfg['nodes']['managers']

    for _, data in managers.items():
        ip_str = str(data['ip'])
        is_local = is_ip_local(ip_str)
        print(f"Testing manager IP: {ip_str} -> {'✔ local' if is_local else '✘ not local'}")
        if is_local:
            return ip_str

    return None


def get_manager_labels(data: Dict) -> Dict:
    """
    Get the name of the openfactory manager node.

    Args:
        data (dict): The infrastructure configuration data.

    Returns:
        dict: A dictionary containing the labels for the manager node.
    """
    # Get IP address of host
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(('8.8.8.8', 80))
        ip_address = s.getsockname()[0]

    # Loop through managers to find the matching IP address
    for manager, details in data['nodes']['managers'].items():
        if details['ip'] == ip_address:
            labels = {'name': manager}
            if 'labels' in details:
                labels.update(details['labels'])
            return labels

    # Return 'ofa_manager' if the IP address is not found
    return {'name': 'ofa_manager'}


def ipam_config(network: Dict) -> docker.types.IPAMConfig:
    """
    Get the IPAM configuration.

    Args:
        network (dict): The network configuration data.

    Returns:
        docker.types.IPAMConfig: The IPAM configuration for the network.
    """
    ipam_pools = []
    if 'ipam' in network and 'config' in network['ipam']:
        for pool in network['ipam']['config']:
            ipam_pools.append(docker.types.IPAMPool(
                subnet=pool.get('subnet'),
                gateway=pool.get('gateway'),
                iprange=pool.get('ip_range')
            ))

    # Create the IPAM configuration
    ipam_config = docker.types.IPAMConfig(
        driver=network['ipam'].get('driver', 'default'),
        pool_configs=ipam_pools
    )
    return ipam_config


def create_volume(client: docker.DockerClient, volume_name: str, driver_opts: Dict) -> None:
    """
    Create a docker volume.

    Args:
        client (docker.DockerClient): The Docker client.
        volume_name (str): The name of the volume to create.
        driver_opts (dict): Driver options for the volume.
    """
    try:
        client.volumes.create(
            name=volume_name,
            driver="local",
            driver_opts=driver_opts
        )
        print(f"Volume '{volume_name}' created successfully")
    except docker.errors.APIError as e:
        print(f"Error creating volume: {e}")


def init_infrastructure(networks: Dict, manager_labels: Dict, volumes: Dict, host_ip: str) -> None:
    """
    Initialize  infrastructure.

    Args:
        networks (dict): The network configuration data.
        manager_labels (dict): The labels for the manager node.
        volumes (dict): The volume configuration data.
        host_ip (str): The IP address of the host on which this script runs.

    Raises:
        SystemExit: If the initialization fails.
    """
    # setup OPENFACTORY_MANAGER_NODE as the first swarm manager
    client = docker.from_env()
    try:
        node_id = client.swarm.init(advertise_addr=host_ip)
    except docker.errors.APIError as err:
        print(f"Could not initalize the OpenFactory manager node on this machine\n{err}")
        sys.exit(1)
    node = client.nodes.get(node_id)
    node_spec = node.attrs['Spec']
    node_spec['Labels'] = manager_labels
    node.update(node_spec)
    print("Initial node created successfully")

    # replace the ingress network if needed
    if 'docker-ingress-network' in networks:
        try:
            network = client.networks.get('ingress')
            network.remove()
        except docker.errors.NotFound:
            pass
        try:
            network = client.networks.get(networks['docker-ingress-network']['name'])
            network.remove()
        except docker.errors.NotFound:
            pass
        client.networks.create(
            name=networks['docker-ingress-network']['name'],
            driver='overlay',
            ingress=True,
            ipam=ipam_config(networks['docker-ingress-network'])
        )
        print("Docker ingress network created successfully")

    # create the openfactory-network
    if 'openfactory-network' in networks:
        ipamConfig = ipam_config(networks['openfactory-network'])
    else:
        ipamConfig = docker.types.IPAMConfig(driver='default', pool_configs=[])

    try:
        network = client.networks.get(config.OPENFACTORY_NETWORK)
        network.remove()
    except docker.errors.NotFound:
        pass

    client.networks.create(
        name=config.OPENFACTORY_NETWORK,
        driver='overlay',
        attachable=True,
        ipam=ipamConfig
    )
    print(f"Network '{config.OPENFACTORY_NETWORK}' created successfully.")

    # create docker volumes
    if volumes:
        for volume_name, volume_config in volumes.items():
            if volume_config:
                driver_opts = volume_config.get('driver_opts', {})
            else:
                driver_opts = {}
            create_volume(client, volume_name, driver_opts)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python init_openfactory_cluster.py <cluster_config_file>")
        sys.exit(1)

    cfg = get_infrastructure_from_config_file(sys.argv[1])

    ip = find_matching_manager_ip(cfg)
    if ip is None:
        print("FATAL ERROR: This host is not listed as a manager node in the provided infrastructure config.")
        sys.exit(1)

    if not cfg.get('networks'):
        cfg['networks'] = []

    init_infrastructure(
        networks=cfg['networks'],
        manager_labels=get_manager_labels(cfg),
        volumes=cfg.get('volumes', {}),
        host_ip=ip)
