"""
OpenFactory Service Deployment Strategies

This module defines abstract interfaces and concrete implementations for deploying
OpenFactory services as Docker containers, supporting both local development
environments and production Docker Swarm clusters.

Key Components:
---------------
- **OpenFactoryServiceDeploymentStrategy**: Abstract base class defining the interface
  for service deployment and removal. Subclasses must implement `deploy` and `remove`.
- **SwarmDeploymentStrategy**: Deploys services in Docker Swarm mode, supporting
  service replication, placement constraints, networks, mounts, and resource limits.
- **LocalDockerDeploymentStrategy**: Deploys single Docker containers locally
  (non-Swarm), primarily for development or testing.

Features:
---------
- Unified interface for deploying and removing OpenFactory services.
- Supports environment variables, container commands, labels, ports, networks,
  constraints, resource limits, and Docker mounts.
- Converts backend mount specifications (Local, NFS, Volume) into Docker-compatible mounts.
- Handles Swarm-specific options (mode, constraints) transparently.
- Provides user-friendly defaults for local container deployment.

Usage Example:
--------------
.. code-block:: python

    # Deploy a local container
    local_strategy = LocalDockerDeploymentStrategy()
    local_strategy.deploy(
        image="ghcr.io/openfactoryio/scheduler:v1.0.0",
        name="scheduler",
        env=["ENV=production"],
        mounts=[{
            "Type": "bind",
            "Source": "/opt/openfactory/data",
            "Target": "/data",
            "ReadOnly": False
        }]
    )

    # Deploy a service in Docker Swarm
    swarm_strategy = SwarmDeploymentStrategy()
    swarm_strategy.deploy(
        image="ghcr.io/openfactoryio/reporter:v1.0.0",
        name="reporter",
        env=["LOG_LEVEL=info"],
        mounts=[{
            "Type": "nfs",
            "Source": "192.168.0.100:/exports/reports",
            "Target": "/mnt/reports",
            "ReadOnly": False
        }],
        ports={8080: 80},
        networks=["openfactory-net"],
        constraints=["node.role==worker"]
    )

This module is used by OpenFactory deployment tools and runtime orchestrators
to standardize service deployment across local development and production environments.
"""

import docker
from docker.types import Mount
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from openfactory.docker.docker_access_layer import dal


class OpenFactoryServiceDeploymentStrategy(ABC):
    """ Abstract base class for OpenFactory service deployment strategies. """

    @abstractmethod
    def deploy(self, *,
               image: str,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[dict]] = None) -> None:
        """
        Deploy a service using the specified parameters.

        Args:
            image (str): Docker image to deploy.
            name (str): Name of the service or container.
            env (List[str]): Environment variables in `KEY=VALUE` format.
            labels (Optional[Dict[str, str]]): Metadata labels (Swarm only).
            command (Optional[str]): Command to override the default entrypoint.
            ports (Optional[Dict[int, int]]): Port mappings from host to container (host:container).
            networks (Optional[List[str]]): Networks to connect the service or container to.
            constraints (Optional[List[str]]): Constraints for placement (Swarm only).
            resources (Optional[Dict[str, Any]]): Resource limits and reservations.
            mode (Optional[Dict[str, Any]]): Service mode (Swarm only).
            mounts (Optional[List[dict]]): List of Docker mount specifications.
        """
        pass

    @abstractmethod
    def remove(self, service_name):
        """
        Remove a service.

        Args:
            service_name (str): Service to be removed.
        """
        pass


class SwarmDeploymentStrategy(OpenFactoryServiceDeploymentStrategy):
    """ Deployment strategy for Docker Swarm mode. """

    def deploy(self, *,
               image: str,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[Mount]] = None) -> None:
        """
        Deploy a Docker service using Docker Swarm.

        See parent method for argument descriptions.
        """
        dal.docker_client.services.create(
            image=image,
            name=name,
            env=env,
            labels=labels,
            command=command,
            endpoint_spec=docker.types.EndpointSpec(ports=ports) if ports else None,
            networks=networks,
            constraints=constraints,
            resources=resources,
            mode=mode,
            mounts=mounts
        )

    def remove(self, service_name):
        """
        Remove a Docker service from a Docker Swarm cluster.

        Args:
            service_name (str): Docker swarm service to be removed.
        """
        service = dal.docker_client.services.get(service_name)
        service.remove()


class LocalDockerDeploymentStrategy(OpenFactoryServiceDeploymentStrategy):
    """ Deployment strategy for local Docker containers (non-Swarm). """

    def deploy(self, *,
               image: str,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[dict]] = None) -> None:
        """
        Run a local Docker container.

        See parent method for argument descriptions.

        Note:
            - Only the first network in the list is used.
            - `constraints` and `mode` are ignored for local containers.
        """
        client = docker.from_env()

        # Convert dict mounts to docker.types.Mount for local Docker
        docker_mounts = []
        if mounts:
            for m in mounts:
                if m["Type"].lower() == "bind":
                    docker_mounts.append(Mount(target=m["Target"],
                                               source=m["Source"],
                                               type="bind",
                                               read_only=m.get("ReadOnly", False)))
                elif m["Type"].lower() == "volume":
                    docker_mounts.append(Mount(target=m["Target"],
                                               source=m["Source"],
                                               type="volume",
                                               read_only=m.get("ReadOnly", False)))
                else:
                    raise ValueError(f"Unsupported mount type: {m['Type']}")

        client.containers.run(
            image=image,
            name=name,
            environment=env,
            command=command,
            detach=True,
            ports={f"{container_port}/tcp": host_port for host_port, container_port in ports.items()} if ports else None,
            network=networks[0] if networks else None,
            labels=labels,
            nano_cpus=resources.get("Limits", {}).get("NanoCPUs") if resources else None,
            mounts=docker_mounts
        )

    def remove(self, service_name):
        """
        Remove a Docker container.

        Args:
            service_name (str): Docker container to be removed.

        Raises:
            docker.errors.NotFound: If the container does not exist.
            docker.errors.APIError: If removal fails due to a Docker API issue.
        """
        client = docker.from_env()
        container = client.containers.get(service_name)
        container.remove(force=True)
