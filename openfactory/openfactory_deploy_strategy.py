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
- Supports configurable image pull policies (``missing`` and ``always``).
- Supports optional runtime UID/GID configuration for compatibility with shared
  filesystems such as NFS.
- Supports environment variables, container commands, labels, ports, networks,
  constraints, resource limits, and Docker mounts.
- Supports configurable open file descriptor limits (ulimits) for containers and services.
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
        image_pull_policy="missing",
        name="scheduler",
        user="1234:5678",
        env=["ENV=production"],
        open_files=65535,
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
        image_pull_policy="missing",
        name="reporter",
        user="1234:5678",
        env=["LOG_LEVEL=info"],
        open_files=65535,
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
from docker.types import Mount, DriverConfig, Ulimit, ContainerSpec, TaskTemplate, EndpointSpec, Placement
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Literal
from openfactory.docker.docker_access_layer import dal


ImagePullPolicy = Literal["missing", "always"]


class OpenFactoryServiceDeploymentStrategy(ABC):
    """ Abstract base class for OpenFactory service deployment strategies. """

    @abstractmethod
    def deploy(self, *,
               image: str,
               image_pull_policy: ImagePullPolicy = "missing",
               user: Optional[str] = None,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               open_files: Optional[int] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[dict]] = None) -> None:
        """
        Deploy a service using the specified parameters.

        Args:
            image (str): Docker image to deploy.
            image_pull_policy (str): Image pull behavior.

                - ``missing``: Pull the image only if it is not available locally.
                - ``always``: Always pull the image before deployment.
            user (Optional[str]): Runtime user in Docker format ``UID:GID``.
            name (str): Name of the service or container.
            env (List[str]): Environment variables in `KEY=VALUE` format.
            labels (Optional[Dict[str, str]]): Metadata labels attached to the deployed container or service.
            command (Optional[str]): Command to override the default entrypoint.
            ports (Optional[Dict[int, int]]): Port mappings from host to container (host:container).
            networks (Optional[List[str]]): Networks to connect the service or container to.
            constraints (Optional[List[str]]): Constraints for placement (Swarm only).
            resources (Optional[Dict[str, Any]]): Resource limits and reservations.
            open_files (Optional[int]): Maximum number of open file descriptors
                (RLIMIT_NOFILE) available to the process inside the container.
                If not specified, the container runtime default is used.
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
    """
    Deployment strategy for Docker Swarm mode.

    Uses the Docker Engine API directly to create services, allowing support
    for advanced container settings such as file descriptor limits (ulimits)
    that are not exposed by the high-level Docker SDK service API.
    """

    def deploy(self, *,
               image: str,
               image_pull_policy: ImagePullPolicy = "missing",
               user: Optional[str] = None,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               open_files: Optional[int] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[Mount]] = None) -> None:
        """
        Deploy a Docker service using Docker Swarm.

        See parent method for argument descriptions.

        Note:
            - ``image_pull_policy`` is currently ignored for Swarm deployments.
            - When ``open_files`` is specified, the service is deployed with a ``nofile`` ulimit using identical soft and hard limits.
            - If ``open_files`` is not specified, the Docker Engine default file descriptor limit is used.
        """

        container = ContainerSpec(
            image=image,
            command=command,
            env=env,
            user=user,
            mounts=mounts,
            labels=labels
        )

        if open_files is not None:
            container["Ulimits"] = [
                {
                    "Name": "nofile",
                    "Soft": open_files,
                    "Hard": open_files
                }
            ]

        task = TaskTemplate(
            container,
            resources=resources,
            placement=Placement(constraints=constraints) if constraints else None
        )

        dal.docker_client.api.create_service(
            task,
            name=name,
            labels=labels,
            mode=mode,
            networks=networks,
            endpoint_spec=EndpointSpec(ports=ports) if ports else None
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

    def swarm_mount_to_container_mount(self, mount_dict: dict) -> Mount:
        """
        Convert a Docker Swarm-style mount dictionary to a local Docker Mount object.

        Args:
            mount_dict (dict): Mount dictionary in Swarm format, as returned by :meth:`openfactory.filelayer.nfs_backend.NFSBackend.get_mount_spec`.

        Returns:
            docker.types.Mount: A Mount object suitable for use with docker.containers.run().

        Note:
            - Supports ``volume`` mounts only; ``bind`` mounts should be handled separately.
            - The returned Mount preserves the target path, source name, read-only flag,
              and driver configuration specified in the input dictionary.
            - NFS-specific options (e.g., ``nfsvers=4``) are already included in the
              mount dictionary by :meth:`~openfactory.filelayer.nfs_backend.NFSBackend.get_mount_spec`.
        """
        driver_cfg_dict = mount_dict.get("VolumeOptions", {}).get("DriverConfig")
        driver_config = None

        if driver_cfg_dict:
            driver_config = DriverConfig(
                name=driver_cfg_dict.get("Name"),
                options=driver_cfg_dict.get("Options", {})
            )

        return Mount(
            target=mount_dict["Target"],
            source=mount_dict.get("Source"),
            type=mount_dict.get("Type", "volume"),
            read_only=mount_dict.get("ReadOnly", False),
            driver_config=driver_config
        )

    def deploy(self, *,
               image: str,
               image_pull_policy: ImagePullPolicy = "missing",
               user: Optional[str] = None,
               name: str,
               env: List[str],
               labels: Optional[Dict[str, str]] = None,
               command: Optional[str] = None,
               ports: Optional[Dict[int, int]] = None,
               networks: Optional[List[str]] = None,
               constraints: Optional[List[str]] = None,
               resources: Optional[Dict[str, Any]] = None,
               open_files: Optional[int] = None,
               mode: Optional[Dict[str, Any]] = None,
               mounts: Optional[List[dict]] = None) -> None:
        """
        Run a local Docker container.

        See parent method for argument descriptions.

        Note:
            - ``constraints`` and ``mode`` are ignored for local containers.
            - ``image_pull_policy="always"`` forces a Docker image pull before deployment.
            - When ``open_files`` is specified, a ``nofile`` ulimit is configured with identical soft and hard limits.
            - If ``open_files`` is not specified, the Docker Engine default file descriptor limit is used.
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
                    docker_mounts.append(self.swarm_mount_to_container_mount(m))
                else:
                    raise ValueError(f"Unsupported mount type: {m['Type']}")

        if image_pull_policy == "always":
            client.images.pull(image)

        ulimits = None
        if open_files is not None:
            ulimits = [
                Ulimit(
                    name="nofile",
                    soft=open_files,
                    hard=open_files
                )
            ]

        container = client.containers.run(
            image=image,
            user=user,
            name=name,
            environment=env,
            command=command,
            detach=True,
            ports={f"{container_port}/tcp": host_port for host_port, container_port in ports.items()} if ports else None,
            network=networks[0] if networks else None,
            labels=labels,
            nano_cpus=resources.get("Limits", {}).get("NanoCPUs") if resources else None,
            mounts=docker_mounts,
            ulimits=ulimits
        )

        # Attach to additional networks
        if networks:
            for net_name in networks[1:]:
                network = client.networks.get(net_name)
                network.connect(container)

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
        container.stop()
        container.remove()
