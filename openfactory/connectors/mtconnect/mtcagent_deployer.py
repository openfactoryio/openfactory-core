"""
Module to deploy MTConnect agents and their adapters for OpenFactory devices.

This module provides the MTConnectAgentDeployer class, which handles loading
device configurations, preparing environment variables, deploying adapter
services if needed, deploying the MTConnect agent service itself, and
registering the assets in the system.

It relies on OpenFactory's deployment strategy, Kafka client, and asset
management utilities.
"""

import docker
import os
from fsspec.core import split_protocol

import openfactory.config as config
from openfactory.assets import Asset, AssetAttribute
from openfactory.exceptions import OFAException
from openfactory.models.user_notifications import user_notify
from openfactory.utils import open_ofa, register_asset
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.schemas.devices import Device
from openfactory.schemas.common import constraints, cpus_limit, cpus_reservation
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy


class MTConnectAgentDeployer:
    """
    Class to deploy MTConnect agents and adapters for OpenFactory devices.
    """

    def __init__(self, device: Device, yaml_config_file: str,
                 deployment_strategy: OpenFactoryServiceDeploymentStrategy,
                 ksqlClient: KSQLDBClient, bootstrap_servers: str):
        """
        Initialize the MTConnectAgentDeployer.

        Args:
            device (Device): The device schema object containing connector information.
            yaml_config_file (str): Path to the YAML config file for device XML resolution.
            deployment_strategy (OpenFactoryServiceDeploymentStrategy): Deployment strategy to use for services.
            ksqlClient (KSQLDBClient): KSQL client instance for asset registration.
            bootstrap_servers (str): Kafka bootstrap servers connection string.
        """
        self.device = device
        self.yaml_config_file = yaml_config_file
        self.ksql = ksqlClient
        self.deployment_strategy = deployment_strategy
        self.bootstrap_servers = bootstrap_servers

    def load_agent_config(self) -> str:
        """
        Load the MTConnect agent configuration file contents.

        Returns:
            str: The contents of the MTConnect agent config file.

        Raises:
            OFAException: If the MTConnect agent config file is not found.
        """
        try:
            with open(config.MTCONNECT_AGENT_CFG_FILE, 'r') as f:
                return f.read()
        except FileNotFoundError:
            raise OFAException(f"Could not find MTConnect agent config file '{config.MTCONNECT_AGENT_CFG_FILE}'")

    def load_device_xml(self) -> str:
        """
        Load and return the device XML model for the MTConnect agent.

        If the agent has an IP defined, returns an empty string.
        Otherwise, reads the device XML from the provided URI, supporting
        relative paths based on the YAML config file location.

        Returns:
            str: The device XML content, or an empty string if agent IP is set.

        Raises:
            OFAException: If the device XML cannot be loaded.
        """
        agent = self.device.connector.agent
        if agent.ip:
            return ""

        device_xml_uri = agent.device_xml
        protocol, _ = split_protocol(device_xml_uri)
        if not protocol and device_xml_uri and not os.path.isabs(device_xml_uri):
            device_xml_uri = os.path.join(os.path.dirname(self.yaml_config_file), device_xml_uri)

        try:
            with open_ofa(device_xml_uri) as f_remote:
                return f_remote.read()
        except Exception as err:
            user_notify.fail(f"Could not load XML device model {agent.device_xml}.\n{err}")
            raise OFAException(f"Failed to load device XML: {err}")

    def prepare_env(self) -> list[str]:
        """
        Prepare environment variables for deploying the MTConnect agent.

        Returns:
            list[str]: List of environment variables as strings.
        """
        agent = self.device.connector.agent
        adapter_ip = agent.adapter.ip if agent.adapter and agent.adapter.ip else self.device.uuid.lower() + '-adapter'
        return [
            f'MTC_AGENT_UUID={self.device.uuid.upper()}-AGENT',
            f'ADAPTER_UUID={self.device.uuid.upper()}',
            f'ADAPTER_IP={adapter_ip}',
            f'ADAPTER_PORT={agent.adapter.port if agent.adapter else ""}',
            f'XML_MODEL={self.load_device_xml()}',
            f'AGENT_CFG_FILE={self.load_agent_config()}',
        ]

    def deploy_adapter_if_needed(self) -> None:
        """ Deploy the MTConnect adapter service if an adapter image is specified. """
        adapter = self.device.connector.agent.adapter
        if adapter and adapter.image:
            self.deploy_adapter()

    def deploy_adapter(self) -> None:
        """
        Deploy the MTConnect adapter service with specified environment and resource constraints.

        Handles deployment failures by notifying the user.
        """
        adapter = self.device.connector.agent.adapter
        env = []
        if adapter.environment is not None:
            for item in adapter.environment:
                var, val = item.split('=')
                env.append(f"{var.strip()}={val.strip()}")

        try:
            self.deployment_strategy.deploy(
                image=adapter.image,
                name=self.device.uuid.lower() + '-adapter',
                mode={"Replicated": {"Replicas": 1}},
                env=env,
                constraints=constraints(adapter.deploy),
                networks=[config.OPENFACTORY_NETWORK],
                resources={
                    "Limits": {"NanoCPUs": int(1e9 * cpus_limit(adapter.deploy, 1.0))},
                    "Reservations": {"NanoCPUs": int(1e9 * cpus_reservation(adapter.deploy, 0.5))},
                }
            )
        except docker.errors.APIError as err:
            user_notify.fail(f"Adapter {self.device.uuid.lower()}-adapter could not be deployed\n{err}")
            return
        user_notify.success(f"Adapter {self.device.uuid.lower()}-adapter deployed successfully")

    def deploy_service(self,) -> None:
        """ Deploy the MTConnect agent service. """
        agent = self.device.connector.agent
        command = (
            "sh -c 'printf \"%b\" \"$XML_MODEL\" > device.xml; "
            "printf \"%b\" \"$AGENT_CFG_FILE\" > agent.cfg; mtcagent run agent.cfg'"
        )
        service_name = self.device.uuid.lower() + '-agent'

        labels = {
            "traefik.enable": "true",
            f"traefik.http.routers.{service_name}.rule": f"Host(`{self.device.uuid.lower()}.agent.{config.OPENFACTORY_DOMAIN}`)",
            f"traefik.http.routers.{service_name}.entrypoints": "web",
            f"traefik.http.services.{service_name}.loadbalancer.server.port": "5000",
        }

        ports = {agent.port: 5000} if agent.port else None

        self.deployment_strategy.deploy(
            image=config.MTCONNECT_AGENT_IMAGE,
            command=command,
            name=service_name,
            mode={"Replicated": {"Replicas": 1}},
            env=self.prepare_env(),
            ports=ports,
            labels=labels,
            networks=[config.OPENFACTORY_NETWORK],
            resources={
                "Limits": {"NanoCPUs": int(1e9 * cpus_limit(agent.deploy, 1.0))},
                "Reservations": {"NanoCPUs": int(1e9 * cpus_reservation(agent.deploy, 0.5))},
            },
            constraints=constraints(agent.deploy),
        )

    def register_assets(self) -> None:
        """ Register the MTConnect agent and device assets. """
        agent_uuid = self.device.uuid.upper() + '-AGENT'
        service_name = self.device.uuid.lower() + '-agent'
        register_asset(agent_uuid, uns=None, asset_type="MTConnectAgent",
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers, docker_service=service_name)

        device_asset = Asset(self.device.uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        device_asset.add_reference_below(agent_uuid)

        agent_asset = Asset(agent_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        agent_asset.add_reference_above(self.device.uuid)
        agent_asset.add_attribute('agent_port',
                                  AssetAttribute(
                                      value=self.device.connector.agent.port,
                                      type='Events',
                                      tag='NetworkPort',
                                  ))

    def deploy(self) -> None:
        """
        Deploy the MTConnect agent including loading XML, deploying adapter, service, and registering assets.

        Raises:
            OFAException: Propagated from underlying methods if loading files fails.
        """
        if self.device.connector.agent.ip is None:
            self.deploy_adapter_if_needed()
            self.deploy_service()
        self.register_assets()
        user_notify.success(f"MTConnect Agent {self.device.uuid.upper()}-AGENT deployed successfully")
