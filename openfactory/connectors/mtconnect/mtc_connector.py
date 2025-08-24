"""
**MTConnectConnector Class**

Provides orchestration for deploying MTConnect-based devices into the OpenFactory
environment. This includes:

- Registering the device with OpenFactory.
- Deploying the MTConnect agent and its adapters via `MTConnectAgentDeployer`.
- Deploying a Kafka producer to publish MTConnect data into the Kafka cluster.
- Tearing down all associated components when the device is removed.

Note:
    This connector delegates agent and adapter deployment to
    `MTConnectAgentDeployer`, ensuring separation between orchestration logic
    and MTConnect-specific deployment details.
"""

import docker
import openfactory.config as config
from openfactory.connectors.base_connector import Connector
from openfactory.assets import Asset
from openfactory.exceptions import OFAException
from openfactory.models.user_notifications import user_notify
from openfactory.utils import register_asset, deregister_asset
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.schemas.devices import Device
from openfactory.schemas.common import constraints, cpus_limit, cpus_reservation
from openfactory.schemas.connectors.mtconnect import MTConnectConnectorSchema
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.connectors.registry import register_connector
from openfactory.connectors.mtconnect.mtcagent_deployer import MTConnectAgentDeployer


@register_connector(MTConnectConnectorSchema)
class MTConnectConnector(Connector):
    """
    Connector for MTConnect devices that manages deployment of MTConnect agents,
    adapters, and Kafka producers, leveraging a pluggable deployment strategy.
    """

    def __init__(self,
                 deployment_strategy: OpenFactoryServiceDeploymentStrategy,
                 ksqlClient: KSQLDBClient,
                 bootstrap_servers: str = config.KAFKA_BROKER):
        """
        Initializes the MTConnectConnector.

        Args:
            deployment_strategy (OpenFactoryServiceDeploymentStrategy) : The deployment strategy to use.
            ksqlClient (KSQLDBClient): The client for interacting with ksqlDB.
            bootstrap_servers (str): The Kafka bootstrap server address. Defaults to config.KAFKA_BROKER.
        """
        self.deployment_strategy = deployment_strategy
        self.ksql = ksqlClient
        self.bootstrap_servers = bootstrap_servers

    def deploy(self, device: Device, yaml_config_file: str) -> None:
        """
        Deploy a device based on its configuration.

        Args:
            device (Device): Device to deploy.
            yaml_config_file (str): Path to the YAML configuration file.
        """
        if device.connector.type != 'mtconnect':
            raise OFAException(f"Device {device.uuid} is not configured with an MTConnect connector")

        # Register device asset
        register_asset(device.uuid, uns=device.uns, asset_type="Device",
                       ksqlClient=self.ksql, docker_service="")

        # Deploy MTConnect agent (which itself deploys adapter if needed)
        agent_deployer = MTConnectAgentDeployer(device, yaml_config_file,
                                                self.deployment_strategy,
                                                self.ksql, self.bootstrap_servers)
        agent_deployer.deploy()

        # Deploy Kafka producer
        self.deploy_kafka_producer(device)

    def _get_mtc_agent_url(self, device):
        """ Returns the URL to use for the MTConnect Agent """
        agent = device.connector.agent
        if agent.ip:
            if agent.port == 443:
                return f"https://{agent.ip}:443"
            else:
                return f"http://{agent.ip}:{agent.port}"
        return f"http://{device.uuid.lower()}-agent:5000"

    def deploy_kafka_producer(self, device: Device) -> None:
        """
        Deploy a Kafka producer.

        Args:
            device (Device): The device for which the producer is to be deployed.

        Raises:
            OFAException: If the producer cannot be deployed.
        """
        service_name = device.uuid.lower() + '-producer'
        producer_uuid = device.uuid.upper() + '-PRODUCER'
        try:
            self.deployment_strategy.deploy(
                image=config.MTCONNECT_PRODUCER_IMAGE,
                name=service_name,
                mode={"Replicated": {"Replicas": 1}},
                env=[f'KAFKA_BROKER={config.KAFKA_BROKER}',
                     f'KAFKA_PRODUCER_UUID={producer_uuid}',
                     f'MTC_AGENT={self._get_mtc_agent_url(device)}'],
                constraints=constraints(device.connector.agent.deploy),
                resources={
                    "Limits": {"NanoCPUs": int(1000000000*cpus_limit(device.connector.agent.deploy, 1.0))},
                    "Reservations": {"NanoCPUs": int(1000000000*cpus_reservation(device.connector.agent.deploy, 0.5))}
                    },
                networks=[config.OPENFACTORY_NETWORK]
            )
        except docker.errors.APIError as err:
            raise OFAException(f"Producer {service_name} could not be created\n{err}")

        # register producer in OpenFactory
        register_asset(producer_uuid, uns=None, asset_type="KafkaProducer",
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers, docker_service=service_name)
        dev = Asset(device.uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        dev.add_reference_below(producer_uuid)
        producer = Asset(producer_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        producer.add_reference_above(device.uuid)

        user_notify.success(f"Kafka producer {producer_uuid} deployed successfully")

    def tear_down(self, device_uuid: str) -> None:
        """
        Tear down a deployed device given its UUID.

        Args:
            device_uuid (str): Unique identifier of the device to be torn down.
        """
        # Tear down adapter
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-adapter')
            user_notify.success(f"Adapter for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            # no adapter running as a Docker swarm service
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)

        # Tear down Producer
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-producer')
            deregister_asset(device_uuid + '-PRODUCER', ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"Kafka producer for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            user_notify.info(f"Kafka producer for device {device_uuid} was not running")
        except docker.errors.APIError as err:
            raise OFAException(err)

        # Tear down MTConnect agent
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-agent')
            deregister_asset(device_uuid.upper() + '-AGENT', ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"MTConnect Agent for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)
