"""
**OPCUAConnector Class**

Provides orchestration for deploying OPC UA-based devices into the OpenFactory
environment. This includes:

- Registering the device with OpenFactory.
- Deploying an OPC UA producer to publish device data into the Kafka cluster.
- Tearing down all associated components when the device is removed.

.. seealso::

   The schema of the OPCUAConnector is :class:`openfactory.schemas.connectors.opcua.OPCUAConnectorSchema`.
"""

import openfactory.config as config
from openfactory.models.user_notifications import user_notify
from openfactory.connectors.base_connector import Connector
from openfactory.assets import Asset
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.utils import register_asset
from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema
from openfactory.exceptions import OFAException
from openfactory.connectors.registry import register_connector


@register_connector(OPCUAConnectorSchema)
class OPCUAConnector(Connector):
    """
    Connector for OPC UA devices that manages deployment of OPC UA producers.

    Responsibilities include:

    - Registering the device as an OpenFactory asset.
    - Deploying an OPC UA producer to stream device data to Kafka.
    - Managing references between the device and its producer in OpenFactory.
    - Tearing down the producer when the device is removed.

    .. seealso::

       The schema of the OPCUAConnector is :class:`openfactory.schemas.connectors.opcua.OPCUAConnectorSchema`.
    """

    CONNECTOR_NAME = "OPCUA"

    def __init__(self,
                 deployment_strategy: OpenFactoryServiceDeploymentStrategy,
                 ksqlClient: KSQLDBClient,
                 bootstrap_servers: str = config.KAFKA_BROKER):
        """
        Initializes the OPCUAConnector.

        Args:
            deployment_strategy (OpenFactoryServiceDeploymentStrategy) : The deployment strategy to use.
            ksqlClient (KSQLDBClient): The client for interacting with ksqlDB.
            bootstrap_servers (str): The Kafka bootstrap server address. Defaults to config.KAFKA_BROKER.
        """
        self.deployment_strategy = deployment_strategy
        self.ksql = ksqlClient
        self.bootstrap_servers = bootstrap_servers

    def _get_coordinator(self) -> Asset:
        """
        Discover and returns the OPC UA coordinator asset.

        Returns:
            Asset: The OPC UA coordinator asset.

        Raises:
            OFAException: If the OPC UA coordinator is not configured or unavailable.
        """
        query = f"select ASSET_UUID FROM ASSETS_TYPE WHERE TYPE='{self.CONNECTOR_NAME}.Coordinator';"
        res = self.ksql.query(query)
        if res:
            coordinator = Asset(asset_uuid=res[0]["ASSET_UUID"], ksqlClient=self.ksql)
        else:
            raise OFAException("OPC UA Coordinator is not deployed")

        if coordinator.avail.value != "AVAILABLE":
            raise OFAException("OPC UA Coordinator is not AVAILABLE")

        return coordinator

    def deploy(self, device: Device, yaml_config_file: str) -> None:
        """
        Deploy a device based on its configuration.

        Args:
            device (Device): Device to deploy.
            yaml_config_file (str): Path to the YAML configuration file.
        """
        if device.connector.type != 'opcua':
            raise OFAException(f"Device {device.uuid} is not configured with an OPC UA connector")

        coordinator = self._get_coordinator()

        try:
            coordinator.register_device(sender_uuid='opcua-connector', device_config=str(device.model_dump_json()))
        except TypeError:
            raise OFAException(f"Asset '{coordinator.asset_uuid}' does not appear to be a valid OPC UA coordinator.")

        user_notify.success(f"OPC UA device {device.uuid} registered successfully")

        # Register device asset
        register_asset(
            asset_uuid=device.uuid,
            uns=device.uns,
            asset_type="Device",
            ksqlClient=self.ksql,
            bootstrap_servers=self.bootstrap_servers
        )

    def tear_down(self, device_uuid: str) -> None:
        """
        Tear down a deployed device given its UUID.

        Args:
            device_uuid (str): Unique identifier of the device to be torn down.
        """
        coordinator = self._get_coordinator()

        try:
            coordinator.deregister_device(sender_uuid='opcua-connector', device_uuid=device_uuid)
        except TypeError:
            raise OFAException(f"Asset {coordinator.asset_uuid} does not appear to be a valid OPC UA coordinator")
        user_notify.success(f"OPC UA device {device_uuid} deregistered successfully")

        return
