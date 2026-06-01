"""
SHDRConnector Class

Provides orchestration for deploying SHDR-based devices into the OpenFactory
environment. This includes:

- Registering the device with OpenFactory.
- Registering an SHDR device with the SHDR coordinator to publish device data into the Kafka cluster.
- Tearing down all associated components when the device is removed.

.. seealso::

   The schema of the SHDRConnector is :class:`openfactory.schemas.connectors.shdr.SHDRConnectorSchema`.
"""

import openfactory.config as config
from openfactory.models.user_notifications import user_notify
from openfactory.connectors.base_connector import Connector
from openfactory.assets import Asset
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.utils import register_asset, deregister_asset
from openfactory.schemas.devices import Device
from openfactory.schemas.connectors.shdr import SHDRConnectorSchema
from openfactory.exceptions import OFAException
from openfactory.connectors.registry import register_connector


@register_connector(SHDRConnectorSchema)
class SHDRConnector(Connector):
    """
    Connector for SHDR devices that manages deployment and registration of SHDR devices.

    Responsibilities include:

    - Registering the device as an OpenFactory asset.
    - Registering the SHDR device with the SHDR coordinator to stream device data to Kafka.

    .. seealso::

       The schema of the SHDRConnector is :class:`openfactory.schemas.connectors.shdr.SHDRConnectorSchema`.
    """

    COORDINATOR_UUID = "SHDR-COORDINATOR"

    def __init__(self,
                 deployment_strategy: OpenFactoryServiceDeploymentStrategy,
                 ksqlClient: KSQLDBClient,
                 bootstrap_servers: str = config.KAFKA_BROKER):
        """
        Initializes the SHDRConnector.

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
        Get the SHDR coordinator asset.

        Returns:
            Asset: The SHDR coordinator asset.

        Raises:
            OFAException: If the SHDR coordinator is not configured or unavailable.
        """
        coordinator = Asset(asset_uuid=self.COORDINATOR_UUID, ksqlClient=self.ksql)
        if coordinator.avail.value != "AVAILABLE":
            raise OFAException(f"SHDR Coordinator '{self.COORDINATOR_UUID}' is not deployed")

        return coordinator

    def deploy(self, device: Device, yaml_config_file: str) -> None:
        """
        Deploy a device based on its configuration.

        Args:
            device (Device): Device to deploy.
            yaml_config_file (str): Path to the YAML configuration file.

        Raises:
            OFAException: If the device cannot be deployed.
        """
        if device.connector.type != "shdr":
            raise OFAException(f"Device {device.uuid} is not configured with an SHDR connector")

        coordinator = self._get_coordinator()

        try:
            coordinator.register_device(sender_uuid='ofa-cli', device_config=str(device.model_dump_json()))
        except TypeError:
            raise OFAException(f"Asset '{self.COORDINATOR_UUID}' does not appear to be a valid SHDR coordinator.")

        user_notify.success(f"SHDR device {device.uuid} registered successfully")

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

        Raises:
            OFAException: If the device cannot be torn down.
        """
        coordinator = self._get_coordinator()

        try:
            coordinator.deregister_device(sender_uuid='shdr-connector', device_uuid=device_uuid)
        except TypeError:
            raise OFAException(f"Asset {self.COORDINATOR_UUID} does not appear to be a valid SHDR coordinator")
        user_notify.success(f"SHDR device {device_uuid} deregistered successfully")

        # De-register device asset
        deregister_asset(
            asset_uuid=device_uuid,
            ksqlClient=self.ksql,
            bootstrap_servers=self.bootstrap_servers
        )
