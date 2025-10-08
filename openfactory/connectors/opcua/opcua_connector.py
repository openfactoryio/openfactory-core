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

import requests
import requests.exceptions
import openfactory.config as config
from openfactory.models.user_notifications import user_notify
from openfactory.connectors.base_connector import Connector
from openfactory.assets import Asset
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.utils import register_asset, deregister_asset
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

    def deploy(self, device: Device, yaml_config_file: str) -> None:
        """
        Deploy a device based on its configuration.

        Args:
            device (Device): Device to deploy.
            yaml_config_file (str): Path to the YAML configuration file.
        """
        if device.connector.type != 'opcua':
            raise OFAException(f"Device {device.uuid} is not configured with an OPC UA connector")

        # Deploy OPC UA connector
        self.deploy_opcua_producer(device)

    def deploy_opcua_producer(self, device: Device) -> None:
        """
        Deploy an OPC UA producer.

        Args:
            device (Device): The device for which the producer is to be deployed.

        Raises:
            OFAException: If the producer cannot be deployed.
        """
        service_name = device.uuid.lower() + '-producer'
        producer_uuid = device.uuid.upper() + '-PRODUCER'

        url = f"{config.OPCUA_CONNECTOR_COORDINATOR}/register_device"
        payload = {"device": device.model_dump()}
        try:
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            user_notify.success(f"OPC UA producer for device {device.uuid} registerd succesfully with gateway {resp.json()['assigned_gateway']}")
        except requests.exceptions.ConnectionError:
            raise OFAException(f"No OPC UA Coordinator running at URL {config.OPCUA_CONNECTOR_COORDINATOR}")
        except Exception as e:
            raise OFAException(f"Connector {service_name} could not be created\n{e}")

        # Register device asset
        register_asset(device.uuid, uns=device.uns, asset_type="Device",
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)

        # register producer in OpenFactory
        register_asset(producer_uuid, uns=None, asset_type="KafkaProducer",
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        dev = Asset(device.uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        dev.add_reference_below(producer_uuid)
        producer = Asset(producer_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        producer.add_reference_above(device.uuid)

    def tear_down(self, device_uuid: str) -> None:
        """
        Tear down a deployed device given its UUID.

        Args:
            device_uuid (str): Unique identifier of the device to be torn down.
        """
        url = f"{config.OPCUA_CONNECTOR_COORDINATOR}/unregister_device/{device_uuid}"
        try:
            response = requests.delete(url)
            deregister_asset(
                device_uuid + '-PRODUCER',
                ksqlClient=self.ksql,
                bootstrap_servers=self.bootstrap_servers
            )
            response.raise_for_status()
            user_notify.success(f"OPC UA producer for device {device_uuid} shut down successfully")

        except requests.exceptions.HTTPError as e:
            try:
                detail = response.json().get("detail", str(e))
            except Exception:
                detail = str(e)
            raise OFAException(f"OPC UA Coordinator: {detail}")

        except requests.exceptions.ConnectionError:
            raise OFAException(f"No OPC UA Coordinator running at URL {config.OPCUA_CONNECTOR_COORDINATOR}")

        except requests.exceptions.HTTPError as err:
            # Try to extract the detail from the response body
            try:
                error_detail = response.json().get("detail", response.text)
            except ValueError:
                # Fallback if not JSON
                error_detail = response.text or str(err)

            raise OFAException(
                f"Failed to unregister device {device_uuid} on OPC UA Coordinator: {error_detail}"
            ) from err
        except requests.exceptions.RequestException as err:
            raise OFAException(err)
