"""
Defines the abstract base class for device connectors.

Each connector handles deployment and teardown of devices
for a specific connector type (e.g., OpenFactory, MTConnect, OPCUA).

Creating a New Connector
------------------------

To add a new connector type to OpenFactory, follow these steps:

1. Create a Schema for Your Connector

   - Define a Pydantic schema for your connector configuration (e.g., ``MyConnectorSchema``).
   - Include all fields required for deployment and teardown, such as IPs, ports,
     resource limits, or other connector-specific parameters.
   - Make sure to include a ``type`` field that matches your connector type string.
     This field is used as a discriminator when parsing device configurations.
   - Register your schema in the :class:`openfactory.schemas.connectors.types.Connector` Union to enable type-based parsing:

2. Implement the Connector Class

   Subclass ``Connector`` and implement the following methods:

   - ``deploy(device: Device, yaml_config_file: str) -> None``
      Register the device asset, deploy any required services (agents, producers),
      and create dependencies (Kafka topics, ksqlDB tables, supervisors).

   - ``tear_down(device_uuid: str) -> None``
      Remove all deployed resources for the device.

3. Register Your Connector

   Use the ``@register_connector`` decorator to register your connector class with its schema:

   .. code-block:: python

    @register_connector(MyConnectorSchema)
    class MyConnector(Connector):

        def deploy(self, device: Device, yaml_config_file: str):
            ...

        def tear_down(self, device_uuid: str):
            ...

4. Write Unit Tests

   - Mock all external dependencies (Docker, Kafka, ksqlDB, user notifications).
   - Test deployment, teardown, error handling, and service generation logic.

5. Integrate With OpenFactoryManager

   Functions like ``deploy_devices_from_config_file`` will automatically use your connector
   as long as it is registered and its schema is correctly referenced in the device configuration.

Minimal Connector Template
--------------------------

.. code-block:: python

    from openfactory.connectors.base_connector import Connector
    from openfactory.connectors.registry import register_connector
    from openfactory.schemas.connectors.my_connector import MyConnectorSchema
    from openfactory.schemas.devices import Device

    @register_connector(MyConnectorSchema)
    class MyConnector(Connector):

        def deploy(self, device: Device, yaml_config_file: str) -> None:
            # Implement your deployment logic here
            pass

        def tear_down(self, device_uuid: str) -> None:
            # Implement your teardown logic here
            pass
"""

from abc import ABC, abstractmethod
from openfactory.schemas.devices import Device


class Connector(ABC):
    """
    Abstract base class defining the interface for device connectors.
    """

    @abstractmethod
    def deploy(self, device: Device, yaml_config_file: str) -> None:
        """
        Deploy a device based on its configuration.

        Args:
            device (Device): Device to deploy.
            yaml_config_file (str): Path to the YAML configuration file.
        """
        pass

    @abstractmethod
    def tear_down(self, device_uuid: str) -> None:
        """
        Tear down a deployed device given its UUID.

        Args:
            device_uuid (str): Unique identifier of the device to be torn down.
        """
        pass
