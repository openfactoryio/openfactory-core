"""
Defines the abstract base class for device connectors.

Each connector handles deployment and teardown of devices
for a specific connector type (e.g., OpenFactory, MTConnect, OPCUA).
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
