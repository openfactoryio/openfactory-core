"""
OpenFactory API.

This module provides the `OpenFactory` class, which serves as the main interface
to deployed assets, devices, and services in an OpenFactory environment through
ksqlDB queries.

Core responsibilities:
    - Retrieve deployed assets and their UUIDs
    - Access availability information for assets
    - Query Docker services associated with assets
    - Classify assets by type (devices, MTConnect agents, Kafka producers, supervisors, applications)
    - Provide high-level `Asset` objects for interacting with deployed components

Key integrations:
    - ksqlDB for querying asset metadata and status
    - Kafka for underlying streaming infrastructure
    - Asset abstraction for modeling and interacting with deployed entities

Usage Example:
    .. code-block:: python

        from openfactory import OpenFactory
        from openfactory.kafka.ksql import KSQLDBClient
        import openfactory.config as config

        ofa = OpenFactory(ksqlClient=KSQLDBClient(config.KSQLDB_URL))

        # List all UUID of deployed assets
        print(ofa.assets_uuid())

Error handling:
    - Returns empty lists when no results are available
    - Relies on the KSQLDBClient to report query execution errors
"""

from typing import List
import openfactory.config as config
from openfactory.assets import Asset
from openfactory.kafka.ksql import KSQLDBClient


class OpenFactory:
    """
    Main API to OpenFactory.

    Provides access to deployed assets, their availability, Docker services,
    and their classification by type (devices, agents, etc.).
    """

    def __init__(self, ksqlClient: KSQLDBClient, bootstrap_servers: str = config.KAFKA_BROKER):
        """
        Initialize the OpenFactory API.

        Args:
            ksqlClient (KSQLDBClient): A client capable of executing KSQL queries.
            bootstrap_servers (str): Kafka bootstrap server address. Defaults to ``openfactory.config.KAFKA_BROKER``.
        """
        self.bootstrap_servers = bootstrap_servers
        self.ksql = ksqlClient

    def assets_uuid(self) -> List[str]:
        """
        Get list of asset UUIDs deployed on OpenFactory.

        Returns:
            List[str]: UUIDs of all deployed assets.
        """
        query = "SELECT ASSET_UUID FROM assets_type;"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def assets(self) -> List[Asset]:
        """
        Get list of Asset objects deployed on OpenFactory.

        Returns:
            List[Asset]: Deployed Asset instances.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.assets_uuid()]

    def assets_availability(self) -> list[dict]:
        """
        Get availability data for all deployed assets.

        Returns:
            list[dict]: Availability data of deployed assets.
        """
        query = "SELECT * FROM assets_avail;"
        return self.ksql.query(query)

    def assets_docker_services(self) -> list[dict]:
        """
        Get Docker services associated with all deployed assets.

        Returns:
            list[dict]: Docker services data of deployed assets.
        """
        query = "SELECT * FROM docker_services;"
        return self.ksql.query(query)

    def devices_uuid(self) -> List[str]:
        """
        Get UUIDs of all devices deployed on OpenFactory.

        Returns:
            List[str]: UUIDs of deployed device-type assets.
        """
        query = "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'Device';"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def devices(self) -> List[Asset]:
        """
        Get Asset objects corresponding to deployed devices.

        Returns:
            List[Asset]: Deployed device-type assets.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.devices_uuid()]

    def agents_uuid(self) -> List[str]:
        """
        Get UUIDs of deployed MTConnect agents.

        Returns:
            List[str]: UUIDs of deployed MTConnect agents.
        """
        query = "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'MTConnectAgent';"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def agents(self) -> List[Asset]:
        """
        Get `Asset` objects corresponding to deployed MTConnect agents.

        Returns:
            List[Asset]: Deployed MTConnect agent assets.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.agents_uuid()]

    def producers_uuid(self) -> List[str]:
        """
        Get UUIDs of deployed Kafka producers.

        Returns:
            List[str]: UUIDs of deployed Kafka producer assets.
        """
        query = "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'KafkaProducer';"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def producers(self) -> List[Asset]:
        """
        Get Asset objects corresponding to deployed Kafka producers.

        Returns:
            List[Asset]: Kafka producer assets.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.producers_uuid()]

    def supervisors_uuid(self) -> List[str]:
        """
        Get UUIDs of deployed Supervisors.

        Returns:
            List[str]: UUIDs of deployed supervisor-type assets.
        """
        query = "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'Supervisor';"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def supervisors(self) -> List[Asset]:
        """
        Get Asset objects corresponding to deployed Supervisors.

        Returns:
            List[Asset]: Deployed supervisor-type assets.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.supervisors_uuid()]

    def applications_uuid(self) -> List[str]:
        """
        Get UUIDs of deployed OpenFactory applications.

        Returns:
            List[str]: UUIDs of deployed OpenFactory application-type assets.
        """
        query = "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'OpenFactoryApp';"
        results = self.ksql.query(query)
        return [row['ASSET_UUID'] for row in results] if results else []

    def applications(self) -> List[Asset]:
        """
        Get Asset objects corresponding to deployed OpenFactory applications.

        Returns:
            List[Asset]: Deployed OpenFactory application-type assets.
        """
        return [Asset(uuid, self.ksql, self.bootstrap_servers) for uuid in self.applications_uuid()]
