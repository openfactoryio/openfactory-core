import unittest
from unittest.mock import MagicMock
from openfactory.connectors.registry import CONNECTOR_REGISTRY, register_connector, build_connector
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.connectors.base_connector import Connector


# Mock schema classes
class FakeSchemaA:
    """ Fake schema class for testing. """
    pass


class FakeSchemaB:
    """ Another fake schema class for testing. """
    pass


# Mock connector classes
class FakeConnectorA(Connector):
    """
    Fake connector implementing abstract methods for testing
    """

    def __init__(self, deployment_strategy, ksql, bootstrap_servers):
        self.deployment_strategy = deployment_strategy
        self.ksql = ksql
        self.bootstrap_servers = bootstrap_servers

    def deploy(self, device, yaml_config_file):
        pass

    def tear_down(self, device_uuid):
        pass


class FakeConnectorB(Connector):
    """
    Another fake connector implementing abstract methods for testing.
    """

    def __init__(self, deployment_strategy, ksql, bootstrap_servers):
        self.deployment_strategy = deployment_strategy
        self.ksql = ksql
        self.bootstrap_servers = bootstrap_servers

    def deploy(self, device, yaml_config_file):
        pass

    def tear_down(self, device_uuid):
        pass


class TestConnectorRegistry(unittest.TestCase):
    """
    Unit tests for connector registry and factory functions.
    """

    def setUp(self):
        # Clear the registry before each test
        CONNECTOR_REGISTRY.clear()
        self.mock_deploy = MagicMock(spec=OpenFactoryServiceDeploymentStrategy)
        self.mock_ksql = MagicMock(spec=KSQLDBClient)
        self.bootstrap_servers = "fake:9092"

    def test_register_connector_adds_to_registry(self):
        """ Decorator should register a connector class in CONNECTOR_REGISTRY. """
        @register_connector(FakeSchemaA)
        class MyConnector(FakeConnectorA):
            pass

        self.assertIn(FakeSchemaA, CONNECTOR_REGISTRY)
        self.assertIs(CONNECTOR_REGISTRY[FakeSchemaA], MyConnector)

    def test_register_connector_returns_class(self):
        """ Decorator should return the original connector class. """
        @register_connector(FakeSchemaA)
        class MyConnector(FakeConnectorA):
            pass

        self.assertIs(MyConnector.__name__, "MyConnector")

    def test_build_connector_returns_instance(self):
        """ Factory should return an instance of the registered connector class. """
        @register_connector(FakeSchemaA)
        class MyConnector(FakeConnectorA):
            pass

        schema = FakeSchemaA()
        instance = build_connector(schema, self.mock_deploy, self.mock_ksql, self.bootstrap_servers)

        self.assertIsInstance(instance, MyConnector)
        self.assertIs(instance.deployment_strategy, self.mock_deploy)
        self.assertIs(instance.ksql, self.mock_ksql)
        self.assertEqual(instance.bootstrap_servers, self.bootstrap_servers)

    def test_build_connector_unregistered_schema_raises(self):
        """ Factory should raise ValueError if no connector is registered for schema type. """
        schema = FakeSchemaB()
        with self.assertRaises(ValueError) as cm:
            build_connector(schema, self.mock_deploy, self.mock_ksql, self.bootstrap_servers)

        self.assertIn("No runtime registered", str(cm.exception))

    def test_register_multiple_connectors(self):
        """ Multiple connectors should register correctly under different schema types. """
        @register_connector(FakeSchemaA)
        class ConnectorA(FakeConnectorA):
            pass

        @register_connector(FakeSchemaB)
        class ConnectorB(FakeConnectorB):
            pass

        self.assertIs(CONNECTOR_REGISTRY[FakeSchemaA], ConnectorA)
        self.assertIs(CONNECTOR_REGISTRY[FakeSchemaB], ConnectorB)
