"""
Registry and factory for OpenFactory connectors.

This module provides a centralized registry for associating connector schema classes
with their runtime implementations. It also provides a factory function to instantiate
connector objects based on a device's schema.

Example usage:
    .. code-block:: python

        from openfactory.connectors.registry import build_connector
        connector = build_connector(schema, deployment_strategy, ksql, bootstrap_servers)
"""

from typing import Type, Callable, Any, Dict
from openfactory.connectors.base_connector import Connector
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy
from openfactory.kafka.ksql import KSQLDBClient

# Registry mapping schema classes to connector runtime classes
CONNECTOR_REGISTRY: Dict[Type[Any], Type[Connector]] = {}


def register_connector(schema_cls: Type[Any]) -> Callable[[Type[Connector]], Type[Connector]]:
    """
    Decorator to register a connector class for a given schema class.

    Args:
        schema_cls (Type[Any]): The schema class to associate with the connector.

    Returns:
        Callable: A decorator that registers the connector class and returns it.

    Example usage:
       .. code-block:: python

          @register_connector(MTConnectConnectorSchema)
          class MTConnectConnector(Connector):
              ...
    """
    def decorator(runtime_cls):
        CONNECTOR_REGISTRY[schema_cls] = runtime_cls
        return runtime_cls
    return decorator


def build_connector(schema: Any,
                    deployment_strategy: OpenFactoryServiceDeploymentStrategy,
                    ksql: KSQLDBClient,
                    bootstrap_servers: str) -> Connector:
    """
    Factory function to instantiate a connector for a given schema.

    Args:
        schema (Any): An instance of a connector schema.
        deployment_strategy (OpenFactoryServiceDeploymentStrategy): Deployment strategy to use.
        ksql (KSQLDBClient): Client for interacting with ksqlDB.
        bootstrap_servers (str): Kafka bootstrap server address.

    Raises:
        ValueError: If no connector is registered for the schema's type.

    Returns:
        Connector: An instance of the registered connector runtime class.

    Example usage:
       .. code-block:: python

           connector = build_connector(schema, deployment_strategy, ksql, bootstrap_servers)
    """
    runtime_cls = CONNECTOR_REGISTRY.get(type(schema))
    if not runtime_cls:
        raise ValueError(f"No runtime registered for schema {type(schema).__name__}")
    return runtime_cls(deployment_strategy, ksql, bootstrap_servers)
