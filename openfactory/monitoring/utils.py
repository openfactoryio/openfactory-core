from openfactory.kafka.ksql import KSQLDBClient
from openfactory.assets import Asset
from openfactory.schemas.apps import OpenFactoryAppSchema
from openfactory.exceptions import OFAException


def discover_prometheus_registry(ksqlClient: KSQLDBClient):
    """ Discover deployed Prometheus registry """
    query = "select ASSET_UUID from ASSETS_TYPE where TYPE='Prometheus.Registry';"
    registry = ksqlClient.query(query)
    if not registry:
        raise OFAException("No Prometheus Registry deployed")
    return registry[0]['ASSET_UUID']


def register_prometheus_target(target: OpenFactoryAppSchema,
                               ksqlClient: KSQLDBClient, bootstrap_servers: str):
    """
    Register a new Prometheus target

    Args:
        target (OpenFactoryAppSchema): target to register.
        ksqlClient: (KSQLDBClient) KSQL client for executing queries.
        bootstrap_servers (str): Kafka bootstrap server address.
    """
    registry_uuid = discover_prometheus_registry(ksqlClient)
    registry = Asset(registry_uuid, ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers)
    registry.method('register_target', 'ofa-cli',
                    args=[
                        ('application_uuid', target.uuid),
                        ('host', target.uuid.lower()),
                        ('port', str(target.metrics.port)),
                        ('path', target.metrics.path),
                        ])
    registry.close()


def deregister_prometheus_target(target_uuid: str,
                                 ksqlClient: KSQLDBClient, bootstrap_servers: str):
    """
    Deregister a new Prometheus target

    Args:
        target_uid (str): OpenFactory UUID of target to deregister.
        ksqlClient: (KSQLDBClient) KSQL client for executing queries.
        bootstrap_servers (str): Kafka bootstrap server address.
    """
    try:
        registry_uuid = discover_prometheus_registry(ksqlClient)
    except OFAException:
        return
    registry = Asset(registry_uuid, ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers)
    registry.method('deregister_target', 'ofa-cli', args=[('application_uuid', target_uuid)])
    registry.close()
