import json
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.kafka import AssetProducer
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
    topic = ksqlClient.get_kafka_topic('METRICS_TARGETS_SOURCE')
    producer = AssetProducer(ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers)
    producer.produce(
        topic=topic,
        key=target.uuid,
        value=json.dumps({
            "HOST": target.uuid.lower(),
            "PORT": str(target.metrics.port),
            "PATH": target.metrics.path
        })
    )
    producer.flush()


def deregister_prometheus_target(target_uuid: str,
                                 ksqlClient: KSQLDBClient, bootstrap_servers: str):
    """
    Deregister a new Prometheus target

    Args:
        target_uuid (str): OpenFactory UUID of target to deregister.
        ksqlClient: (KSQLDBClient) KSQL client for executing queries.
        bootstrap_servers (str): Kafka bootstrap server address.
    """
    topic = ksqlClient.get_kafka_topic('METRICS_TARGETS_SOURCE')
    producer = AssetProducer(ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers)
    producer.produce(
        topic=topic,
        key=target_uuid,
        value=None
    )
    producer.flush()
