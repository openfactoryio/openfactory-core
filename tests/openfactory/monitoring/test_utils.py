import json
from unittest import TestCase
from unittest.mock import patch, MagicMock
from openfactory.monitoring.utils import discover_prometheus_registry, register_prometheus_target, deregister_prometheus_target
from openfactory.exceptions import OFAException


class TestUtils(TestCase):
    """
    Unit tests for Prometheus registry helper functions.
    """

    def test_discover_prometheus_registry(self):
        """ Should return the UUID of the deployed Prometheus registry. """

        ksql = MagicMock()
        ksql.query.return_value = [{"ASSET_UUID": "registry-1"}]

        result = discover_prometheus_registry(ksql)

        self.assertEqual(result, "registry-1")
        ksql.query.assert_called_once_with(
            "select ASSET_UUID from ASSETS_TYPE where TYPE='Prometheus.Registry';"
        )

    def test_discover_prometheus_registry_not_found(self):
        """ Should raise OFAException when no registry exists. """

        ksql = MagicMock()
        ksql.query.return_value = []

        with self.assertRaises(OFAException) as ctx:
            discover_prometheus_registry(ksql)

        self.assertEqual(
            str(ctx.exception),
            "No Prometheus Registry deployed"
        )

    @patch("openfactory.monitoring.utils.AssetProducer")
    def test_register_prometheus_target(self, MockAssetProducer):
        """ Should publish target registration to METRICS_TARGETS_SOURCE. """

        target = MagicMock()
        target.uuid = "APP1"
        target.metrics = MagicMock()
        target.metrics.port = 4000
        target.metrics.path = "/metrics"

        ksql = MagicMock()
        ksql.get_kafka_topic.return_value = "metrics-topic"
        producer = MockAssetProducer.return_value

        register_prometheus_target(target, ksqlClient=ksql, bootstrap_servers="broker:9092")

        ksql.get_kafka_topic.assert_called_once_with("METRICS_TARGETS_SOURCE")
        MockAssetProducer.assert_called_once_with(ksqlClient=ksql, bootstrap_servers="broker:9092")
        producer.produce.assert_called_once_with(
            topic="metrics-topic",
            key="APP1",
            value=json.dumps({
                "HOST": "app1",
                "PORT": "4000",
                "PATH": "/metrics"
            })
        )
        producer.flush.assert_called_once()

    @patch("openfactory.monitoring.utils.AssetProducer")
    def test_deregister_prometheus_target(self, MockAssetProducer):
        """ Should publish tombstone message to deregister target. """

        ksql = MagicMock()
        ksql.get_kafka_topic.return_value = "metrics-topic"
        producer = MockAssetProducer.return_value

        deregister_prometheus_target(target_uuid="APP1", ksqlClient=ksql, bootstrap_servers="broker:9092")

        ksql.get_kafka_topic.assert_called_once_with("METRICS_TARGETS_SOURCE")
        MockAssetProducer.assert_called_once_with(ksqlClient=ksql, bootstrap_servers="broker:9092")
        producer.produce.assert_called_once_with(
            topic="metrics-topic",
            key="APP1",
            value=None
        )
        producer.flush.assert_called_once()
