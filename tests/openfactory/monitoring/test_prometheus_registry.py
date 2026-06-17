from unittest import TestCase
from unittest.mock import patch, MagicMock
from openfactory.monitoring.utils import discover_prometheus_registry, register_prometheus_target
from openfactory.exceptions import OFAException


class TestLocalBackend(TestCase):
    """
    Unit tests for discover_prometheus_registry method.
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

    @patch("openfactory.monitoring.utils.Asset")
    @patch("openfactory.monitoring.utils.discover_prometheus_registry")
    def test_register_prometheus_target(self, mock_discover_registry, MockAsset):
        """ Should invoke register_target on the discovered registry. """

        mock_discover_registry.return_value = "registry-1"

        target = MagicMock()
        target.uuid = "APP1"
        target.metrics = MagicMock()
        target.metrics.port = 4000
        target.metrics.path = "/metrics"

        ksql = MagicMock()

        register_prometheus_target(target, ksqlClient=ksql, bootstrap_servers="broker:9092")

        MockAsset.assert_called_once_with(
            "registry-1",
            ksqlClient=ksql,
            bootstrap_servers="broker:9092"
        )

        registry = MockAsset.return_value

        registry.method.assert_called_once_with(
            "register_target",
            "ofa-cli",
            args=[
                ("application_uuid", "APP1"),
                ("host", "app1"),
                ("port", "4000"),
                ("path", "/metrics")
            ]
        )

        registry.close.assert_called_once()
