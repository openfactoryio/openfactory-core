import json
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch
from openfactory.monitoring.registry.src.metrics_registry import MetricsRegistry


class TestMetricsRegistry(TestCase):
    """
    Unit tests for MetricsRegistry.
    """

    def setUp(self):
        """ Create a test registry instance. """
        self.registry = MetricsRegistry(
            ksqlClient=MagicMock(),
            bootstrap_servers="broker:9092",
            test_mode=True
        )

    @patch.dict(os.environ, {"PROMETHEUS_SD_ENDPOINT": "/custom-targets"})
    def test_custom_prometheus_sd_endpoint(self):
        registry = MetricsRegistry(
            ksqlClient=MagicMock(),
            bootstrap_servers="broker:9092",
            test_mode=True
        )

        paths = [route.path for route in registry.api.routes]

        self.assertIn("/custom-targets", paths)

    def test_register_target(self):
        """ Should publish a metrics target registration. """
        self.registry.producer = MagicMock()

        self.registry.register_target(
            application_uuid="APP1",
            host="app1",
            port=4000,
            path="/metrics"
        )

        self.registry.producer.produce.assert_called_once_with(
            topic="metrics_targets",
            key="APP1",
            value=json.dumps({
                "HOST": "app1",
                "PORT": 4000,
                "PATH": "/metrics"
            })
        )

    def test_register_target_default_path(self):
        """ Should use /metrics as the default metrics path. """

        self.registry.producer = MagicMock()

        self.registry.register_target(
            application_uuid="APP1",
            host="app1",
            port=4000
        )

        self.registry.producer.produce.assert_called_once_with(
            topic="metrics_targets",
            key="APP1",
            value=json.dumps({
                "HOST": "app1",
                "PORT": 4000,
                "PATH": "/metrics"
            })
        )

    def test_deregister_target(self):
        """ Should publish a tombstone record. """

        self.registry.producer = MagicMock()

        self.registry.deregister_target(
            application_uuid="APP1"
        )

        self.registry.producer.produce.assert_called_once_with(
            topic="metrics_targets",
            key="APP1",
            value=None
        )

    def test_prometheus_targets(self):
        """ Should return targets in Prometheus HTTP service discovery format. """

        self.registry.ksql.query.return_value = [
            {
                "APPLICATION_UUID": "APP1",
                "HOST": "app1",
                "PORT": 4000,
                "PATH": "/metrics"
            }
        ]

        result = self.registry.prometheus_targets()

        self.assertEqual(
            result,
            [
                {
                    "targets": [
                        "app1:4000"
                    ],
                    "labels": {
                        "__metrics_path__": "/metrics"
                    }
                }
            ]
        )

    def test_prometheus_targets_multiple(self):
        """ Should return multiple targets in Prometheus HTTP service discovery format. """

        self.registry.ksql.query.return_value = [
            {
                "APPLICATION_UUID": "APP1",
                "HOST": "app1",
                "PORT": 4000,
                "PATH": "/metrics"
            },
            {
                "APPLICATION_UUID": "APP2",
                "HOST": "app2",
                "PORT": 8000,
                "PATH": "/prometheus"
            }
        ]

        result = self.registry.prometheus_targets()

        self.assertEqual(len(result), 2)

        self.assertEqual(
            result[0],
            {
                "targets": [
                    "app1:4000"
                ],
                "labels": {
                    "__metrics_path__": "/metrics"
                }
            }
        )

        self.assertEqual(
            result[1],
            {
                "targets": [
                    "app2:8000"
                ],
                "labels": {
                    "__metrics_path__": "/prometheus"
                }
            }
        )

    def test_prometheus_targets_empty(self):
        """ Should return an empty list when no targets are registered. """
        self.registry.ksql.query.return_value = []

        result = self.registry.prometheus_targets()

        self.assertEqual(result, [])
