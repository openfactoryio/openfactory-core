import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import (
    OPCUAConnectorSchema,
    OPCUAServerConfig,
    OPCUASubscriptionConfig
)


class TestOPCUAServerConfig(unittest.TestCase):
    """
    Unit tests for OPCUAServerConfig
    """

    def test_valid_server_config(self):
        """ Test valid server configuration. """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
        }
        server = OPCUAServerConfig(**data)
        self.assertEqual(server.uri, data["uri"])

    def test_missing_uri_raises_validation_error(self):
        """ Missing 'uri' should raise ValidationError. """
        data = {
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAServerConfig(**data)
        err_str = str(cm.exception)
        self.assertIn("uri", err_str)

    def test_extra_fields_forbid_raises_validation_error(self):
        """ Providing unknown fields should raise ValidationError. """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            "namespace_uri": "http://examples.openfactory.local/opcua",
            "foo": "bar"
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAServerConfig(**data)
        err_str = str(cm.exception)
        self.assertIn("foo", err_str)
        self.assertIn("extra", err_str.lower())

    def test_server_with_subscription(self):
        """ Server with subscription object should be valid """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840",
            "subscription": {
                "publishing_interval": 100,
                "queue_size": 10,
                "sampling_interval": 50
            }
        }
        server = OPCUAServerConfig(**data)
        self.assertIsInstance(server.subscription, OPCUASubscriptionConfig)
        self.assertEqual(server.subscription.publishing_interval, 100)
        self.assertEqual(server.subscription.queue_size, 10)
        self.assertEqual(server.subscription.sampling_interval, 50)

    def test_server_subscription_optional(self):
        """ Server without subscription is valid and defaults to None """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840",
        }
        server = OPCUAServerConfig(**data)

        self.assertIsInstance(server.subscription, OPCUASubscriptionConfig)
        self.assertEqual(server.subscription.publishing_interval, 100.0)
        self.assertEqual(server.subscription.queue_size, 1)
        self.assertEqual(server.subscription.sampling_interval, 0.0)

    def test_subscription_extra_fields_forbid(self):
        """ Extra fields in subscription should raise ValidationError """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840",
            "namespace_uri": "http://example.com",
            "subscription": {"publishing_interval": 100, "foo": "bar"}
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAServerConfig(**data)
        self.assertIn("foo", str(cm.exception))
        self.assertIn("extra", str(cm.exception).lower())

    def test_variables_normalized_from_server_defaults(self):
        """ Ensure variables inherit server subscription defaults when not provided. """
        schema_data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
                # subscription omitted → defaults: publishing_interval=100, queue_size=1, sampling_interval=0
                "variables": {
                    "temp": {                  # simple variable → should inherit server defaults
                        "node_id": "ns=3;i=1050",
                        "tag": "Temperature"
                    },
                    "hum": {                   # full config → overrides queue_size and sampling_interval
                        "node_id": "ns=2;i=10",
                        "tag": "Humidity",
                        "queue_size": 5,
                        "sampling_interval": 50
                    }
                }
            }
        }

        schema = OPCUAConnectorSchema(**schema_data)
        temp_var = schema.server.variables["temp"]
        hum_var = schema.server.variables["hum"]

        # Server defaults applied to temp
        self.assertEqual(temp_var.queue_size, 1)
        self.assertEqual(temp_var.sampling_interval, 0)

        # Overrides preserved for hum
        self.assertEqual(hum_var.queue_size, 5)
        self.assertEqual(hum_var.sampling_interval, 50)

        # Tag values must be present
        self.assertEqual(temp_var.tag, "Temperature")
        self.assertEqual(hum_var.tag, "Humidity")
