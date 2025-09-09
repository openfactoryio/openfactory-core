import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUAServerConfig, OPCUASubscriptionConfig


class TestOPCUAServerConfig(unittest.TestCase):
    """
    Unit tests for OPCUAServerConfig
    """

    def test_valid_server_config(self):
        """ Test valid server configuration. """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            "namespace_uri": "http://examples.openfactory.local/opcua"
        }
        server = OPCUAServerConfig(**data)
        self.assertEqual(server.uri, data["uri"])
        self.assertEqual(server.namespace_uri, data["namespace_uri"])

    def test_missing_uri_raises_validation_error(self):
        """ Missing 'uri' should raise ValidationError. """
        data = {
            "namespace_uri": "http://examples.openfactory.local/opcua"
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAServerConfig(**data)
        err_str = str(cm.exception)
        self.assertIn("uri", err_str)

    def test_missing_namespace_uri_raises_validation_error(self):
        """ Missing 'namespace_uri' should raise ValidationError. """
        data = {
            "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/"
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAServerConfig(**data)
        err_str = str(cm.exception)
        self.assertIn("namespace_uri", err_str)

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
            "namespace_uri": "http://example.com",
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
            "namespace_uri": "http://example.com"
        }
        server = OPCUAServerConfig(**data)
        self.assertIsNone(server.subscription)

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
