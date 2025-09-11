import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import (
    OPCUAConnectorSchema,
    OPCUAServerConfig,
    OPCUAVariableConfig,
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
                "uri": "opc.tcp://127.0.0.1:4840",
                "namespace_uri": "http://example.com"
                # subscription omitted
            },
            "device": {
                "path": "Sensors/TemperatureSensor_1",
                "variables": {
                    "temp": "Temperature",  # simple string → should inherit server defaults
                    "hum": {                # full config → overrides queue_size and sampling_interval
                        "browse_name": "Humidity",
                        "queue_size": 5,
                        "sampling_interval": 50
                    }
                },
                "methods": {"calibrate": "Calibrate"}
            }
        }

        schema = OPCUAConnectorSchema(**schema_data)

        # Check server subscription defaults
        self.assertIsInstance(schema.server.subscription, OPCUASubscriptionConfig)
        self.assertEqual(schema.server.subscription.publishing_interval, 100.0)
        self.assertEqual(schema.server.subscription.queue_size, 1)
        self.assertEqual(schema.server.subscription.sampling_interval, 0.0)

        # Check variable normalization
        temp_var = schema.device.variables["temp"]
        self.assertIsInstance(temp_var, OPCUAVariableConfig)
        self.assertEqual(temp_var.browse_name, "Temperature")
        self.assertEqual(temp_var.queue_size, 1)  # inherited from server
        self.assertEqual(temp_var.sampling_interval, 0.0)  # inherited from server

        hum_var = schema.device.variables["hum"]
        self.assertIsInstance(hum_var, OPCUAVariableConfig)
        self.assertEqual(hum_var.browse_name, "Humidity")
        self.assertEqual(hum_var.queue_size, 5)  # overridden
        self.assertEqual(hum_var.sampling_interval, 50)  # overridden

    def test_device_without_variables(self):
        """ Device can have no variables and schema still initializes. """
        schema_data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840", "namespace_uri": "http://example.com"},
            "device": {"path": "Sensors/EmptyDevice"}
        }
        schema = OPCUAConnectorSchema(**schema_data)
        self.assertIsNone(schema.device.variables)
