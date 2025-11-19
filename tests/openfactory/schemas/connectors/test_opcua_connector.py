import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema, OPCUAVariableConfig


class TestOPCUAConnectorSchema(unittest.TestCase):
    """ Unit tests for OPCUAConnectorSchema """

    def test_valid_connector_schema_minimal(self):
        """ Valid minimal connector schema with server and variables """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            }
        }
        schema = OPCUAConnectorSchema(**data)
        self.assertEqual(schema.type, "opcua")
        self.assertEqual(schema.server.uri, "opc.tcp://127.0.0.1:4840/freeopcua/server/")
        temp_var = schema.variables["temp"]
        self.assertIsInstance(temp_var, OPCUAVariableConfig)
        self.assertEqual(temp_var.node_id, "ns=3;i=1050")
        self.assertEqual(temp_var.tag, "Temperature")
        self.assertEqual(temp_var.queue_size, 1)         # default
        self.assertEqual(temp_var.sampling_interval, 0)  # default

    def test_invalid_node_id_format_raises(self):
        """ Invalid node_id format should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "invalid", "tag": "Temperature"}
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("Invalid node_id format", str(cm.exception))

    def test_missing_type_field_raises(self):
        """ Missing 'type' field should raise ValidationError """
        data = {
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            }
        }
        with self.assertRaises(ValidationError):
            OPCUAConnectorSchema(**data)

    def test_extra_fields_forbid(self):
        """ Unknown fields in the top-level schema should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            },
            "foo": "bar"
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("foo", str(cm.exception))

    def test_missing_tag_raises(self):
        """ Missing 'tag' in a variable should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050"}  # tag missing
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("tag", str(cm.exception))

    def test_missing_node_id_raises(self):
        """ Missing 'node_id' in a variable should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"tag": "Temperature"}  # node_id missing
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("node_id", str(cm.exception))

    def test_duplicate_node_id_raises(self):
        """ Duplicate node_id across variables should raise ValidationError """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp1": {"node_id": "ns=3;i=1050", "tag": "Temperature1"},
                "temp2": {"node_id": "ns=3;i=1050", "tag": "Temperature2"}  # duplicate
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("Duplicate node_id within variables", str(cm.exception))

    def test_variables_normalization_with_server_defaults(self):
        """ Variables are normalized to OPCUAVariableConfig and inherit server defaults """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
                "subscription": {"queue_size": 10, "sampling_interval": 100},
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"},
                "hum": {
                    "node_id": "ns=2;i=10",
                    "tag": "Humidity",
                    "queue_size": 5,
                    "sampling_interval": 50
                }
            }
        }
        schema = OPCUAConnectorSchema(**data)

        temp_var = schema.variables["temp"]
        hum_var = schema.variables["hum"]

        # All variables normalized
        self.assertIsInstance(temp_var, OPCUAVariableConfig)
        self.assertIsInstance(hum_var, OPCUAVariableConfig)

        # Server defaults applied
        self.assertEqual(temp_var.queue_size, 10)
        self.assertEqual(temp_var.sampling_interval, 100)

        # Explicit overrides preserved
        self.assertEqual(hum_var.queue_size, 5)
        self.assertEqual(hum_var.sampling_interval, 50)

    def test_mixed_variable_config(self):
        """ Multiple variables with different subscription overrides work """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
                "subscription": {"queue_size": 10, "sampling_interval": 20},
            },
            "variables": {
                "sensor_model": {"node_id": "ns=1;i=1", "tag": "Model"},  # uses defaults
                "temperature": {
                    "node_id": "ns=2;i=2",
                    "tag": "Temp",
                    "queue_size": 5,  # overrides
                    "sampling_interval": 50
                }
            }
        }
        schema = OPCUAConnectorSchema(**data)

        sensor_model = schema.variables["sensor_model"]
        temperature = schema.variables["temperature"]

        # Defaults applied
        self.assertEqual(sensor_model.queue_size, 10)
        self.assertEqual(sensor_model.sampling_interval, 20)

        # Overrides applied
        self.assertEqual(temperature.queue_size, 5)
        self.assertEqual(temperature.sampling_interval, 50)

    def test_variable_deadband_defaults_to_zero(self):
        """ If deadband not provided, it should default to 0.0 """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            }
        }
        schema = OPCUAConnectorSchema(**data)
        temp_var = schema.variables["temp"]
        self.assertEqual(temp_var.deadband, 0.0)

    def test_variable_deadband_explicit_value(self):
        """ Explicit deadband value is preserved """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature", "deadband": 0.1}
            }
        }
        schema = OPCUAConnectorSchema(**data)
        temp_var = schema.variables["temp"]
        self.assertEqual(temp_var.deadband, 0.1)

    def test_multiple_variables_with_mixed_deadband(self):
        """ Some variables have explicit deadband, others default """
        data = {
            "type": "opcua",
            "server": {
                "uri": "opc.tcp://127.0.0.1:4840/freeopcua/server/",
            },
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"},
                "hum": {"node_id": "ns=2;i=10", "tag": "Humidity", "deadband": 0.05}
            }
        }
        schema = OPCUAConnectorSchema(**data)
        temp_var = schema.variables["temp"]
        hum_var = schema.variables["hum"]

        # Default deadband
        self.assertEqual(temp_var.deadband, 0.0)
        # Explicit deadband
        self.assertEqual(hum_var.deadband, 0.05)

    def test_events_parsing(self):
        """ Test proper event parsing """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840/server/"},
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            },
            "events": {
                "iolink_master": {"node_id": "ns=6;i=10"},
                "sensor": {"node_id": "ns=3;i=45"}
            }
        }
        schema = OPCUAConnectorSchema(**data)

        # node_ids should be parsed correctly
        self.assertEqual(schema.events["iolink_master"].namespace_index, 6)
        self.assertEqual(schema.events["iolink_master"].identifier, "10")
        self.assertEqual(schema.events["sensor"].namespace_index, 3)
        self.assertEqual(schema.events["sensor"].identifier, "45")

    def test_duplicate_node_id_within_events(self):
        """ Duplicate node_id within events should raise """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840/server/"},
            "events": {
                "evt1": {"node_id": "ns=3;i=100"},
                "evt2": {"node_id": "ns=3;i=100"}  # duplicate
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("Duplicate node_id within events", str(cm.exception))

    def test_key_conflict_between_variables_and_events(self):
        """ Keys cannot exist in both variables and events """
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840/server/"},
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            },
            "events": {
                "temp": {"node_id": "ns=6;i=10"}  # conflict with variable
            }
        }
        with self.assertRaises(ValidationError) as cm:
            OPCUAConnectorSchema(**data)
        self.assertIn("Local name conflict", str(cm.exception))

    def test_node_id_overlap_between_variables_and_events_allowed(self):
        """ A same node_id can exist both in variables and events"""
        data = {
            "type": "opcua",
            "server": {"uri": "opc.tcp://127.0.0.1:4840/server/"},
            "variables": {
                "temp": {"node_id": "ns=3;i=1050", "tag": "Temperature"}
            },
            "events": {
                "sensor_temp": {"node_id": "ns=3;i=1050"}  # same node_id, different local name
            }
        }
        schema = OPCUAConnectorSchema(**data)
        self.assertIn("temp", schema.variables)
        self.assertIn("sensor_temp", schema.events)
        self.assertEqual(schema.events["sensor_temp"].node_id, "ns=3;i=1050")
