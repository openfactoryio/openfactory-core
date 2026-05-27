import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.shdr import SHDRDataPointSchema, SHDRConnectorSchema


class TestSHDRConnectorSchema(unittest.TestCase):
    """ Unit tests for SHDRConnectorSchema """

    def test_valid_connector_schema_minimal(self):
        """ Valid minimal connector schema """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        schema = SHDRConnectorSchema(**data)

        self.assertEqual(schema.type, "shdr")
        self.assertEqual(str(schema.host), "127.0.0.1")
        self.assertEqual(schema.port, 7878)

        temp = schema.data["temp"]

        self.assertIsInstance(temp, SHDRDataPointSchema)
        self.assertEqual(temp.tag, "Temperature")
        self.assertEqual(temp.type, "Samples")

    def test_missing_type_field_raises(self):
        """ Missing 'type' field should raise ValidationError """
        data = {
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("type",))
        self.assertEqual(errors[0]["type"], "missing")

    def test_invalid_type_field_raises(self):
        """ Invalid connector type should raise ValidationError """
        data = {
            "type": "does_not_exist",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("type",))
        self.assertEqual(errors[0]["type"], "literal_error")

    def test_missing_host_raises(self):
        """ Missing host should raise ValidationError """
        data = {
            "type": "shdr",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("host",))
        self.assertEqual(errors[0]["type"], "missing")

    def test_invalid_host_raises(self):
        """ Invalid IP address should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "not-an-ip",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("host",))
        self.assertEqual(errors[0]["type"], "ip_any_address")

    def test_missing_port_raises(self):
        """ Missing port should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("port",))
        self.assertEqual(errors[0]["type"], "missing")

    def test_invalid_port_low_raises(self):
        """ Port lower than 1 should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 0,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("port",))
        self.assertEqual(errors[0]["type"], "greater_than_equal")

    def test_invalid_port_high_raises(self):
        """ Port higher than 65535 should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 70000,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("port",))
        self.assertEqual(errors[0]["type"], "less_than_equal")

    def test_missing_data_point_tag_raises(self):
        """ Missing tag in data point should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("data", "temp", "tag"))
        self.assertEqual(errors[0]["type"], "missing")

    def test_missing_data_point_type_raises(self):
        """ Missing type in data point should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("data", "temp", "type"))
        self.assertEqual(errors[0]["type"], "missing")

    def test_invalid_data_point_type_raises(self):
        """ Invalid data point type should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Invalid"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("data", "temp", "type"))
        self.assertEqual(errors[0]["type"], "literal_error")

    def test_multiple_data_points(self):
        """ Multiple data points are parsed correctly """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                },
                "humi": {
                    "tag": "Humidity",
                    "type": "Events"
                }
            }
        }

        schema = SHDRConnectorSchema(**data)

        self.assertEqual(schema.data["temp"].tag, "Temperature")
        self.assertEqual(schema.data["temp"].type, "Samples")

        self.assertEqual(schema.data["humi"].tag, "Humidity")
        self.assertEqual(schema.data["humi"].type, "Events")

    def test_duplicate_tags_allowed(self):
        """ Multiple data points can use the same SHDR tag """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp1": {
                    "tag": "Temperature",
                    "type": "Samples"
                },
                "temp2": {
                    "tag": "Temperature",
                    "type": "Events"
                }
            }
        }

        schema = SHDRConnectorSchema(**data)

        self.assertEqual(schema.data["temp1"].tag, "Temperature")
        self.assertEqual(schema.data["temp2"].tag, "Temperature")

    def test_extra_top_level_field_forbidden(self):
        """ Unknown top-level fields should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "foo": "bar",
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("foo",))
        self.assertEqual(errors[0]["type"], "extra_forbidden")

    def test_extra_data_point_field_forbidden(self):
        """ Unknown fields in data point should raise ValidationError """
        data = {
            "type": "shdr",
            "host": "127.0.0.1",
            "port": 7878,
            "data": {
                "temp": {
                    "tag": "Temperature",
                    "type": "Samples",
                    "foo": "bar"
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            SHDRConnectorSchema(**data)

        errors = cm.exception.errors()

        self.assertEqual(errors[0]["loc"], ("data", "temp", "foo"))
        self.assertEqual(errors[0]["type"], "extra_forbidden")
