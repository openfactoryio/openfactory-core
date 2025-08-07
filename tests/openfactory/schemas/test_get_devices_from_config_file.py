import unittest
import yaml
import os
from unittest.mock import patch
from tempfile import NamedTemporaryFile
from openfactory.schemas.devices import get_devices_from_config_file
from openfactory.schemas.uns import UNSSchema


class TestGetDevicesFromConfigFile(unittest.TestCase):
    """
    Unit tests for get_devices_from_config_file function
    """

    def setUp(self):
        """ Prepare a minimal valid UNS schema and device config. """
        self.schema_data = {
            "namespace_structure": [
                {"inc": "OpenFactory"},
                {"workcenter": ["WC1", "WC2"]},
                {"asset": "ANY"},
                {"attribute": "ANY"}
            ],
            "uns_template": "inc/workcenter/asset/attribute"
        }
        self.uns_schema_file = NamedTemporaryFile(mode="w+", delete=False)
        yaml.dump(self.schema_data, self.uns_schema_file)
        self.uns_schema_file.close()
        self.uns_schema = UNSSchema(schema_yaml_file=self.uns_schema_file.name)

        self.valid_devices_data = {
            "devices": {
                "device1": {
                    "uuid": "uuid1",
                    "connector": {
                        "type": "mtconnect",
                        "agent": {
                            "port": 8080,
                            "device_xml": "xml1",
                            "adapter": {"image": "ofa/adapter", "port": 9090}
                        }
                    },
                    "ksql_tables": [],
                    "uns": {"workcenter": "WC2", "asset": "cnc"}
                }
            }
        }

    def tearDown(self):
        os.remove(self.uns_schema_file.name)

    def test_successful_load_and_enrich(self):
        """ Test successful YAML loading, device validation, UNS attachment, and correct device dictionary return. """
        with NamedTemporaryFile(mode='w') as temp_file:
            yaml.dump(self.valid_devices_data, temp_file)
            temp_file.flush()
            with patch("openfactory.schemas.devices.user_notify") as mock_notify:
                devices_dict = get_devices_from_config_file(temp_file.name, self.uns_schema)
                self.assertIsInstance(devices_dict, dict)
                self.assertIn("device1", devices_dict)
                mock_notify.fail.assert_not_called()

    def test_invalid_yaml_returns_none_and_notifies(self):
        """ Test that invalid YAML triggers validation failure, calls user_notify.fail, and returns None. """
        invalid_data = {
            "devices": {
                "device1": {
                    "uuid": "uuid1",
                    "connector": {
                        "type": "mtconnect",
                        "agent": {
                            "port": 8080,
                            "adapter": {"port": 9090}  # missing ip or image, invalid
                        }
                    }
                }
            }
        }
        with NamedTemporaryFile(mode='w') as temp_file:
            yaml.dump(invalid_data, temp_file)
            temp_file.flush()
            with patch("openfactory.schemas.devices.user_notify") as mock_notify:
                devices_dict = get_devices_from_config_file(temp_file.name, self.uns_schema)
                self.assertIsNone(devices_dict)
                mock_notify.fail.assert_called_once()

    def test_attach_uns_failure_returns_none_and_notifies(self):
        """ Test that UNS attachment failure calls user_notify.fail and returns None. """
        with NamedTemporaryFile(mode='w') as temp_file:
            yaml.dump(self.valid_devices_data, temp_file)
            temp_file.flush()
            with patch("openfactory.schemas.devices.user_notify") as mock_notify:
                with patch("openfactory.schemas.devices.Device.attach_uns", side_effect=Exception("fail")):
                    devices_dict = get_devices_from_config_file(temp_file.name, self.uns_schema)
                    self.assertIsNone(devices_dict)
                    mock_notify.fail.assert_called_once()
