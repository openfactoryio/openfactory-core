import unittest
from openfactory.schemas.devices import DevicesConfig


class TestDevicesConfig(unittest.TestCase):
    """
    Unit tests for class DevicesConfig.
    """

    def setUp(self):
        """
        Prepare a minimal valid device configuration for use in tests.
        """
        self.valid_device = {
            "uuid": "uuid1",
            "connector": {
                "type": "mtconnect",
                "agent": {
                    "port": 8080,
                    "device_xml": "xml1",
                    "adapter": {"image": "ofa/adapter", "port": 9090}
                }
            }
        }

    def test_validate_devices_valid(self):
        """
        Test case where devices have valid configurations.
        """
        cfg = DevicesConfig(devices={"dev1": self.valid_device})
        self.assertIsNone(cfg.validate_devices())

    def test_duplicate_uuid_raises(self):
        """
        Two devices with the same UUID should raise ValueError.
        """
        cfg = DevicesConfig(devices={
            "dev1": self.valid_device,
            "dev2": {**self.valid_device},  # same uuid as dev1
        })
        with self.assertRaises(ValueError) as cm:
            cfg.validate_devices()
        self.assertIn("Duplicate uuid", str(cm.exception))

    def test_devices_dict_property(self):
        """
        devices_dict should return a plain serializable dictionary.
        """
        cfg = DevicesConfig(devices={"dev1": self.valid_device})
        out = cfg.devices_dict
        self.assertIsInstance(out, dict)
        self.assertIn("dev1", out)
        self.assertEqual(out["dev1"]["uuid"], "uuid1")
