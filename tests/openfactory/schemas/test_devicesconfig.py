import unittest
from unittest.mock import MagicMock, patch
from openfactory.schemas.devices import DevicesConfig, Device


class TestDevicesConfig(unittest.TestCase):
    """
    Unit tests for class DevicesConfig.
    """

    def setUp(self):
        """
        Prepare a minimal valid device configuration for use in tests.
        """
        self.valid_device = Device(
            uuid="uuid1",
            connector={
                "type": "mtconnect",
                "agent": {
                    "port": 8080,
                    "device_xml": "xml1",
                    "adapter": {"image": "ofa/adapter", "port": 9090}
                }
            }
        )

        # Mock UNS schema
        self.mock_uns_schema = MagicMock()

        # Patch attach_uns so it doesn’t actually do anything
        Device.attach_uns = MagicMock()

    def test_validate_devices_valid(self):
        """
        Test case where devices have valid configurations.
        """
        cfg = DevicesConfig(devices={"dev1": self.valid_device})
        self.assertIsNone(cfg.validate_devices(self.mock_uns_schema))
        self.valid_device.attach_uns.assert_called_once_with(self.mock_uns_schema)

    def test_duplicate_uuid_raises(self):
        """
        Two devices with the same UUID should raise ValueError.
        """
        cfg = DevicesConfig(devices={
            "dev1": self.valid_device,
            "dev2": Device(**self.valid_device.model_dump())  # duplicate uuid

        })
        with self.assertRaises(ValueError) as cm:
            cfg.validate_devices(self.mock_uns_schema)
        self.assertIn("Duplicate uuid", str(cm.exception))

    def test_uns_validation_failure_raises(self):
        """
        If attach_uns raises an exception, ValueError should be raised.
        """
        failing_device = Device(**self.valid_device.model_dump())
        with patch.object(Device, "attach_uns", side_effect=Exception("UNS fail")):
            cfg = DevicesConfig(devices={"dev1": failing_device})
            with self.assertRaises(ValueError) as cm:
                cfg.validate_devices(self.mock_uns_schema)
            self.assertIn("UNS validation failed", str(cm.exception))

    def test_devices_dict_property(self):
        """
        devices_dict should return a plain serializable dictionary.
        """
        cfg = DevicesConfig(devices={"dev1": self.valid_device})
        out = cfg.devices_dict
        self.assertIsInstance(out, dict)
        self.assertIn("dev1", out)
        self.assertEqual(out["dev1"]["uuid"], "uuid1")
