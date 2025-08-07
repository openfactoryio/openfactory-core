import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.mtconnect import Adapter


class TestAdapter(unittest.TestCase):
    """
    Unit tests for MTConnect Adapter
    """

    def test_valid_adapter_with_ip(self):
        """ Test valid Adapter with IP specified. """
        data = {
            "ip": "10.0.0.1",
            "port": 7878,
        }
        mod = Adapter(**data)
        self.assertEqual(mod.ip, "10.0.0.1")
        self.assertEqual(mod.port, 7878)
        self.assertIsNone(mod.image)
        self.assertIsNone(mod.environment)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 1)

    def test_valid_adapter_with_image(self):
        """ Test valid Adapter with image specified. """
        data = {
            "image": "adapter_image",
            "port": 7878,
        }
        mod = Adapter(**data)
        self.assertEqual(mod.image, "adapter_image")
        self.assertEqual(mod.port, 7878)
        self.assertIsNone(mod.ip)
        self.assertIsNone(mod.environment)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 1)

    def test_adapter_with_deploy(self):
        """ Test Adapter with deploy configuration. """
        data = {
            "ip": "10.0.0.1",
            "port": 7878,
            "deploy": {
                "replicas": 2
            }
        }
        mod = Adapter(**data)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 2)

    def test_extra_fields_forbid_raises_validation_error(self):
        """ Providing undefined fields for an app should raise ValidationError. """
        invalid_config = {
            "ip": "10.0.0.1",
            "port": 7878,
            "foo": "bar",
        }

        with self.assertRaises(ValidationError) as cm:
            Adapter(**invalid_config)

        # Basic sanity checks: the error mentions the offending field
        err_str = str(cm.exception)
        self.assertIn("foo", err_str)
        self.assertIn("extra", err_str.lower())

    def test_adapter_ip_and_image_defined_raises(self):
        """ Test error when both IP and image are specified. """
        data = {
            "ip": "10.0.0.1",
            "image": "adapter_image",
            "port": 7878,
        }
        with self.assertRaises(ValueError) as context:
            Adapter(**data)
        self.assertIn("Either 'ip' or 'image' must be specified in the adapter.", str(context.exception))

    def test_adapter_environment(self):
        """ Test Adapter with environment variables. """
        data = {
            "image": "adapter_image",
            "port": 7878,
            "environment": ['var1=toto', 'var2=titi']
        }

        mod = Adapter(**data)

        # Check no exception is raised and the model is correctly populated
        self.assertEqual(mod.image, "adapter_image")
        self.assertEqual(mod.port, 7878)
        self.assertEqual(mod.environment, ['var1=toto', 'var2=titi'])
        self.assertIsNone(mod.ip)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 1)

    def test_adapter_deploy_defaults_to_replicas_1_if_missing(self):
        """ Test that deploy is automatically created with replicas=1 if missing. """
        data = {
            "ip": "10.0.0.1",
            "port": 7878,
        }
        mod = Adapter(**data)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 1)

    def test_adapter_deploy_replicas_defaults_to_1_if_none(self):
        """ Test that deploy.replicas defaults to 1 if set to None. """
        data = {
            "ip": "10.0.0.1",
            "port": 7878,
            "deploy": {
                "replicas": None
            }
        }
        mod = Adapter(**data)
        self.assertIsNotNone(mod.deploy)
        self.assertEqual(mod.deploy.replicas, 1)
