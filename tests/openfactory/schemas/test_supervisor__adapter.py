import unittest
from openfactory.schemas.supervisors import SupervisorAdapter


class TestSupervisorAdapter(unittest.TestCase):
    """
    Unit tests for class SupervisorAdapter
    """

    def test_valid_ip_only(self):
        """ Test SupervisorAdapter initializes correctly with only `ip` specified. """
        data = {
            "ip": "10.0.0.1",
            "port": 8080
        }
        model = SupervisorAdapter(**data)
        self.assertEqual(model.ip, "10.0.0.1")
        self.assertIsNone(model.image)

    def test_valid_image_only(self):
        """ Test SupervisorAdapter initializes correctly with only `image` specified. """
        data = {
            "image": "adapter-image:latest",
            "port": 8080
        }
        model = SupervisorAdapter(**data)
        self.assertEqual(model.image, "adapter-image:latest")
        self.assertIsNone(model.ip)

    def test_both_ip_and_image_error(self):
        """ Test that providing both `ip` and `image` raises a ValueError. """
        with self.assertRaises(ValueError) as cm:
            SupervisorAdapter(ip="1.2.3.4", image="adapter", port=8080)
        self.assertIn("Either 'ip' or 'image' must be specified", str(cm.exception))

    def test_neither_ip_nor_image_error(self):
        """ Test that omitting both `ip` and `image` raises a ValueError. """
        with self.assertRaises(ValueError) as cm:
            SupervisorAdapter(port=8080)
        self.assertIn("Either 'ip' or 'image' must be specified", str(cm.exception))

    def test_invalid_type_input(self):
        """ Test that passing a non-dict input raises a ValidationError. """
        with self.assertRaises(TypeError):
            SupervisorAdapter.model_validate("not_a_dict")
