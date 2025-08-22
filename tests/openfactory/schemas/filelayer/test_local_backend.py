import unittest
from unittest.mock import patch
from openfactory.schemas.filelayer.local_backend import LocalBackendConfig


class TestLocalBackendConfig(unittest.TestCase):
    """
    Unit tests for class LocalBackendConfig
    """

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    def test_valid_config(self, mock_isdir, mock_exists):
        """ Test a valid configuration """
        config = LocalBackendConfig(
            type="local",
            local_path="/tmp/host_dir",
            mount_point="/mnt/container_dir"
        )
        self.assertEqual(config.local_path, "/tmp/host_dir")
        self.assertEqual(config.mount_point, "/mnt/container_dir")
        self.assertEqual(config.type, "local")

    @patch("os.path.exists", return_value=False)
    def test_invalid_path_not_exists(self, mock_exists):
        """ Test that a non-existent host path raises a ValueError """
        with self.assertRaises(ValueError) as cm:
            LocalBackendConfig(local_path="/nonexistent", mount_point="/mnt/data")
        self.assertIn("does not exist", str(cm.exception))

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=False)
    def test_invalid_path_not_directory(self, mock_isdir, mock_exists):
        """ Test that a host path that exists but is not a directory raises a ValueError. """
        with self.assertRaises(ValueError) as cm:
            LocalBackendConfig(local_path="/tmp/file.txt", mount_point="/mnt/data")
        self.assertIn("must be a directory", str(cm.exception))

    def test_invalid_mount_point_relative(self):
        """ Test that a relative container mount point raises a ValueError. """
        with self.assertRaises(ValueError) as cm:
            LocalBackendConfig(path="/tmp/host", mount_point="mnt/data")
        self.assertIn("must be an absolute path", str(cm.exception))

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    def test_invalid_mount_point_dotdots(self, mock_isdir, mock_exists):
        """ Test that a mount point containing '..' raises ValueError. """
        with self.assertRaises(ValueError) as cm:
            LocalBackendConfig(
                type="local",
                path="/tmp/host",
                mount_point="/mnt/../data"
            )
        self.assertIn("must not contain '..'", str(cm.exception))

    def test_invalid_mount_point_chars(self):
        """ Test that a mount point with invalid characters raises a ValueError. """
        with self.assertRaises(ValueError) as cm:
            LocalBackendConfig(path="/tmp/host", mount_point="/mnt/data$#@")
        self.assertIn("contains invalid characters", str(cm.exception))

    @patch("os.path.exists", return_value=True)
    @patch("os.path.isdir", return_value=True)
    def test_create_backend_instance(self, mock_isdir, mock_exists):
        """ Test that create_backend_instance returns a LocalBackend instance. """
        with patch("openfactory.filelayer.local_backend.LocalBackend") as mock_backend:
            config = LocalBackendConfig(type="local",
                                        local_path="/tmp/host",
                                        mount_point="/mnt/data")
            instance = config.create_backend_instance()
            mock_backend.assert_called_once_with(config)
            self.assertEqual(instance, mock_backend.return_value)
