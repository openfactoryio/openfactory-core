import unittest
from pydantic import ValidationError
from openfactory.schemas.filelayer.nfs_backend import NFSBackendConfig


class TestNFSBackendConfig(unittest.TestCase):
    """
    Tests for schema NFSBackendConfig
    """

    def test_valid_config(self):
        """ Test a valid configuration """
        cfg = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point="/mnt/nfs",
            mount_options=["rw", "vers=4.1", " noatime "]
        )
        self.assertEqual(cfg.server, "192.168.1.10")
        self.assertEqual(cfg.remote_path, "/exports/data")
        self.assertEqual(cfg.mount_point, "/mnt/nfs")
        self.assertEqual(cfg.mount_options, ["rw", "vers=4.1", "noatime"])

    def test_valid_ipv6_server(self):
        """ Test that a valid IPv6 server in brackets is accepted """
        cfg = NFSBackendConfig(
            type="nfs",
            server="[2001:db8::1]",
            remote_path="/exports/data",
            mount_point="/mnt/nfs"
        )
        self.assertEqual(cfg.server, "[2001:db8::1]")

    def test_invalid_server_empty(self):
        """ Test that an empty server field raises a ValidationError. """
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="",
                remote_path="/exports/data",
                mount_point="/mnt/nfs"
            )
        errors = ctx.exception.errors()
        # There should be exactly one error for the "server" field
        self.assertEqual(errors[0]["loc"], ("server",))
        self.assertIn("server cannot be empty", errors[0]["msg"])

    def test_invalid_server_bad_hostname(self):
        """ Test that an invalid hostname raises a ValidationError. """
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="invalid_hostname!",
                remote_path="/exports/data",
                mount_point="/mnt/nfs"
            )
        errors = ctx.exception.errors()
        # Ensure the error is from the "server" field
        self.assertEqual(errors[0]["loc"], ("server",))
        self.assertIn("server must be a valid hostname, IPv4, or IPv6 address", errors[0]["msg"])

    def test_invalid_remote_path(self):
        """ Test that a relative remote_path raises a ValidationError. """
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="relative/path",
                mount_point="/mnt/nfs"
            )
        errors = ctx.exception.errors()
        # Ensure the error is from the "remote_path" field
        self.assertEqual(errors[0]["loc"], ("remote_path",))
        self.assertIn("remote_path must be an absolute path", errors[0]["msg"])

    def test_invalid_mount_point_relative(self):
        """ Test that a relative mount_point raises a ValidationError. """
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="/exports/data",
                mount_point="relative/path"
            )
        errors = ctx.exception.errors()
        self.assertEqual(errors[0]["loc"], ("mount_point",))
        self.assertIn("mount_point must be an absolute path", errors[0]["msg"])

    def test_invalid_mount_point_invalid_chars(self):
        """ Test that a mount_point with invalid characters raises a ValidationError. """
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="/exports/data",
                mount_point="/mnt/nfs$"
            )
        errors = ctx.exception.errors()
        self.assertEqual(errors[0]["loc"], ("mount_point",))
        self.assertIn("mount_point contains invalid characters", errors[0]["msg"])

    def test_invalid_mount_point_dotdots(self):
        """ Test that a mount point containing '..' raises ValueError. """
        with self.assertRaises(ValueError) as cm:
            NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="/tmp/host",
                mount_point="/mnt/../data"
            )
        self.assertIn("must not contain '..'", str(cm.exception))

    def test_invalid_mount_point_too_long(self):
        """ Test that a mount_point longer than 255 characters raises a ValidationError. """
        long_path = "/" + "a" * 256
        with self.assertRaises(ValidationError) as ctx:
            NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="/exports/data",
                mount_point=long_path
            )
        errors = ctx.exception.errors()
        self.assertEqual(errors[0]["loc"], ("mount_point",))
        self.assertIn("mount_point is too long", errors[0]["msg"])

    def test_mount_options_cleaning(self):
        """ Test that mount_options are cleaned of empty or whitespace-only entries. """
        cfg = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point="/mnt/nfs",
            mount_options=["rw", " ", "noatime"]
        )
        self.assertEqual(cfg.mount_options, ["rw", "noatime"])
