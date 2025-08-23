import unittest
import tempfile
import os
from openfactory.schemas.filelayer.nfs_backend import NFSBackendConfig
from openfactory.filelayer.backend import FileBackend
from openfactory.filelayer.nfs_backend import NFSBackend


class TestNFSBackend(unittest.TestCase):
    """
    Tests for NFSBackend class using a temporary directory to simulate NFS mount.
    """

    def setUp(self):
        """ Create a temporary directory to simulate the NFS mount point. """
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=["rw", "noatime"]
        )
        self.backend = NFSBackend(config=self.config)

    def tearDown(self):
        """ Clean up temporary directory. """
        self.temp_dir.cleanup()

    def test_inheritance_and_initialization(self):
        """ Test that NFSBackend inherits from FileBackend and initializes correctly. """

        self.assertIsInstance(self.backend, FileBackend)

    def test_file_creation_and_reading(self):
        """ Test that a file can be created, written, and read correctly. """
        file_path = os.path.join(self.temp_dir.name, "test.txt")
        with self.backend.open(file_path, "w") as f:
            f.write("hello world")

        with self.backend.open(file_path, "r") as f:
            content = f.read()
        self.assertEqual(content, "hello world")

    def test_file_exists(self):
        """ Test exists() method returns True for existing files. """
        file_path = os.path.join(self.temp_dir.name, "exists.txt")
        with open(file_path, "w") as f:
            f.write("x")
        self.assertTrue(self.backend.exists(file_path))

    def test_file_does_not_exist(self):
        """ Test exists() method returns False for a non-existing file. """
        relative_path = "missing.txt"  # relative to backend root
        self.assertFalse(self.backend.exists(relative_path))

    def test_path_outside_backend_root(self):
        """ Test that accessing a path outside the backend root raises ValueError. """
        outside_path = "/tmp/forbidden.txt"  # absolute path outside root
        with self.assertRaises(ValueError) as ctx:
            self.backend.exists(outside_path)
        errors = str(ctx.exception)
        self.assertIn("outside backend root", errors)

    def test_file_deletion(self):
        """ Test that delete() removes a file. """
        file_path = os.path.join(self.temp_dir.name, "delete.txt")
        with open(file_path, "w") as f:
            f.write("x")
        self.backend.delete(file_path)
        self.assertFalse(os.path.exists(file_path))

    def test_listdir(self):
        """ Test listdir() lists files correctly. """
        file_names = ["a.txt", "b.txt", "c.txt"]
        for name in file_names:
            with open(os.path.join(self.temp_dir.name, name), "w") as f:
                f.write("x")
        listed = self.backend.listdir(self.temp_dir.name)
        self.assertCountEqual(listed, file_names)

    def test_make_volume_name_without_options(self):
        """ Test deterministic volume name generation without mount options. """
        config = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=[]
        )
        backend = NFSBackend(config=config)
        vol_name = backend.make_volume_name()

        self.assertEqual(vol_name, "nfs_192_168_1_10_exports_data")

    def test_make_volume_name_with_options(self):
        """ Test deterministic volume name generation with mount options. """
        config = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=["rw", "noatime"]
        )
        backend = NFSBackend(config=config)
        vol_name1 = backend.make_volume_name()

        # Order of options must not matter
        config2 = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=["noatime", "rw"]
        )
        backend2 = NFSBackend(config=config2)
        vol_name2 = backend2.make_volume_name()

        self.assertEqual(vol_name1, vol_name2)
        self.assertTrue(vol_name1.startswith("nfs_192_168_1_10_exports_data_"))

    def test_make_volume_name_different_options(self):
        """ Test that different mount options yield different volume names. """
        config_rw = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=["rw"]
        )
        backend_rw = NFSBackend(config=config_rw)

        config_ro = NFSBackendConfig(
            type="nfs",
            server="192.168.1.10",
            remote_path="/exports/data",
            mount_point=self.temp_dir.name,
            mount_options=["ro"]
        )
        backend_ro = NFSBackend(config=config_ro)

        self.assertNotEqual(
            backend_rw.make_volume_name(),
            backend_ro.make_volume_name()
        )

    def test_make_volume_name_nested_remote_path(self):
        """ Test that nested remote paths are converted into underscores. """
        config = NFSBackendConfig(
            type="nfs",
            server="10.0.0.5",
            remote_path="/exports/data/logs/archive",
            mount_point=self.temp_dir.name,
            mount_options=[]
        )
        backend = NFSBackend(config=config)
        vol_name = backend.make_volume_name()

        # Slashes in remote_path should be replaced with underscores
        self.assertEqual(vol_name, "nfs_10_0_0_5_exports_data_logs_archive")

    def test_get_mount_spec_basic(self):
        """ Test that get_mount_spec() returns correct structure for NFS backend. """
        vol_name = self.backend.make_volume_name()
        mount_spec = self.backend.get_mount_spec()

        # Basic checks
        self.assertIsInstance(mount_spec, dict)
        self.assertEqual(mount_spec["Type"], "volume")
        self.assertEqual(mount_spec["Source"], vol_name)
        self.assertEqual(mount_spec["Target"], self.config.mount_point)

        # VolumeOptions checks
        vol_opts = mount_spec.get("VolumeOptions", {}).get("DriverConfig", {})
        self.assertEqual(vol_opts.get("Name"), "local")
        opts = vol_opts.get("Options", {})
        self.assertEqual(opts.get("type"), "nfs")
        self.assertEqual(opts.get("device"), f":{self.config.remote_path}")

        # 'o' options string includes server address
        self.assertIn(f"addr={self.config.server}", opts.get("o", ""))

    def test_get_mount_spec_with_options_affects_source_and_o(self):
        """ Test that mount_options influence the volume name and 'o' string. """
        backend_with_opts = NFSBackend(
            config=NFSBackendConfig(
                type="nfs",
                server="192.168.1.10",
                remote_path="/exports/data",
                mount_point=self.temp_dir.name,
                mount_options=["ro", "noatime"]
            )
        )

        mount_spec = backend_with_opts.get_mount_spec()
        vol_name = backend_with_opts.make_volume_name()

        # Volume name matches
        self.assertEqual(mount_spec["Source"], vol_name)

        # 'o' string contains all mount options
        o_string = mount_spec["VolumeOptions"]["DriverConfig"]["Options"]["o"]
        self.assertIn("ro", o_string)
        self.assertIn("noatime", o_string)
        self.assertIn(f"addr={backend_with_opts.config.server}", o_string)
