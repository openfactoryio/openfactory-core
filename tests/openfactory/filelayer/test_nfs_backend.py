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
