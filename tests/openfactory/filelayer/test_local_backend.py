import unittest
import tempfile
import shutil
from pathlib import Path
from openfactory.schemas.filelayer.local_backend import LocalBackendConfig
from openfactory.filelayer.local_backend import LocalBackend


class TestLocalBackend(unittest.TestCase):
    """
    Unit tests for LocalBackend class.
    """

    def setUp(self):
        """ Create a temporary directory for testing. """
        self.temp_dir = tempfile.mkdtemp()
        self.config = LocalBackendConfig(
            type="local",
            local_path=self.temp_dir,
            mount_point="/mnt/data"
        )
        self.backend = LocalBackend(self.config)

    def tearDown(self):
        """ Remove the temporary directory after tests. """
        shutil.rmtree(self.temp_dir)

    def test_full_path_relative(self):
        """ Test that _full_path correctly computes relative paths. """
        rel_path = "subdir/file.txt"
        full_path = self.backend._full_path(rel_path)
        expected = Path(self.temp_dir) / rel_path
        self.assertEqual(full_path, expected)

    def test_full_path_absolute_inside_root(self):
        """ Test that _full_path accepts absolute paths inside root. """
        file_path = Path(self.temp_dir) / "file.txt"
        full_path = self.backend._full_path(str(file_path))
        self.assertEqual(full_path, file_path)

    def test_full_path_absolute_outside_root_raises(self):
        """ Test that _full_path rejects absolute paths outside root. """
        outside_path = "/tmp/other/file.txt"
        with self.assertRaises(ValueError):
            self.backend._full_path(outside_path)

    def test_open_write_and_read(self):
        """ Test that open can write to and read from a file. """
        file_path = "test.txt"
        content = "hello world"

        # Write
        with self.backend.open(file_path, "w") as f:
            f.write(content)

        # Read
        with self.backend.open(file_path, "r") as f:
            read_content = f.read()

        self.assertEqual(read_content, content)

    def test_exists(self):
        """ Test exists method for existing and non-existing files. """
        file_path = "file.txt"
        full_path = Path(self.temp_dir) / file_path
        full_path.write_text("data")

        self.assertTrue(self.backend.exists(file_path))
        self.assertFalse(self.backend.exists("nonexistent.txt"))

    def test_delete(self):
        """ Test that delete removes a file and raises FileNotFoundError if missing. """
        file_path = "file.txt"
        full_path = Path(self.temp_dir) / file_path
        full_path.write_text("data")

        self.backend.delete(file_path)
        self.assertFalse(full_path.exists())

        with self.assertRaises(FileNotFoundError):
            self.backend.delete(file_path)

    def test_listdir(self):
        """ Test that listdir returns all entries in a directory. """
        (Path(self.temp_dir) / "file1.txt").write_text("1")
        (Path(self.temp_dir) / "file2.txt").write_text("2")
        (Path(self.temp_dir) / "subdir").mkdir()

        entries = self.backend.listdir(".")
        self.assertCountEqual(entries, ["file1.txt", "file2.txt", "subdir"])

    def test_from_config(self):
        """ Test that from_config builds a LocalBackend instance. """
        config_dict = {"type": "local", "local_path": self.temp_dir, "mount_point": "/mnt/data"}
        backend = LocalBackend.from_config(config_dict)
        self.assertIsInstance(backend, LocalBackend)
        self.assertEqual(backend.root, Path(self.temp_dir))

    def test_get_mount_spec(self):
        """ Test that get_mount_spec returns a correct Docker bind spec. """
        spec = self.backend.get_mount_spec()
        self.assertEqual(spec["Type"], "bind")
        self.assertEqual(spec["Source"], str(Path(self.temp_dir)))
        self.assertEqual(spec["Target"], self.config.mount_point)
        self.assertFalse(spec["ReadOnly"])
