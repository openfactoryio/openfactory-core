import unittest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from pathlib import Path
from openfactory.ofa.templates.copy_templates import copy_templates


class TestClickTemplatesCopyCommand(unittest.TestCase):
    """
    Unit tests for ofa.templates.copy_templates.click_copy command
    """

    @patch("openfactory.ofa.templates.copy_templates.shutil.copy")
    @patch("openfactory.ofa.templates.copy_templates.files")
    def test_click_copy_copies_files(self, mock_files, mock_copy):
        """
        Test that 'ofa templates copy fanoutlayer <dest>' copies the correct files
        """
        # Simulate Traversable resource paths returned by importlib.resources.files()
        mock_traversable = MagicMock()
        mock_traversable.__truediv__.side_effect = lambda f: Path(f"/mock/{f}")
        mock_files.return_value = mock_traversable

        runner = CliRunner()
        with runner.isolated_filesystem():
            dest_dir = Path("workdir")

            result = runner.invoke(copy_templates, ["fanoutlayer", str(dest_dir)])

            # Assertions
            self.assertEqual(result.exit_code, 0)
            self.assertIn("📄 Copied docker-compose.yml", result.output)
            self.assertIn("✅ Fanoutlayer templates exported successfully", result.output)

            # Check directory was created and shutil.copy was called
            mock_copy.assert_any_call(Path("/mock/docker-compose.yml"), dest_dir / "docker-compose.yml")
            mock_copy.assert_any_call(Path("/mock/docker-compose.override.yml"), dest_dir / "docker-compose.override.yml")

    @patch("openfactory.ofa.templates.copy_templates.shutil.copy")
    @patch("openfactory.ofa.templates.copy_templates.files")
    def test_click_copy_unknown_module(self, mock_files, mock_copy):
        """
        Test that 'ofa templates copy' fails with an unknown module
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(copy_templates, ["nonexistent", "somepath"])

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Unknown module 'nonexistent'", result.output)

    @patch("openfactory.ofa.templates.copy_templates.shutil.copy")
    @patch("openfactory.ofa.templates.copy_templates.files")
    def test_click_copy_creates_destination(self, mock_files, mock_copy):
        """
        Test that destination directory is created automatically
        """
        mock_traversable = MagicMock()
        mock_traversable.__truediv__.side_effect = lambda f: Path(f"/mock/{f}")
        mock_files.return_value = mock_traversable

        runner = CliRunner()
        with runner.isolated_filesystem():
            dest_dir = Path("auto_created_dir")

            result = runner.invoke(copy_templates, ["fanoutlayer", str(dest_dir)])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("📁 Created destination directory", result.output)
            self.assertTrue(dest_dir.exists())

    @patch("openfactory.ofa.templates.copy_templates.shutil.copy")
    @patch("openfactory.ofa.templates.copy_templates.files")
    def test_click_copy_skips_existing_files(self, mock_files, mock_copy):
        """
        Test that existing files are not overwritten and a warning is shown
        """
        # Simulate Traversable resource paths returned by importlib.resources.files()
        mock_traversable = MagicMock()
        mock_traversable.__truediv__.side_effect = lambda f: Path(f"/mock/{f}")
        mock_files.return_value = mock_traversable

        runner = CliRunner()
        with runner.isolated_filesystem():
            dest_dir = Path("existing_dir")
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Create a dummy existing file that should trigger the warning
            existing_file = dest_dir / "docker-compose.yml"
            existing_file.write_text("existing content")

            # Run command
            result = runner.invoke(copy_templates, ["fanoutlayer", str(dest_dir)])

            # Assertions
            self.assertEqual(result.exit_code, 0)
            self.assertIn("⚠️  Skipping docker-compose.yml", result.output)
            self.assertIn("✅ Fanoutlayer templates exported successfully", result.output)

            # Ensure copy was NOT called for the existing file
            copied_files = [args[0] for args, _ in mock_copy.call_args_list]
            self.assertNotIn(Path("/mock/docker-compose.yml"), copied_files)

            # But other files should still be copied
            mock_copy.assert_any_call(Path("/mock/docker-compose.override.yml"), dest_dir / "docker-compose.override.yml")
