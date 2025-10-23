import unittest
from click.testing import CliRunner
from openfactory.ofa.templates.ls import ls


class TestClickTemplatesListCommand(unittest.TestCase):
    """
    Unit tests for ofa.templates.ls.ls command
    """

    def test_ls_outputs_available_templates(self):
        """
        Test that 'ofa templates list' outputs available template modules
        """
        runner = CliRunner()
        result = runner.invoke(ls)

        self.assertEqual(result.exit_code, 0)
        # Expected base output
        self.assertIn("Available template modules:", result.output)
        # Example: ensure known templates are listed
        self.assertIn("fanoutlayer", result.output)

    def test_ls_exit_code_success(self):
        """
        Ensure the command exits cleanly
        """
        runner = CliRunner()
        result = runner.invoke(ls)

        self.assertEqual(result.exit_code, 0)
        self.assertTrue(len(result.output) > 0)
