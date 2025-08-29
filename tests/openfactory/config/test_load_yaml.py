import os
import unittest
from unittest.mock import patch, mock_open
from openfactory.config import load_yaml


class TestLoadYaml(unittest.TestCase):
    """
    Test cases for loading YAML files with environment variable substitution
    """

    @patch("builtins.open", new_callable=mock_open, read_data="""
           key1: value1
           key2: ${ENV_VAR}
           """)
    def test_load_yaml_with_env_var(self, mock_open_file):
        """ Test loading YAML with environment variable substitution """
        os.environ["ENV_VAR"] = "env_value"

        yaml_file = "dummy_path/openfactory.yml"
        result = load_yaml(yaml_file)

        self.assertEqual(result["key1"], "value1")
        self.assertEqual(result["key2"], "env_value")

        del os.environ["ENV_VAR"]

    @patch("builtins.open", new_callable=mock_open, read_data="""
           key1: value1
           nested:
            child: null
           list:
            - item1
            - item2
           """)
    def test_load_yaml_without_env_var(self, mock_open_file):
        """ Test loading YAML without environment variable substitution """
        yaml_file = "dummy_path/openfactory.yml"
        result = load_yaml(yaml_file)

        self.assertEqual(result["key1"], "value1")
        self.assertIsNone(result["nested"]["child"])
        self.assertEqual(result["list"], ["item1", "item2"])

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_yaml_file_not_found(self, mock_open_file):
        """ Test loading YAML file that does not exist """
        yaml_file = "non_existent_file.yml"

        with self.assertRaises(FileNotFoundError):
            load_yaml(yaml_file)

    @patch("builtins.open", new_callable=mock_open, read_data="""
           key1: ${ENV_VAR:-default_value}
           key2: ${NOT_SET_VAR:-fallback}
           """)
    def test_load_yaml_with_env_vars_and_defaults(self, mock_open_file):
        """ Test loading YAML with env vars including default values """
        # Setup environment
        os.environ["ENV_VAR"] = "env_value"
        # Ensure NOT_SET_VAR is not set
        os.environ.pop("NOT_SET_VAR", None)

        yaml_file = "dummy_path/openfactory.yml"
        result = load_yaml(yaml_file)

        # ENV_VAR is set → replaced by env var value
        self.assertEqual(result["key1"], "env_value")
        # NOT_SET_VAR is not set, default used
        self.assertEqual(result["key2"], "fallback")

        # Cleanup
        del os.environ["ENV_VAR"]
