import sys
import os
import unittest
from unittest.mock import patch
from importlib import import_module


class TestOpenFactoryInit(unittest.TestCase):
    """
    Unit tests for openfactory/__init__.py
    """
    def setUp(self):
        sys.modules.pop("openfactory", None)

    def test_load_dotenv_called(self):
        """ Check that load_dotenv('.ofaenv') is called on import. """
        with patch("dotenv.load_dotenv") as mock_load_dotenv:
            import_module("openfactory")
            mock_load_dotenv.assert_called_once_with('.ofaenv')

    def test_openfactory_version_logic(self):
        """ Check that OPENFACTORY_VERSION is 'latest' in dev and real version otherwise. """
        # Test DEV environment
        with patch.dict(os.environ, {"OPENFACTORY_ENV": "dev"}, clear=False), \
             patch("dotenv.load_dotenv"):
            import_module("openfactory")
            self.assertEqual(os.environ["OPENFACTORY_VERSION"], "latest")
            sys.modules.pop("openfactory")

        # Test normal environment
        with patch.dict(os.environ, {}, clear=True), \
             patch("dotenv.load_dotenv"), \
             patch("importlib.metadata.version", return_value="1.2.3"):
            import_module("openfactory")
            self.assertEqual(os.environ["OPENFACTORY_VERSION"], "v1.2.3")
