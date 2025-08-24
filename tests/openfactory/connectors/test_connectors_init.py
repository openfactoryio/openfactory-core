import unittest
from unittest.mock import patch, MagicMock
import openfactory.connectors as connectors_init


class TestImportAllConnectors(unittest.TestCase):
    """
    Unit tests for the import_all_connectors function in openfactory.connectors.__init__
    """

    @patch("openfactory.connectors.importlib.import_module")
    @patch("openfactory.connectors.pkgutil.iter_modules")
    def test_import_all_connectors_single_module(self, mock_iter_modules, mock_import_module):
        """ Should import each module returned by pkgutil.iter_modules. """
        # Setup: one module, no subpackage
        mock_iter_modules.return_value = [(None, "mod_a", False)]

        # Make import_module return a fake package with __path__
        fake_pkg = MagicMock()
        fake_pkg.__path__ = ["fake_path"]
        mock_import_module.return_value = fake_pkg

        connectors_init.import_all_connectors("openfactory.connectors")
        mock_import_module.assert_called_with("openfactory.connectors.mod_a")

    @patch("openfactory.connectors.importlib.import_module")
    @patch("openfactory.connectors.pkgutil.iter_modules")
    def test_import_all_connectors_with_subpackage(self, mock_iter_modules, mock_import_module):
        """ Should recurse into subpackages when is_pkg=True. """
        # Setup: one module and one subpackage
        mock_iter_modules.side_effect = [
            [(None, "mod_a", False), (None, "subpkg", True)],  # top-level
            [(None, "mod_b", False)]  # inside subpackage
        ]

        # Make import_module return a fake package with __path__ for every call
        def fake_import(name):
            pkg = MagicMock()
            pkg.__path__ = ["fake_path"]
            return pkg

        mock_import_module.side_effect = fake_import

        connectors_init.import_all_connectors("openfactory.connectors")

        # import_module should be called for top-level module and subpackage module
        expected_calls = [
            unittest.mock.call("openfactory.connectors"),               # top-level package
            unittest.mock.call("openfactory.connectors.mod_a"),         # module
            unittest.mock.call("openfactory.connectors.subpkg"),        # subpackage first call
            unittest.mock.call("openfactory.connectors.subpkg"),        # subpackage again due to recursion
            unittest.mock.call("openfactory.connectors.subpkg.mod_b"),  # module inside subpackage
        ]
        mock_import_module.assert_has_calls(expected_calls, any_order=False)

    @patch("openfactory.connectors.importlib.import_module")
    @patch("openfactory.connectors.pkgutil.iter_modules")
    def test_import_all_connectors_no_modules(self, mock_iter_modules, mock_import_module):
        """ Should complete without error if pkgutil.iter_modules returns empty list. """
        mock_iter_modules.return_value = []

        # Make import_module return a fake package with __path__
        fake_pkg = MagicMock()
        fake_pkg.__path__ = ["fake_path"]
        mock_import_module.return_value = fake_pkg

        try:
            connectors_init.import_all_connectors("openfactory.connectors")
        except Exception as e:
            self.fail(f"import_all_connectors raised {e} unexpectedly")
