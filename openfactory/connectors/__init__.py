"""
OpenFactory Connectors Package

This package contains all connector implementations for OpenFactory devices.
It provides dynamic discovery and registration of connector modules so that
all connectors are automatically available in the global registry
(CONNECTOR_REGISTRY) when the package is imported.

Note:
    Connector registration relies on decorators that run when the module is imported.
    The dynamic loader import_all_connectors() is called automatically from __init__.py,
    so you do not need to manually import individual connector modules.

Example usage:
    .. code-block:: python

        from openfactory.connectors.registry import build_connector
        # All connectors are now registered automatically

        connector = build_connector(schema, deployment_strategy, ksql, bootstrap_servers)
"""

import importlib
import pkgutil

def import_all_connectors(package_name: str = __name__) -> None:
    """
    Recursively imports all modules in the specified package and its subpackages
    to ensure that all connectors are registered in CONNECTOR_REGISTRY.

    This is necessary because connector registration relies on decorators
    that run when the module is imported. Without importing the module,
    the connector will not be registered.

    Args:
        package_name (str): Name of the package to import modules from.
            Defaults to the current package (__name__).
    """
    package = importlib.import_module(package_name)
    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        full_name = f"{package_name}.{module_name}"
        importlib.import_module(full_name)
        if is_pkg:
            import_all_connectors(full_name)  # recurse into subpackage


# Automatically import all connectors when the package is loaded
import_all_connectors()
