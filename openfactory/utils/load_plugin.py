"""
Plugin Loader Utilities for OpenFactory.

This module provides an utility function to dynamically load plugin classes
from entry points defined in `pyproject.toml`.

It enables flexible configuration of pluggable components such as:
  - Deployment platforms (entry point group: `openfactory.deployment_platforms`)

Plugins are selected based on the names provided via environment variables in the
application settings.

Example:
    .. code-block:: python

        from openfactory.utils import load_plugin

        strategy_cls = load_plugin("openfactory.deployment_platforms", "swarm")
        strategy = strategy_cls()

Raises:
    ValueError: If no matching plugin is found for the given group and name.
"""

from importlib.metadata import entry_points


def load_plugin(group: str, name: str):
    """
    Load a plugin class or factory function from entry points.

    Args:
        group (str): Entry point group name.
        name (str): Name of the registered plugin.

    Returns:
        The loaded plugin object (class or function).
    """
    eps = entry_points().select(group=group)
    for ep in eps:
        if ep.name == name:
            return ep.load()

    raise ValueError(f"No entry point named '{name}' found in group '{group}'")
