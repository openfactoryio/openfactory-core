"""
Module for managing and copying template files for OpenFactory.

This module provides a mapping of available templates (`TEMPLATE_MAP`)

Attributes:
    TEMPLATE_MAP (dict): A dictionary mapping module names to their template
        information, including the package containing the templates and
        a list of template filenames.
"""

TEMPLATE_MAP = {
    "fanoutlayer": {
        "package": "openfactory.fanoutlayer",
        "files": [
            "docker-compose.yml",
            "docker-compose.override.yml",
        ],
    },
}
