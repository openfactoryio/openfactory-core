""" OpenFactory configuration module. """

import yaml
import os
import re
from dotenv import load_dotenv
from pathlib import Path
from importlib import resources
from importlib.metadata import version
from typing import Any


def expandvars_with_defaults(s: str) -> str:
    """
    Expands environment variables in the input string.

    Replaces all occurrences of environment variables in the format `${VAR}` or `${VAR:-default}`
    within the input string `s`. If the environment variable `VAR` is set, its value is used.
    If it is not set and a default value is provided (`:-default`), the default is used.
    If no default is provided and the variable is not set, it is replaced with an empty string.

    Args:
        s (str): The input string potentially containing environment variable expressions.

    Returns:
        str: The input string with environment variables expanded to their values or defaults.
    """
    pattern = re.compile(r'\$\{([^}:\s]+)(:-([^}]*))?\}')

    def replacer(match):
        var_name = match.group(1)
        default_val = match.group(3) or ''
        return os.environ.get(var_name, default_val)

    return pattern.sub(replacer, s)


def load_yaml(yaml_file: str) -> Any:
    """
    Loads a YAML file and parses it, expanding environment variables.

    Reads a YAML file, expands any environment variables in the content,
    and loads the parsed data. It also sets the `OPENFACTORY_VERSION` environment variable 
    to the current version of the OpenFactory package and loads environment variables 
    from the `.ofaenv` file.

    Args:
        yaml_file (str): The path to the YAML file to be loaded.

    Returns:
        Any: The parsed YAML data, which can be of any structure depending on the file contents.
    """
    # Set the OPENFACTORY_VERSION env variable
    os.environ["OPENFACTORY_VERSION"] = f"v{version('openfactory')}"

    # Load env vars from file
    load_dotenv('.ofaenv')

    # Read raw YAML and expand env vars before parsing
    with open(yaml_file, 'r') as stream:
        raw_yaml = stream.read()
        expanded_yaml = expandvars_with_defaults(raw_yaml)
        return yaml.safe_load(expanded_yaml)


# assign variables
config_file = Path.joinpath(Path(__file__).resolve().parent, 'openfactory.yml')
globals().update(load_yaml(config_file))

with resources.as_file(resources.files("openfactory.resources.mtcagent").joinpath("agent.cfg")) as cfg_path:
    MTCONNECT_AGENT_CFG_FILE = str(cfg_path)
