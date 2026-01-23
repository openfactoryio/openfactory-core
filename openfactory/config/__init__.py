""" OpenFactory configuration module. """

import yaml
import os
import re
from pathlib import Path
from importlib import resources
from typing import Any


def expandvars_with_defaults(s: str) -> str:
    """
    Expands environment variables in the input string using Docker Compose style semantics.

    Replaces all occurrences of environment variable expressions in the formats:

      - ${VAR}
      - ${VAR:-default}

    Expansion rules:
      - If VAR is set to a non-empty value, that value is used.
      - If VAR is unset or set to an empty string:
          - If a default is provided using ':-', the default value is used.
          - Otherwise, the variable is replaced with an empty string.

    Args:
        s (str): The input string potentially containing environment variable expressions.

    Returns:
        str: The input string with environment variables expanded according to the rules above.
    """
    pattern = re.compile(r'\$\{([^}:\s]+)(:-([^}]*))?\}')

    def replacer(match):
        var_name = match.group(1)
        default_val = match.group(3) or ''
        value = os.environ.get(var_name)
        if value:
            return value

        return default_val

    return pattern.sub(replacer, s)


def load_yaml(yaml_file: str) -> Any:
    """
    Loads a YAML file and parses it, expanding environment variables.

    Reads a YAML file, expands any environment variables in the content,
    and loads the parsed data.

    Args:
        yaml_file (str): The path to the YAML file to be loaded.

    Returns:
        Any: The parsed YAML data, which can be of any structure depending on the file contents.
    """
    with open(yaml_file, 'r') as stream:
        raw_yaml = stream.read()
        expanded_yaml = expandvars_with_defaults(raw_yaml)
        return yaml.safe_load(expanded_yaml)


# assign variables
config_file = Path.joinpath(Path(__file__).resolve().parent, 'openfactory.yml')
globals().update(load_yaml(config_file))

with resources.as_file(resources.files("openfactory.resources.mtcagent").joinpath("agent.cfg")) as cfg_path:
    MTCONNECT_AGENT_CFG_FILE = str(cfg_path)
