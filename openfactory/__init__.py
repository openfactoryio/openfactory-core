""" OpenFactory package. """
import os
from dotenv import load_dotenv
from importlib.metadata import version

# load env vars from .ofaenv
load_dotenv('.ofaenv')

# assign OPENFACTORY_VERSION
if os.environ.get("OPENFACTORY_ENV") == "dev":
    os.environ["OPENFACTORY_VERSION"] = "latest"
else:
    os.environ["OPENFACTORY_VERSION"] = f"v{version('openfactory')}"


from openfactory.openfactory import OpenFactory
from openfactory.openfactory_manager import OpenFactoryManager
from openfactory.openfactory_cluster import OpenFactoryCluster


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


try:
    __version__ = version('openfactory')
except Exception:
    __version__ = "unknown"
