""" ofa setup-kafka command. """

import requests
import time
from importlib import resources
from openfactory.models.user_notifications import user_notify

ksql_package = "openfactory.resources.ksql"

sql_files = sorted([
    file.name
    for file in resources.files(ksql_package).iterdir()
    if file.name.endswith(".sql")
])


def setup_kafka(ksqldb_server: str) -> None:
    """
    Apply all SQL scripts in `openfactory.resources.ksql` to a ksqlDB server.

    The SQL scripts are applied in alphabetical order, which allows
    numeric prefixes in filenames to control execution order
    (e.g., `001-assets.sql`, `002-ofacmds.sql`).

    Args:
        ksqldb_server (str): The URL of the ksqlDB server, e.g., "http://localhost:8088".

    Raises:
        requests.HTTPError: If a request to the ksqlDB server returns an HTTP error.
        Exception: If an unexpected error occurs while reading or applying a SQL script.
    """
    for filename in sql_files:
        user_notify.info(f"Applying {filename} to {ksqldb_server}...")
        try:
            with resources.path(ksql_package, filename) as path:
                statements = path.read_text()

            response = requests.post(
                f"{ksqldb_server}/ksql",
                headers={"Content-Type": "application/vnd.ksql.v1+json"},
                json={"ksql": statements, "streamsProperties": {}},
                timeout=10,
            )
            response.raise_for_status()
            time.sleep(5)
            user_notify.success(f"{filename} applied successfully.")

        except requests.HTTPError as e:
            user_notify.fail(f"Failed to apply {filename}: {e}")
        except Exception as e:
            user_notify.fail(f"Unexpected error with {filename}: {e}")
