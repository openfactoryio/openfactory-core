""" ofa setup-kafka command. """

import requests
import time
import itertools
import sys
import threading
from importlib import resources
from openfactory.models.user_notifications import user_notify

ksql_package = "openfactory.resources.ksql"

sql_files = sorted([
    file.name
    for file in resources.files(ksql_package).iterdir()
    if file.name.endswith(".sql")
])


YELLOW = "\033[33m"
GREEN = "\033[32m"
RED = "\033[31m"
RESET = "\033[0m"


def spinner(message: str, stop_event: threading.Event):
    """
    Displays a spinner animation in the console while a process runs.

    This function continuously updates the console with a spinning character
    and a message until the provided threading.Event is set. Useful for
    indicating that a background task is in progress.

    Args:
        message (str): The message to display next to the spinner.
        stop_event (threading.Event): An event that, when set, stops the spinner.

    Example:
        ```python
        import threading
        import time

        stop_event = threading.Event()

        # Start spinner in a separate thread
        threading.Thread(target=spinner, args=("Loading...", stop_event)).start()

        # Simulate a long-running task
        time.sleep(5)
        stop_event.set()  # Stop the spinner
        ```
    """
    frames = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    for ch in itertools.cycle(frames):
        if stop_event.is_set():
            break
        sys.stdout.write(f"\r {YELLOW}{ch}{RESET} {message}")
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write("\r")
    sys.stdout.flush()


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
    user_notify.info("Applying SQL scripts to ksqlDB:")
    for filename in sql_files:
        message = f"Applying {filename} to {ksqldb_server}..."
        stop = threading.Event()
        t = threading.Thread(target=spinner, args=(message, stop))
        t.start()
        try:
            with resources.path(ksql_package, filename) as path:
                statements = path.read_text()

            response = requests.post(
                f"{ksqldb_server}/ksql",
                headers={"Content-Type": "application/vnd.ksql.v1+json"},
                json={"ksql": statements, "streamsProperties": {}},
                timeout=20,
            )
            response.raise_for_status()
            time.sleep(5)

            stop.set()
            t.join()
            print(f"\r {GREEN}✔{RESET} {message} done")

        except requests.HTTPError as e:
            user_notify.fail(f"Failed to apply {filename}: {e}")
        except Exception as e:
            user_notify.fail(f"Unexpected error with {filename}: {e}")
