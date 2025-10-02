""" Methods related to time operations. """

from datetime import datetime, timezone


def openfactory_timestamp(timestamp: datetime) -> str:
    """
    Convert a datetime to OpenFactory timestamp format.

    The format is ISO 8601 with milliseconds precision and a 'Z' to indicate UTC time,
    e.g., '2025-05-04T12:34:56.789Z'.

    Args:
        timestamp (datetime.datetime): A datetime object (UTC recommended).

    Returns:
        str: Timestamp formatted in OpenFactory style.
    """
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def current_timestamp() -> str:
    """
    Returns the current timestamp in OpenFactory format.

    The format is ISO 8601 with milliseconds precision and a 'Z' to indicate UTC time,
    e.g., '2025-05-04T12:34:56.789Z'.

    Returns:
        str: The current UTC timestamp formatted in OpenFactory style.
    """
    return openfactory_timestamp(datetime.now(timezone.utc))
