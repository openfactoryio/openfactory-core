import requests
import openfactory.config as config
from openfactory.exceptions import OFAConfigurationException


def get_nats_cluster_url(asset_uuid: str) -> str:
    """
    Fetch the NATS URL for a given asset UUID.

    Args:
        asset_uuid (str): The asset identifier (e.g., "toto").

    Returns:
        str: The nats_url of the asset.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
        ValueError: If the response is not valid JSON or nats_url is missing.
    """
    url = f"{config.ASSET_ROUTER_URL}/asset/{asset_uuid}"

    try:
        response = requests.get(url)
    except Exception as e:
        raise OFAConfigurationException(f'Could not connect to Asset Router. Is ASSET_ROUTER_URL={config.ASSET_ROUTER_URL} defined correctly?\n{e}')
    response.raise_for_status()

    data = response.json()
    nats_url = data.get("nats_url")

    if not nats_url:
        raise ValueError(f"'nats_url' not found in response for asset {asset_uuid}")

    return nats_url
