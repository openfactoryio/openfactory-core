import unittest
from unittest.mock import patch, Mock
import requests
from openfactory.assets.utils import get_nats_cluster_url


class TestGetNATSClusterURL(unittest.TestCase):
    """
    Unit tests for get_nats_cluster_url function
    """

    @patch("openfactory.assets.utils.get_nats_cluster.requests.get")
    def test_returns_nats_url_when_valid_response(self, mock_get):
        """ Test that function returns nats_url from valid response """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"nats_url": "nats://localhost:4222"}
        mock_get.return_value = mock_response

        asset_uuid = "sensor-123"
        result = get_nats_cluster_url(asset_uuid, "http://mock-router")

        mock_get.assert_called_once_with(f"http://mock-router/asset/{asset_uuid}")
        self.assertEqual(result, "nats://localhost:4222")

    @patch("openfactory.assets.utils.get_nats_cluster.requests.get")
    def test_raises_configuration_exception_when_connection_fails(self, mock_get):
        """ Test that OFAConfigurationException is raised if connection to router fails """

        mock_get.side_effect = Exception("connection failed")

        asset_uuid = "sensor-123"

        from openfactory.exceptions import OFAConfigurationException

        with self.assertRaises(OFAConfigurationException) as ctx:
            get_nats_cluster_url(asset_uuid, "http://mock-router")

        self.assertIn("Could not connect to Asset Router", str(ctx.exception))

    @patch("openfactory.assets.utils.get_nats_cluster.requests.get")
    def test_raises_value_error_if_nats_url_missing(self, mock_get):
        """ Test that ValueError is raised when nats_url is missing """
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        asset_uuid = "sensor-123"
        with self.assertRaises(ValueError) as ctx:
            get_nats_cluster_url(asset_uuid, "http://mock-router")
        self.assertIn("'nats_url' not found", str(ctx.exception))

    @patch("openfactory.assets.utils.get_nats_cluster.requests.get")
    def test_raises_request_exception_on_http_error(self, mock_get):
        """ Test that HTTP errors are propagated """
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("HTTP error")
        mock_get.return_value = mock_response

        asset_uuid = "sensor-123"
        with self.assertRaises(requests.exceptions.RequestException):
            get_nats_cluster_url(asset_uuid, "http://mock-router")
