import unittest
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError
from openfactory.ofa.ksql_setup import setup_kafka


class TestSetupKafka(unittest.TestCase):
    """
    Unit tests for the `setup_kafka` function.
    """

    @patch("openfactory.ofa.ksql_setup.sql_files", new=["mtcdevices.sql", "ofacmds.sql", "assets_uns.sql"])
    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.requests.post")
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.user_notify")
    def test_successful_application(self, mock_notify, mock_resources_path, mock_requests_post, mock_sleep):
        """ Test all SQL scripts are applied successfully """
        # Mock resources.path context manager
        mock_file = MagicMock()
        mock_file.read_text.return_value = "SELECT 1;"
        mock_resources_path.return_value.__enter__.return_value = mock_file

        # Mock requests.post to succeed
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_requests_post.return_value = mock_response

        # Run the function under test
        setup_kafka("http://fake-ksqldb:8088")

        # Check notifications for each SQL file
        for filename in ["mtcdevices.sql", "ofacmds.sql", "assets_uns.sql"]:
            mock_notify.info.assert_any_call(f"Applying {filename} to http://fake-ksqldb:8088...")
            mock_notify.success.assert_any_call(f"{filename} applied successfully.")

    @patch("openfactory.ofa.ksql_setup.sql_files", new=["mtcdevices.sql"])
    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.requests.post")
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.user_notify")
    def test_http_error_handling(self, mock_notify, mock_resources_path, mock_requests_post, mock_sleep):
        """ Test HTTPError is handled and triggers user_notify.fail. """
        # Mock resources.path context manager
        mock_file = MagicMock()
        mock_file.read_text.return_value = "INVALID SQL;"
        mock_resources_path.return_value.__enter__.return_value = mock_file

        # Mock requests.post to raise HTTPError
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError("HTTP 400 Bad Request")
        mock_requests_post.return_value = mock_response

        # Run function
        setup_kafka("http://fake-ksqldb:8088")

        # Assert user_notify.fail called with correct message
        mock_notify.fail.assert_any_call("Failed to apply mtcdevices.sql: HTTP 400 Bad Request")

    @patch("openfactory.ofa.ksql_setup.sql_files", new=["mtcdevices.sql"])
    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.user_notify")
    def test_file_read_error_handling(self, mock_notify, mock_resources_path, mock_sleep):
        """ Test unexpected file read error is caught. """
        # Simulate a file read error when entering the context manager
        mock_resources_path.return_value.__enter__.side_effect = Exception("File not found")

        setup_kafka("http://fake-ksqldb:8088")

        mock_notify.fail.assert_any_call("Unexpected error with mtcdevices.sql: File not found")
