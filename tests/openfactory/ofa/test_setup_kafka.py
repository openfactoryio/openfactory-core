import os
import unittest
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError
from openfactory.ofa.ksql_setup import setup_kafka


@patch("openfactory.ofa.ksql_setup.threading.Thread")
class TestSetupKafka(unittest.TestCase):
    """
    Unit tests for the `setup_kafka` function.
    """

    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.requests.post")
    @patch("openfactory.ofa.ksql_setup.sql_files", new=[
        "001-mock.sql",
        "002-mock.sql",
        "003-mock.sql"
    ])
    def test_successful_application(self, mock_requests_post, mock_resources_path, mock_sleep, mock_thread):
        """ Test that all SQL scripts are applied successfully. """

        # --- Mock spinner threads so they do not actually run ---
        mock_thread.return_value.start.return_value = None
        mock_thread.return_value.join.return_value = None

        # --- Mock resources.path context manager ---
        mock_file = MagicMock()
        mock_file.read_text.return_value = "SELECT 1;"
        mock_resources_path.return_value.__enter__.return_value = mock_file

        # --- Mock requests.post to simulate successful SQL application ---
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [{"commandStatus": {"status": "SUCCESS", "message": "ok"}}]
        mock_requests_post.return_value = mock_response

        # --- Run the function ---
        setup_kafka("http://fake-ksqldb:8088")

        # --- Assertions ---

        # --- Check that each SQL file was read ---
        expected_calls = [
            ("openfactory.resources.ksql", "001-mock.sql"),
            ("openfactory.resources.ksql", "002-mock.sql"),
            ("openfactory.resources.ksql", "003-mock.sql"),
        ]

        actual_calls = [call.args for call in mock_resources_path.call_args_list]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual)

        # Check that requests.post was called for each SQL file
        self.assertEqual(mock_requests_post.call_count, 3)
        for call in mock_requests_post.call_args_list:
            self.assertIn("json", call.kwargs)
            self.assertIn("ksql", call.kwargs["json"])
            self.assertEqual(call.kwargs["json"]["ksql"], "SELECT 1;")

        # Check that spinner threads were started and joined
        self.assertEqual(mock_thread.return_value.start.call_count, 3)
        self.assertEqual(mock_thread.return_value.join.call_count, 3)

        # Check that time.sleep was called
        self.assertEqual(mock_sleep.call_count, 3)

    @patch("openfactory.ofa.ksql_setup.sql_files", new=["mtcdevices.sql"])
    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.requests.post")
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.user_notify")
    def test_http_error_handling(self, mock_notify, mock_resources_path, mock_requests_post, mock_sleep, mock_thread):
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
    def test_file_read_error_handling(self, mock_notify, mock_resources_path, mock_sleep, mock_thread):
        """ Test unexpected file read error is caught. """
        # Simulate a file read error when entering the context manager
        mock_resources_path.return_value.__enter__.side_effect = Exception("File not found")

        setup_kafka("http://fake-ksqldb:8088")

        mock_notify.fail.assert_any_call("Unexpected error with mtcdevices.sql: File not found")

    @patch("openfactory.ofa.ksql_setup.time.sleep", return_value=None)
    @patch("openfactory.ofa.ksql_setup.resources.path")
    @patch("openfactory.ofa.ksql_setup.requests.post")
    @patch("openfactory.ofa.ksql_setup.sql_files", new=["001-env.sql"])
    def test_env_var_expansion_in_sql(
        self, mock_requests_post, mock_resources_path, mock_sleep, mock_thread
    ):
        """ Test that environment variables in SQL scripts are expanded before submission. """

        # --- Isolate environment ---
        original_env = os.environ.copy()
        os.environ.clear()

        try:
            os.environ["TOPIC_NAME"] = "assets_topic"
            os.environ["PARTITIONS"] = ""

            # --- Mock spinner thread ---
            mock_thread.return_value.start.return_value = None
            mock_thread.return_value.join.return_value = None

            # --- SQL with env vars (compose-style) ---
            raw_sql = """
            CREATE STREAM test_stream (
                id VARCHAR
            ) WITH (
                KAFKA_TOPIC = '${TOPIC_NAME:-default_topic}',
                PARTITIONS = ${PARTITIONS:-3}
            );
            """

            expected_sql = """
            CREATE STREAM test_stream (
                id VARCHAR
            ) WITH (
                KAFKA_TOPIC = 'assets_topic',
                PARTITIONS = 3
            );
            """

            mock_file = MagicMock()
            mock_file.read_text.return_value = raw_sql
            mock_resources_path.return_value.__enter__.return_value = mock_file

            # --- Mock successful request ---
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_requests_post.return_value = mock_response

            # --- Run ---
            setup_kafka("http://fake-ksqldb:8088")

            # --- Assert SQL was expanded ---
            sent_sql = mock_requests_post.call_args.kwargs["json"]["ksql"]

            self.assertEqual(
                sent_sql.strip(),
                expected_sql.strip()
            )

        finally:
            # --- Restore environment ---
            os.environ.clear()
            os.environ.update(original_env)
