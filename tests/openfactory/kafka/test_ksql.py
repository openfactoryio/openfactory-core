import unittest
from unittest.mock import patch, MagicMock
import httpx
import json
import signal
from openfactory.kafka.ksql import KSQLDBClient, KSQLDBClientException


class TestKSQLDBClient(unittest.TestCase):
    """
    Test KSQLDBClient class
    """
    def setUp(self):
        self.ksqldb_url = "http://localhost:8088"
        self.client = KSQLDBClient(self.ksqldb_url)

    def tearDown(self):
        self.client.close()

    @patch("openfactory.kafka.ksql.configure_prefixed_logger")
    @patch("openfactory.kafka.ksql.setup_third_party_loggers")
    def test_logger_setup(self, mock_setup_loggers, mock_configure_logger):
        """ Test logger setup in KSQLDBClient """

        # Mock logger returned by configure_prefixed_logger
        mock_logger = MagicMock()
        mock_configure_logger.return_value = mock_logger

        # Mock the actual creation of the KSQLDBClient
        ksqldb_url = "http://localhost:8088"
        KSQLDBClient(ksqldb_url, loglevel="MOCK_LEVEL")

        # Assertions to ensure the logger was correctly set up
        mock_setup_loggers.assert_called_once()
        mock_configure_logger.assert_called_once_with("openfactory.ksqlDB", prefix="KSQL", level="MOCK_LEVEL")
        mock_logger.info.assert_called_once_with(f"Connected to ksqlDB at {ksqldb_url}")

    def test_custom_max_requests_per_connection(self):
        """ Test that a custom recycle threshold is stored. """

        client = KSQLDBClient(
            self.ksqldb_url,
            max_requests_per_connection=42,
        )

        self.assertEqual(client._max_requests_per_connection, 42)
        client.close()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_info_success(self, mock_request):
        """ Test info method of KSQLDBClient """

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"KsqlServerInfo": {"version": "0.28.0"}}
        mock_request.return_value = mock_response

        # Call the method
        result = self.client.info()

        # Assertions
        self.assertEqual(result["KsqlServerInfo"]["version"], "0.28.0")
        mock_request.assert_called_once_with("GET", "info")

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_get_kafka_topic_success(self, mock_request):
        """ Test get_kafka_topic method of KSQLDBClient """
        stream_name = "test_stream"

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"sourceDescription": {"topic": "test_topic"}}]
        mock_request.return_value = mock_response

        # Call the method
        result = self.client.get_kafka_topic(stream_name)

        # Assertions
        self.assertEqual(result, "test_topic")
        mock_request.assert_called_once_with(
            "POST",
            "/ksql",
            json_payload={"ksql": f"DESCRIBE {stream_name} EXTENDED;"}
        )

    @patch("httpx.Client.request")
    def test_get_kafka_topic_not_found(self, mock_request):
        """ Test get_kafka_topic method of KSQLDBClient when stream is not found """
        stream_name = "missing_stream"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{}]
        mock_request.return_value = mock_response

        with self.assertRaises(KSQLDBClientException):
            self.client.get_kafka_topic(stream_name)

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_streams_success(self, mock_request):
        """ Test streams method of KSQLDBClient """
        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"streams": [{"name": "stream1"}, {"name": "stream2"}]}
        ]
        mock_request.return_value = mock_response

        # Call method
        result = self.client.streams()

        # Assertions
        self.assertEqual(result, ["stream1", "stream2"])
        mock_request.assert_called_once_with(
            "POST",
            "/ksql",
            json_payload={
                "ksql": "SHOW STREAMS;",
                "streamsProperties": {}
            }
        )

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_tables_success(self, mock_request):
        """ Test tables method of KSQLDBClient """
        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"tables": [{"name": "table1"}, {"name": "table2"}]}
        ]
        mock_request.return_value = mock_response

        # Call method
        result = self.client.tables()

        # Assertions
        self.assertEqual(result, ["table1", "table2"])
        mock_request.assert_called_once_with(
            "POST",
            "/ksql",
            json_payload={
                "ksql": "SHOW TABLES;",
                "streamsProperties": {}
            }
        )

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_query_success(self, mock_request):
        """ Test query method of KSQLDBClient returns list of dicts """
        query = "SELECT * FROM test_table;"

        # Mock response: same as before
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.read.return_value = (
            b'{"columnNames":["id","value"]}\n[1,"a"]\n[2,"b"]\n'
        )

        # Mock context manager return
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_request.return_value = mock_context

        # Call method
        result = self.client.query(query)

        # Assert result is list of dicts
        expected_result = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"}
        ]
        self.assertEqual(result, expected_result)

        # Verify _request was called correctly
        mock_request.assert_called_once_with(
            "POST",
            "/query",
            json_payload={"ksql": query, "streamsProperties": {}},
            stream=True
        )

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_statement_query_success(self, mock_request):
        """ Test statement_query method of KSQLDBClient """
        sql = "CREATE STREAM test_stream AS SELECT * FROM source_stream;"

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        mock_request.return_value = mock_response

        # Call the method
        result = self.client.statement_query(sql)

        # Assertions
        self.assertEqual(result.status_code, 200)
        mock_request.assert_called_once_with(
            "POST",
            "/ksql",
            json_payload={"ksql": sql},
            headers={'Accept': 'application/vnd.ksql.v1+json'}
        )

    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_insert_into_stream_success(self, mock_request):
        """ Test insert_into_stream method of KSQLDBClient """
        stream_name = "test_stream"
        rows = [{"field1": "value1"}, {"field1": "value2"}]

        # Simulate streaming response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_bytes.return_value = iter([
            b'{"status":"success"}\n', b'{"status":"success"}\n'
        ])
        mock_request.return_value.__enter__.return_value = mock_response

        # Call the method
        result = self.client.insert_into_stream(stream_name, rows)

        # Construct expected request content
        expected_lines = [
            json.dumps({"target": stream_name}).encode() + b"\n"
        ] + [json.dumps(row).encode() + b"\n" for row in rows]
        expected_content = b"".join(expected_lines)

        # Assertions
        self.assertEqual(result, [{"status": "success"}, {"status": "success"}])
        mock_request.assert_called_once_with(
            'POST',
            '/inserts-stream',
            stream=True,
            headers={"Content-Type": "application/vnd.ksql.v1+json"},
            content=expected_content
        )

    @patch("httpx.Client.request")
    def test_request_retries_on_failure(self, mock_request):
        """ Test that failed requests are retried. """
        mock_request.side_effect = [httpx.RequestError("Connection failed"), MagicMock(status_code=200)]
        result = self.client.info()
        self.assertIsNotNone(result)
        self.assertEqual(mock_request.call_count, 2)

    @patch("httpx.Client.request")
    def test_request_fails_after_max_retries(self, mock_request):
        """ Test that an exception is raised after exhausting retries. """
        mock_request.side_effect = httpx.RequestError("Connection failed")
        with self.assertRaises(KSQLDBClientException):
            self.client.info()
        self.assertEqual(mock_request.call_count, self.client.max_retries)

    @patch("openfactory.kafka.ksql.atexit.register")
    @patch("openfactory.kafka.ksql.signal.signal")
    @patch("openfactory.kafka.ksql.signal.getsignal")
    def test_register_cleanup(self, mock_getsignal, mock_signal, mock_atexit_register):
        """Test _register_cleanup behavior during init"""

        # Setup mock return values for old signal handlers
        mock_getsignal.side_effect = [MagicMock(), MagicMock()]

        # IMPORTANT: new instance created after mocks are applied
        client = KSQLDBClient(self.ksqldb_url)

        # Check that atexit registered the client.close method
        mock_atexit_register.assert_called_once_with(client.close)

        # Check signal handlers were fetched and updated
        mock_getsignal.assert_any_call(signal.SIGINT)
        mock_getsignal.assert_any_call(signal.SIGTERM)
        mock_signal.assert_any_call(signal.SIGINT, client._handle_exit)
        mock_signal.assert_any_call(signal.SIGTERM, client._handle_exit)

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_info_records_completed_request(self, mock_request, mock_completed):
        """ Test that info() records completion of a successful request. """
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        self.client.info()

        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_streams_records_completed_request(self, mock_request, mock_completed):
        """ Test that streams() records completion of a successful request. """

        mock_response = MagicMock()
        mock_response.json.return_value = [{"streams": []}]
        mock_request.return_value = mock_response

        self.client.streams()

        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_tables_records_completed_request(self, mock_request, mock_completed):
        """ Test that tables() records completion of a successful request. """

        mock_response = MagicMock()
        mock_response.json.return_value = [{"tables": []}]
        mock_request.return_value = mock_response

        self.client.tables()

        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_get_kafka_topic_records_completed_request(self, mock_request, mock_completed):
        """ Test that get_kafka_topic() records completion of a successful request. """

        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"sourceDescription": {"topic": "test"}}
        ]
        mock_request.return_value = mock_response

        self.client.get_kafka_topic("STREAM")

        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_statement_query_records_completed_request(self, mock_request, mock_completed):
        """ Test that statement_query() records completion of a successful request. """
        mock_request.return_value = MagicMock()
        self.client.statement_query("SHOW STREAMS;")
        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_query_records_completed_request(self, mock_request, mock_completed):
        """ Test that query() records completion of a successful request. """

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.read.return_value = (
            b'{"columnNames":["id","value"]}\n'
            b'[1,"a"]\n'
            b'[2,"b"]\n'
        )

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_request.return_value = mock_context

        self.client.query("SELECT * FROM test_table;")

        mock_completed.assert_called_once()

    @patch("openfactory.kafka.ksql.KSQLDBClient._request_completed")
    @patch("openfactory.kafka.ksql.KSQLDBClient._request")
    def test_insert_into_stream_records_completed_request(self, mock_request, mock_completed):
        """ Test that insert_into_stream() records completion of a successful request. """

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_bytes.return_value = iter([])

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_response
        mock_request.return_value = mock_context

        self.client.insert_into_stream("STREAM", [])

        mock_completed.assert_called_once()

    def test_request_completed_recycles_client(self):
        """ Test that the HTTP client is recycled after the configured number of requests. """

        old_client = MagicMock()
        new_client = MagicMock()

        self.client._client = old_client
        self.client._create_client = MagicMock(return_value=new_client)

        self.client._request_count = (
            self.client._max_requests_per_connection - 1
        )

        self.client._request_completed()

        old_client.close.assert_called_once()
        self.client._create_client.assert_called_once()

        self.assertIs(self.client._client, new_client)
        self.assertEqual(self.client._request_count, 0)

    def test_close_closes_http_client(self):
        """ Test that close() closes the HTTP client. """

        mock_client = MagicMock()
        self.client._client = mock_client

        self.client.close()

        mock_client.close.assert_called_once()
        self.assertIsNone(self.client._client)

    def test_context_manager(self):
        """ Test context manager support. """

        client = KSQLDBClient(self.ksqldb_url)
        client.close = MagicMock()

        with client as c:
            self.assertIs(c, client)

        client.close.assert_called_once()

    @patch("openfactory.kafka.ksql.signal.getsignal")
    def test_handle_exit(self, mock_getsignal):
        """ Test graceful shutdown on termination signal. """

        self.client.close = MagicMock()
        self.client.old_sigint = None
        self.client.old_sigterm = None

        with self.assertRaises(SystemExit):
            self.client._handle_exit(signal.SIGINT, None)

        self.client.close.assert_called_once()

    def test_request_completed_before_threshold(self):
        """ Test that the HTTP client is not recycled before the threshold. """

        old_client = MagicMock()

        self.client._client = old_client
        self.client._create_client = MagicMock()

        self.client._request_count = 0

        self.client._request_completed()

        old_client.close.assert_not_called()
        self.client._create_client.assert_not_called()
        self.assertEqual(self.client._request_count, 1)
