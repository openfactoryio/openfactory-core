import unittest
from unittest.mock import MagicMock
from openfactory.utils.assets import register_device_connector
from openfactory.schemas.devices import Device
from openfactory.kafka.ksql import KSQLDBClient, KSQLDBClientException


def normalize(sql: str) -> str:
    """ Normalize SQL by collapsing all whitespace into single spaces. """
    return " ".join(sql.split())


class TestRegisterDeviceConnector(unittest.TestCase):
    """
    Test register_device_connector
    """

    def setUp(self):
        # Mock device
        self.device = MagicMock(spec=Device)
        self.device.uuid = "test-uuid"

        # Mock connector on the device
        self.device.connector = MagicMock()
        self.device.connector.model_dump_json.return_value = '{"config": "value"}'

        # Mock KSQLDB client
        self.ksql_client = MagicMock(spec=KSQLDBClient)

    def test_register_device_connector_success_default_table(self):
        """ Test successful registration with default table name. """
        register_device_connector(self.device, self.ksql_client)

        expected_sql = """
        INSERT INTO DEVICE_CONNECTOR_SOURCE (ASST_UUID, CONNECTOR_CONFIG)
        VALUES ('test-uuid', '{"config": "value"}');
        """

        # Verify statement_query was called once with correct SQL
        self.ksql_client.statement_query.assert_called_once()
        actual_sql = self.ksql_client.statement_query.call_args[0][0]
        self.assertEqual(normalize(actual_sql), normalize(expected_sql))

    def test_register_device_connector_success_custom_table(self):
        """ Test successful registration with a custom table name. """
        register_device_connector(self.device, self.ksql_client, table_name="MY_TABLE")
        expected_sql = """
        INSERT INTO MY_TABLE (ASST_UUID, CONNECTOR_CONFIG)
        VALUES ('test-uuid', '{"config": "value"}');
        """
        self.ksql_client.statement_query.assert_called_once()
        actual_sql = self.ksql_client.statement_query.call_args[0][0]
        self.assertEqual(normalize(actual_sql), normalize(expected_sql))

    def test_register_device_connector_raises_ksqldb_exception(self):
        """ Test that KSQLDBClientException is propagated if insert fails. """
        self.ksql_client.statement_query.side_effect = KSQLDBClientException("Insert failed")
        with self.assertRaises(KSQLDBClientException):
            register_device_connector(self.device, self.ksql_client)
