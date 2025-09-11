from unittest import TestCase
from unittest.mock import patch, MagicMock
from openfactory import OpenFactory


class TestOpenFactory(TestCase):
    """
    Test class OpenFactory
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

    def test_init_success(self):
        """ Test OpenFactory initialization when KSQL connection succeeds """
        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        self.assertEqual(ofa.ksql, self.ksql_mock)

    def test_assets_uuid(self):
        """ Test assets_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.assets_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with("SELECT ASSET_UUID FROM assets_type;")

    def test_assets_empty(self):
        """ Test assets() when no assets exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        assets = ofa.assets()

        # Expect an empty list
        assert assets == []

    @patch("openfactory.openfactory.Asset")
    def test_assets(self, MockAsset):
        """ Test assets() """
        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.assets_uuid = MagicMock()
        ofa.assets_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.assets()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_assets_docker_services(self):
        """ Test assets_docker_services() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"service_id": 1, "status": "running"},
            {"service_id": 2, "status": "stopped"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.assets_docker_services()

        # Ensure the function returns the expected list of dicts
        expected_result = [
            {"service_id": 1, "status": "running"},
            {"service_id": 2, "status": "stopped"}
        ]
        assert result == expected_result

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with("SELECT * FROM docker_services;")

    def test_devices_uuid(self):
        """ Test devices_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.devices_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with(
            "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'Device';"
        )

    @patch("openfactory.openfactory.Asset")
    def test_devices(self, MockAsset):
        """ Test devices() """

        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.devices_uuid = MagicMock()
        ofa.devices_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.devices()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_devices_empty(self):
        """ Test devices() when no devices exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        devices = ofa.devices()

        # Expect an empty list
        assert devices == []

    def test_agents_uuid(self):
        """ Test agents_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.agents_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with(
            "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'MTConnectAgent';"
        )

    @patch("openfactory.openfactory.Asset")
    def test_agents(self, MockAsset):
        """ Test agents() """

        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.agents_uuid = MagicMock()
        ofa.agents_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.agents()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_agents_empty(self):
        """ Test agents() when no agents exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        agents = ofa.agents()

        # Expect an empty list
        assert agents == []

    def test_producers_uuid(self):
        """ Test producers_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.producers_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with(
            "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'KafkaProducer';"
        )

    @patch("openfactory.openfactory.Asset")
    def test_producers(self, MockAsset):
        """ Test producers() """

        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.producers_uuid = MagicMock()
        ofa.producers_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.producers()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_producers_empty(self):
        """ Test producers() when no producers exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        producers = ofa.producers()

        # Expect an empty list
        assert producers == []

    def test_supervisors_uuid(self):
        """ Test supervisors_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.supervisors_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with(
            "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'Supervisor';"
        )

    @patch("openfactory.openfactory.Asset")
    def test_supervisors(self, MockAsset):
        """ Test supervisors() """

        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.supervisors_uuid = MagicMock()
        ofa.supervisors_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.supervisors()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_supervisors_empty(self):
        """ Test supervisors() when no supervisors exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        supervisors = ofa.supervisors()

        # Expect an empty list
        assert supervisors == []

    def test_applications_uuid(self):
        """ Test applications_uuid() """
        # Mock query result as list of dicts
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = [
            {"ASSET_UUID": "uuid1"},
            {"ASSET_UUID": "uuid2"}
        ]

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        result = ofa.applications_uuid()

        # Ensure the function returns the expected result
        assert result == ["uuid1", "uuid2"]

        # Verify the correct query was executed
        mock_ksql.query.assert_called_once_with(
            "SELECT ASSET_UUID FROM assets_type WHERE TYPE = 'OpenFactoryApp';"
        )

    @patch("openfactory.openfactory.Asset")
    def test_applications(self, MockAsset):
        """ Test applications() """

        # Mock Asset instances
        mock_asset_instances = [MagicMock(), MagicMock()]
        MockAsset.side_effect = mock_asset_instances

        ofa = OpenFactory(ksqlClient=self.ksql_mock, bootstrap_servers="MockedBroker")
        ofa.applications_uuid = MagicMock()
        ofa.applications_uuid.return_value = ["asset-001", "asset-002"]

        result = ofa.applications()

        # Assert that Asset was called with the correct arguments
        MockAsset.assert_any_call("asset-001", self.ksql_mock, "MockedBroker")
        MockAsset.assert_any_call("asset-002", self.ksql_mock, "MockedBroker")

        # Assert that the return value matches the mock objects
        self.assertEqual(result, mock_asset_instances)

    def test_applications_empty(self):
        """ Test applications() when no applications exist """
        mock_ksql = MagicMock()
        mock_ksql.query.return_value = []  # Simulate empty result

        ofa = OpenFactory(ksqlClient=mock_ksql, bootstrap_servers="MockedBroker")
        applications = ofa.applications()

        # Expect an empty list
        assert applications == []
