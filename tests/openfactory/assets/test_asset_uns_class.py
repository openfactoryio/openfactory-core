import unittest
from unittest.mock import MagicMock, patch, call
from openfactory.assets import AssetUNS
from openfactory.assets.asset_base import BaseAsset
from openfactory.kafka import KafkaAssetUNSConsumer
from openfactory.exceptions import OFAException


@patch("openfactory.assets.asset_base.AssetProducer")
class TestAssetUNS(unittest.TestCase):
    """
    Test class AssetUNS
    """

    def setUp(self):
        self.patcher_fetch_attributes = patch.object(BaseAsset, "_fetch_attributes")
        self.patcher_fetch_methods = patch.object(BaseAsset, "_fetch_methods")
        self.patcher_get_nats_cluster_url = patch(
            "openfactory.assets.asset_base.get_nats_cluster_url",
            return_value="nats://mock"
        )
        self.patcher_nats_subscriber = patch(
            "openfactory.assets.asset_base.NATSSubscriber"
        )

        self.patcher_fetch_attributes.start()
        self.patcher_fetch_methods.start()
        self.patcher_get_nats_cluster_url.start()
        self.patcher_nats_subscriber.start()

        self.addCleanup(self.patcher_fetch_attributes.stop)
        self.addCleanup(self.patcher_fetch_methods.stop)
        self.addCleanup(self.patcher_get_nats_cluster_url.stop)
        self.addCleanup(self.patcher_nats_subscriber.stop)

    def test_inherits_from_baseasset(self, MockAssetProducer):
        """ Test AssetUNS derives from BaseAsset """
        self.assertTrue(issubclass(AssetUNS, BaseAsset))

    def test_init_sets_attributes(self, MockAssetProducer):
        """ Test correct inital attributes """
        asset = AssetUNS('test_uns_001', ksqlClient=MagicMock(),
                         bootstrap_servers='mocked_broker', asset_router_url='mocked_asset_url')
        self.assertEqual(asset.KSQL_ASSET_TABLE, 'assets_uns')
        self.assertEqual(asset.KSQL_ASSET_ID, 'uns_id')
        self.assertEqual(asset.ASSET_CONSUMER_CLASS, KafkaAssetUNSConsumer)
        self.assertEqual(asset.ASSET_ID, 'test_uns_001')

    def test_asset_uuid_returns_value(self, MockAssetProducer):
        """ Test asset_uuid returns correct value """
        mock_result = [{'ASSET_UUID': 'uuid-123'}]
        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.return_value = mock_result

        asset = AssetUNS('test_uns_001', ksqlClient=mock_ksqlClient,
                         bootstrap_servers='mocked_broker', asset_router_url='mocked_asset_url')
        result = asset.asset_uuid

        expected_query = (
            "SELECT asset_uuid FROM asset_to_uns_map WHERE uns_id='test_uns_001';"
        )
        self.assertIn(call(expected_query), mock_ksqlClient.query.call_args_list)
        self.assertEqual(result, 'uuid-123')

    def test_asset_uuid_raises_when_mapping_missing(self, MockAssetProducer):
        """Test AssetUNS raises when no UUID mapping exists."""
        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.return_value = []

        with self.assertRaises(OFAException) as cm:
            AssetUNS(
                "test_uns_001",
                ksqlClient=mock_ksqlClient,
                bootstrap_servers="mocked_broker",
                asset_router_url="mocked_asset_url",
            )

        self.assertEqual(str(cm.exception), "No Asset UUID mapping found for UNS asset 'test_uns_001'.")

    def test_get_reference_list_returns_uuids(self, MockAssetProducer):
        """ Test _get_reference_list returns list of UUIDs """
        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.side_effect = [
            [{"ASSET_UUID": "uuid-123"}],        # __start_nats_consumer()
            [{"ASSET_UUID": "uuid-123"}],        # self._subscribed_asset_uuid
            [{"VALUE": "ref_001, ref_002"}],     # _get_reference_list()
        ]

        asset = AssetUNS('test_uns_001', ksqlClient=mock_ksqlClient,
                         bootstrap_servers='mocked_broker', asset_router_url='mocked_asset_url')
        result = asset._get_reference_list('above')

        self.assertEqual(result, ['ref_001', 'ref_002'])
        expected_query = "SELECT VALUE FROM assets_uns WHERE key='test_uns_001|references_above';"
        self.assertIn(call(expected_query), mock_ksqlClient.query.call_args_list)

    def test_get_reference_list_returns_empty_on_empty_df(self, MockAssetProducer):
        """ Test _get_reference_list returns empty list on empty query """
        mock_map = [{'ASSET_UUID': 'uuid-123'}]
        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.side_effect = [
            mock_map,   # __start_nats_consumer()
            mock_map,   # self._subscribed_asset_uuid
            [],         # _get_reference_list()
        ]

        asset = AssetUNS('test_uns_001', ksqlClient=mock_ksqlClient,
                         bootstrap_servers='mocked_broker', asset_router_url='mocked_asset_url')
        result = asset._get_reference_list('below')
        self.assertEqual(result, [])

    def test_get_reference_list_returns_empty_on_blank_value(self, MockAssetProducer):
        """ Test _get_reference_list returns empty list on blank VALUE """
        mock_map = [{'ASSET_UUID': 'uuid-123'}]
        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.side_effect = [
            mock_map,                # __start_nats_consumer()
            mock_map,                # self._subscribed_asset_uuid
            [{"VALUE": "   "}],      # _get_reference_list()
        ]

        asset = AssetUNS('test_uns_001', ksqlClient=mock_ksqlClient,
                         bootstrap_servers='mocked_broker', asset_router_url='mocked_asset_url')
        result = asset._get_reference_list('above')
        self.assertEqual(result, [])

    @patch("openfactory.assets.asset_uns_class.AssetUNS")
    def test_get_reference_list_returns_assets(self, MockAssetUNS, MockAssetProducer):
        """ Test _get_reference_list returns AssetUNS instances when as_assets=True. """

        mock_map = [{"ASSET_UUID": "uuid-123"}]

        mock_ksqlClient = MagicMock()
        mock_ksqlClient.query.side_effect = [
            mock_map,                             # __start_nats_consumer()
            mock_map,                             # self._subscribed_asset_uuid
            [{"VALUE": "uns_010, uns_020"}],      # _get_reference_list()
        ]

        # Setup return values for mocked AssetUNS constructor
        mock_asset_1 = MagicMock()
        mock_asset_2 = MagicMock()
        MockAssetUNS.side_effect = [mock_asset_1, mock_asset_2]

        asset = AssetUNS(
            "test_uns_001",
            ksqlClient=mock_ksqlClient,
            bootstrap_servers="mocked_broker",
            asset_router_url="mocked_asset_url",
        )

        result = asset._get_reference_list("below", as_assets=True)

        self.assertEqual(result, [mock_asset_1, mock_asset_2])
        MockAssetUNS.assert_any_call("uns_010", ksqlClient=mock_ksqlClient)
        MockAssetUNS.assert_any_call("uns_020", ksqlClient=mock_ksqlClient)
        self.assertEqual(MockAssetUNS.call_count, 2)

    @patch.object(BaseAsset, "_restart_nats_subscription")
    def test_check_asset_mapping_restarts_when_uuid_changes(self, mock_restart, MockAssetProducer):
        mock_ksql = MagicMock()
        mock_ksql.query.side_effect = [
            [{"ASSET_UUID": "uuid-123"}],   # start_nats_consumer
            [{"ASSET_UUID": "uuid-123"}],   # _subscribed_asset_uuid
            [{"ASSET_UUID": "uuid-456"}],   # _check_asset_mapping
        ]

        asset = AssetUNS(
            "test_uns_001",
            ksqlClient=mock_ksql,
            bootstrap_servers="mocked_broker",
            asset_router_url="mocked_asset_url",
            start_mapping_watcher=False,
        )

        asset._check_asset_mapping()

        mock_restart.assert_called_once()
        self.assertEqual(asset._subscribed_asset_uuid, "uuid-456")

    @patch.object(BaseAsset, "_restart_nats_subscription")
    def test_check_asset_mapping_does_nothing_when_uuid_unchanged(self, mock_restart, MockAssetProducer):
        """ No restart should occur when the UUID mapping is unchanged. """
        mock_ksql = MagicMock()
        mock_ksql.query.side_effect = [
            [{"ASSET_UUID": "uuid-123"}],   # start_nats_consumer
            [{"ASSET_UUID": "uuid-123"}],   # _subscribed_asset_uuid
            [{"ASSET_UUID": "uuid-123"}],   # _check_asset_mapping
        ]

        asset = AssetUNS(
            "test_uns_001",
            ksqlClient=mock_ksql,
            bootstrap_servers="mocked_broker",
            asset_router_url="mocked_asset_url",
            start_mapping_watcher=False,
        )

        asset._check_asset_mapping()

        mock_restart.assert_not_called()
        self.assertEqual(asset._subscribed_asset_uuid, "uuid-123")
