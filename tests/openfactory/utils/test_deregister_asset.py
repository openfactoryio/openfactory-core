import unittest
import json
from unittest.mock import patch, MagicMock
from openfactory.utils import deregister_asset
from openfactory.assets import AssetAttribute


class TestDeregisterAsset(unittest.TestCase):
    """
    Test deregister_asset
    """

    @patch("openfactory.utils.assets.AssetProducer")
    def test_deregister_asset_calls_producer_correctly(self, MockAssetProducer):
        """ Test deregister_asset logic """

        mock_producer_instance = MagicMock()
        MockAssetProducer.return_value = mock_producer_instance

        # Inputs
        asset_uuid = "5678-EFGH"
        bootstrap_servers = "kafka-broker:9092"

        mock_ksql_client = MagicMock()
        mock_ksql_client.get_kafka_topic.side_effect = (
            lambda table: f"topic_for_{table}"
        )

        # Mock polling loop response
        mock_ksql_client.query.return_value = [
            {"AVAILABILITY": "REMOVED"}
        ]

        # Call function
        deregister_asset(asset_uuid, mock_ksql_client, bootstrap_servers)

        # Check producer instantiation
        MockAssetProducer.assert_called_once_with(
            mock_ksql_client,
            bootstrap_servers
        )

        # Check send_asset_attribute calls
        calls = mock_producer_instance.send_asset_attribute.call_args_list

        # 1 REMOVED + 2 reference removals
        self.assertEqual(len(calls), 3)

        # First call: REMOVED event
        self.assertEqual(calls[0][0][0], asset_uuid)
        self.assertIsInstance(calls[0][0][1], AssetAttribute)
        self.assertEqual(calls[0][0][1].id, "avail")
        self.assertEqual(calls[0][0][1].value, "REMOVED")
        self.assertEqual(calls[0][0][1].type, "Events")
        self.assertEqual(calls[0][0][1].tag, "Availability")

        # Verify polling query happened
        mock_ksql_client.query.assert_called_once_with(
            f"SELECT AVAILABILITY FROM assets_avail "
            f"WHERE ASSET_UUID='{asset_uuid}';"
        )

        # Reference cleanup messages
        expected_ids = ["references_below", "references_above"]
        for i, ref_id in enumerate(expected_ids, start=1):
            call = calls[i]
            self.assertEqual(call[0][0], asset_uuid)
            self.assertIsInstance(call[0][1], AssetAttribute)
            self.assertEqual(call[0][1].id, ref_id)
            self.assertEqual(call[0][1].value, "")
            self.assertEqual(call[0][1].type, "OpenFactory")
            self.assertEqual(call[0][1].tag, "AssetsReferences")

        # Check produce tombstone messages
        expected_topics = [
            "assets_type",
            "assets_avail",
            "docker_services",
        ]

        for topic in expected_topics:
            mock_ksql_client.get_kafka_topic.assert_any_call(topic)
            mock_producer_instance.produce.assert_any_call(
                topic=f"topic_for_{topic}",
                key=asset_uuid,
                value=None,
            )

        # Ensure flush was called
        self.assertEqual(mock_producer_instance.flush.call_count, 2)

    @patch("openfactory.utils.assets.AssetProducer")
    @patch("openfactory.utils.assets.now_iso_to_epoch_millis")
    def test_deregister_asset_removes_uns_map(self, mock_now, MockAssetProducer):
        """ Test that UNS map tombstone message is produced correctly """
        mock_producer_instance = MagicMock()
        MockAssetProducer.return_value = mock_producer_instance

        # Mock timestamp
        mock_now.return_value = 1720000000000

        asset_uuid = "mocked-uuid"
        expected_topic = "mocked_uns_map_topic"

        mock_ksql_client = MagicMock()
        mock_ksql_client.get_kafka_topic.return_value = expected_topic

        # Mock polling loop response
        mock_ksql_client.query.return_value = [
            {"AVAILABILITY": "REMOVED"}
        ]

        # Call function
        deregister_asset(asset_uuid, bootstrap_servers="kafka-broker:9092", ksqlClient=mock_ksql_client)

        # Assert get_kafka_topic was called correctly
        mock_ksql_client.get_kafka_topic.assert_any_call("asset_to_uns_map_raw")

        # Verify UNS cleanup message
        mock_producer_instance.produce.assert_any_call(
            topic=expected_topic,
            key=asset_uuid,
            value=json.dumps({
                "asset_uuid": asset_uuid,
                "uns_id": None,
                "uns_levels": None,
                "updated_at": 1720000000000
            })
        )

    @patch("openfactory.utils.assets.AssetProducer")
    def test_deregister_asset_waits_until_removed_state(self, MockAssetProducer):
        """ Should keep polling until asset availability becomes REMOVED. """

        mock_producer_instance = MagicMock()
        MockAssetProducer.return_value = mock_producer_instance

        asset_uuid = "5678-EFGH"
        bootstrap_servers = "kafka-broker:9092"

        mock_ksql_client = MagicMock()
        mock_ksql_client.query.side_effect = [
            [],
            [],
            [{"AVAILABILITY": "REMOVED"}]
        ]

        deregister_asset(
            asset_uuid,
            mock_ksql_client,
            bootstrap_servers
        )

        self.assertEqual(mock_ksql_client.query.call_count, 3)
