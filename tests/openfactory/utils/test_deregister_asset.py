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
        """Test deregister_asset logic."""

        mock_producer_instance = MagicMock()
        MockAssetProducer.return_value = mock_producer_instance

        asset_uuid = "5678-EFGH"
        bootstrap_servers = "kafka-broker:9092"

        mock_ksql_client = MagicMock()
        mock_ksql_client.get_kafka_topic.side_effect = (
            lambda table: f"topic_for_{table}"
        )

        deregister_asset(asset_uuid, mock_ksql_client, bootstrap_servers)

        MockAssetProducer.assert_called_once_with(
            mock_ksql_client,
            bootstrap_servers
        )

        calls = mock_producer_instance.send_asset_attribute.call_args_list

        self.assertEqual(len(calls), 2)

        expected_ids = ["references_below", "references_above"]

        for call, ref_id in zip(calls, expected_ids):
            self.assertEqual(call[0][0], asset_uuid)

            attr = call[0][1]
            self.assertIsInstance(attr, AssetAttribute)
            self.assertEqual(attr.id, ref_id)
            self.assertEqual(attr.value, "")
            self.assertEqual(attr.type, "OpenFactory")
            self.assertEqual(attr.tag, "AssetsReferences")

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

        mock_producer_instance.flush.assert_called_once()

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
