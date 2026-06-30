import json
import unittest
from unittest.mock import MagicMock, patch
from openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service import AssetForwarderService


class TestAssetForwarderService(unittest.TestCase):
    """
    Unit tests for AssetForwarderService helper methods.
    """

    def setUp(self):
        """ Create a lightweight service instance for testing. """
        self.service = AssetForwarderService(
            ksqlClient=MagicMock(),
            bootstrap_servers="localhost:9092",
            test_mode=True,
        )

        self.service.logger = MagicMock()
        self.service.hash_ring = MagicMock()
        self.service.nats_clusters = {}

    def test_decode_message_value_bytes(self):
        """ Test decoding a JSON payload from bytes. """
        value = self.service.decode_message_value(b'{"ID":"123","name":"asset"}')
        self.assertEqual(value["ID"], "123")
        self.assertEqual(value["name"], "asset")

    def test_decode_message_value_string(self):
        """ Test decoding a JSON payload from string. """
        value = self.service.decode_message_value('{"ID":"123"}')
        self.assertEqual(value["ID"], "123")

    def test_decode_message_value_invalid_type(self):
        """ Test that unsupported payload types raise ValueError. """
        with self.assertRaises(ValueError):
            self.service.decode_message_value(123)

    def test_decode_message_value_invalid_json(self):
        """ Test that invalid JSON raises JSONDecodeError. """
        with self.assertRaises(json.JSONDecodeError):
            self.service.decode_message_value(b'{"ID":')

    def test_add_timestamps_producer(self):
        """ Test adding producer timestamp metadata. """
        msg = MagicMock()
        msg.timestamp.return_value = (1, 1700000000000)
        value = {}

        result = self.service.add_timestamps(value, msg)

        self.assertIn("attributes", result)
        self.assertEqual(result["attributes"]["kafka_timestamp_type"], "producer")
        self.assertIn("asset_forwarder_timestamp", result["attributes"])
        self.assertIn("kafka_timestamp", result["attributes"])

    def test_add_timestamps_broker(self):
        """ Test adding broker timestamp metadata. """
        msg = MagicMock()
        msg.timestamp.return_value = (2, 1700000000000)
        value = {}

        result = self.service.add_timestamps(value, msg)

        self.assertEqual(result["attributes"]["kafka_timestamp_type"], "broker")

    def test_add_timestamps_unknown_type(self):
        """ Test adding unknown Kafka timestamp type. """
        msg = MagicMock()
        msg.timestamp.return_value = (999, 1700000000000)
        value = {}

        result = self.service.add_timestamps(value, msg)

        self.assertEqual(result["attributes"]["kafka_timestamp_type"], "unknown")

    def test_add_timestamps_without_kafka_timestamp(self):
        """ Test handling messages without Kafka timestamp. """
        msg = MagicMock()
        msg.timestamp.return_value = (0, None)
        value = {}

        result = self.service.add_timestamps(value, msg)

        self.assertIn("asset_forwarder_timestamp", result["attributes"])
        self.assertNotIn("kafka_timestamp", result["attributes"])
        self.assertNotIn("kafka_timestamp_type", result["attributes"])

    def test_add_timestamps_preserves_and_extends_attributes(self):
        """ Test that existing attributes are preserved. """
        msg = MagicMock()
        msg.timestamp.return_value = (1, 1700000000000)

        value = {
            "attributes": {
                "existing": "value"
            }
        }

        result = self.service.add_timestamps(value, msg)

        self.assertEqual(result["attributes"]["existing"], "value")
        self.assertIn("asset_forwarder_timestamp", result["attributes"])
        self.assertIn("kafka_timestamp", result["attributes"])

    def test_build_nats_message_bytes_key(self):
        """ Test building a NATS message from a bytes Kafka key. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"
        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        value = {
            "ID": "abc123",
            "name": "test"
        }

        returned_cluster, subject, payload = self.service.build_nats_message(msg, value)

        self.assertEqual(returned_cluster, cluster)
        self.assertEqual(subject, "asset-1.abc123")

        payload_dict = json.loads(payload.decode())

        self.assertNotIn("ID", payload_dict)
        self.assertEqual(payload_dict["name"], "test")

    def test_build_nats_message_string_key(self):
        """ Test building a NATS message from a string Kafka key. """
        msg = MagicMock()
        msg.key.return_value = "asset-1"

        cluster = MagicMock()
        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}
        value = {"ID": "abc123"}

        returned_cluster, subject, payload = self.service.build_nats_message(msg, value)

        self.assertEqual(returned_cluster, cluster)
        self.assertEqual(subject, "asset-1.abc123")

        payload_dict = json.loads(payload.decode())
        self.assertEqual(payload_dict, {})

    def test_build_nats_message_case_insensitive_id(self):
        """ Test that ID lookup is case-insensitive. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        value = {
            "Id": "abc123",
            "name": "test"
        }

        _, subject, payload = self.service.build_nats_message(msg, value)

        self.assertEqual(subject, "asset-1.abc123")

        payload_dict = json.loads(payload.decode())

        self.assertNotIn("Id", payload_dict)
        self.assertEqual(payload_dict["name"], "test")

    def test_build_nats_message_lowercase_id(self):
        """ Test that lowercase id is accepted. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        value = {
            "id": "abc123",
            "name": "test"
        }

        _, subject, payload = self.service.build_nats_message(msg, value)

        self.assertEqual(subject, "asset-1.abc123")

        payload_dict = json.loads(payload.decode())

        self.assertNotIn("id", payload_dict)
        self.assertEqual(payload_dict["name"], "test")

    def test_build_nats_message_missing_id(self):
        """ Test that missing ID raises ValueError. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"
        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        with self.assertRaises(ValueError):
            self.service.build_nats_message(msg, {"name": "test"})

    def test_build_nats_message_empty_id(self):
        """ Test that empty ID raises ValueError. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        with self.assertRaises(ValueError):
            self.service.build_nats_message(msg, {"ID": ""})

    def test_build_nats_message_none_id(self):
        """ Test that None ID raises ValueError. """
        msg = MagicMock()
        msg.key.return_value = b"asset-1"

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        with self.assertRaises(ValueError):
            self.service.build_nats_message(msg, {"ID": None})

    def test_build_nats_message_missing_key(self):
        """ Test that missing Kafka key raises ValueError. """
        msg = MagicMock()
        msg.key.return_value = None

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        with self.assertRaises(ValueError):
            self.service.build_nats_message(msg, {"ID": "abc123"})

    def test_build_nats_message_uses_asset_uuid_for_hash_ring(self):
        """ Test that hash ring lookup uses the asset UUID bytes. """
        msg = MagicMock()
        msg.key.return_value = "asset-1"

        cluster = MagicMock()

        self.service.hash_ring.get.return_value = "C1"
        self.service.nats_clusters = {"C1": cluster}

        self.service.build_nats_message(msg, {"ID": "abc123"})

        self.service.hash_ring.get.assert_called_once_with(b"asset-1")

    def test_on_assign_logs(self):
        """ Test partition assignment logging. """
        consumer = MagicMock()
        partitions = [MagicMock()]

        self.service.register_prometheus_metrics = MagicMock()

        self.service._on_assign(consumer, partitions)

        self.service.logger.info.assert_called_once_with(
            "Assigned: %s",
            partitions
        )

    def test_on_assign(self):
        """ Test partition assignment callback. """
        consumer = MagicMock()
        partitions = [MagicMock()]
        self.service.register_prometheus_metrics = MagicMock()

        self.service._on_assign(consumer, partitions)

        consumer.assign.assert_called_once_with(partitions)
        self.service.register_prometheus_metrics.assert_called_once_with(
            metrics_port=4000,
            metrics_path="/metrics"
        )

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.os._exit", side_effect=SystemExit(1))
    def test_on_assign_empty_partition_list_is_fatal(self, mock_exit):
        """ Test that an empty partition assignment is treated as fatal. """

        consumer = MagicMock()
        self.service.register_prometheus_metrics = MagicMock()

        with self.assertRaises(SystemExit):
            self.service._on_assign(consumer, [])

        self.service.logger.critical.assert_called_once_with(
            "Kafka consumer was assigned zero partitions. "
            "Are there too many forwarders running comapred to topic partions ?"
        )

        mock_exit.assert_called_once_with(1)
        consumer.assign.assert_not_called()
        self.service.register_prometheus_metrics.assert_not_called()

    def test_on_revoke(self):
        """ Test partition revocation callback. """
        consumer = MagicMock()
        partitions = [MagicMock()]

        self.service._on_revoke(consumer, partitions)

        consumer.commit.assert_called_once_with(asynchronous=False)

    def test_on_revoke_logs(self):
        """ Test partition revocation logging. """
        consumer = MagicMock()
        partitions = [MagicMock()]

        self.service._on_revoke(consumer, partitions)

        self.service.logger.info.assert_called_once_with("Revoked: %s", partitions)

    def test_on_revoke_commit_failure(self):
        """ Test that revoke continues if commit fails. """
        consumer = MagicMock()
        consumer.commit.side_effect = Exception()

        self.service._on_revoke(consumer, [])
