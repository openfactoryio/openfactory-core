import unittest
from unittest.mock import patch, MagicMock
from openfactory.utils.assets import deregister_device_connector
from kafka.errors import KafkaError


class TestDeregisterDeviceConnectorKafka(unittest.TestCase):
    """
    Test deregister_device_connector
    """

    @patch("openfactory.utils.assets.KafkaProducer")
    def test_tombstone_sent_correctly(self, mock_producer_class):
        """ Test that the tombstone is sent with the correct key and value=None. """
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        device_uuid = "test-device-uuid"
        topic = "test-topic"
        bootstrap_servers = "localhost:9092"

        deregister_device_connector(
            device_uuid=device_uuid,
            bootstrap_servers=bootstrap_servers,
            topic=topic
        )

        # Check that producer was created with correct bootstrap servers
        mock_producer_class.assert_called_once_with(
            bootstrap_servers=bootstrap_servers,
            key_serializer=unittest.mock.ANY,
            value_serializer=unittest.mock.ANY
        )

        # Check that send() was called with key=device_uuid and value=None
        mock_producer.send.assert_called_once_with(topic, key=device_uuid, value=None)

        # Check that flush() was called
        mock_producer.flush.assert_called_once()

    @patch("openfactory.utils.assets.KafkaProducer")
    def test_key_and_value_serializers(self, mock_producer_class):
        """ Test that key and value serializers behave correctly. """
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        device_uuid = "uuid123"
        deregister_device_connector(device_uuid, bootstrap_servers="kafka-broker:9092")

        # Extract the serializers
        key_serializer = mock_producer_class.call_args[1]["key_serializer"]
        value_serializer = mock_producer_class.call_args[1]["value_serializer"]

        # Key serializer encodes string to bytes
        self.assertEqual(key_serializer("abc"), b"abc")

        # Value serializer encodes JSON to bytes
        import json
        self.assertEqual(value_serializer({"foo": "bar"}), json.dumps({"foo": "bar"}).encode("utf-8"))

        # Value serializer returns None when value is None (tombstone)
        self.assertIsNone(value_serializer(None))

    @patch("openfactory.utils.assets.KafkaProducer")
    def test_produce_failure_raises_kafka_error(self, mock_producer_class):
        """ Test that a KafkaError is propagated if send fails. """
        mock_producer = MagicMock()
        mock_producer.send.side_effect = KafkaError("Kafka failure")
        mock_producer_class.return_value = mock_producer

        with self.assertRaises(KafkaError):
            deregister_device_connector("some-uuid", bootstrap_servers="kafka-broker:9092")
