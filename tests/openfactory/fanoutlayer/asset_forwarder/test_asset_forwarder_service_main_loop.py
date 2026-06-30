import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from confluent_kafka import KafkaError
from openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service import AssetForwarderService


class TestAssetForwarderMainLoop(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for AssetForwarderService async main loop.
    """

    def setUp(self):
        """ Create a service instance configured for main loop testing. """
        self.service = AssetForwarderService(
            ksqlClient=MagicMock(),
            bootstrap_servers="localhost:9092",
            test_mode=True,
        )

        self.service.logger = MagicMock()
        self.service.consumer = MagicMock()
        self.service.queue = asyncio.Queue()

    async def test_async_main_loop_subscribes_consumer(self):
        """ Test Kafka consumer subscription is configured. """

        self.service.worker = AsyncMock()
        self.service.consumer.consume.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        self.service.consumer.subscribe.assert_called_once_with(
            [self.service.kafka_topic],
            on_assign=self.service._on_assign,
            on_revoke=self.service._on_revoke,
            on_lost=self.service._on_lost,
        )

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_queues_valid_message(self, mock_metrics):
        """ Test valid Kafka messages are queued. """

        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = b'{"ID":"123"}'

        self.service.worker = AsyncMock()
        self.service.decode_message_value = MagicMock(return_value={"ID": "123"})
        self.service.add_timestamps = MagicMock(return_value={"ID": "123"})
        self.service.consumer.consume.side_effect = [
            [msg],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        item = self.service.queue.get_nowait()

        self.assertEqual(item["msg"], msg)
        self.assertEqual(item["value"]["ID"], "123")
        mock_metrics.KAFKA_MESSAGES_CONSUMED.labels.assert_called_once_with(forwarder=self.service.asset_uuid)
        mock_metrics.KAFKA_MESSAGES_CONSUMED.labels.return_value.inc.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_updates_queue_size_metric(self, mock_metrics):
        """ Test queue size gauge is updated when message is queued. """

        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = b'{"ID":"123"}'

        self.service.worker = AsyncMock()

        self.service.decode_message_value = MagicMock(return_value={"ID": "123"})
        self.service.add_timestamps = MagicMock(return_value={"ID": "123"})
        self.service.consumer.consume.side_effect = [
            [msg],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        mock_metrics.QUEUE_SIZE.labels.assert_called_once_with(forwarder=self.service.asset_uuid)
        mock_metrics.QUEUE_SIZE.labels.return_value.set.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_ignores_partition_eof(self, mock_metrics):
        """ Test partition EOF messages are ignored. """

        err = MagicMock()
        err.code.return_value = KafkaError._PARTITION_EOF
        msg = MagicMock()
        msg.error.return_value = err
        self.service.worker = AsyncMock()
        self.service.consumer.consume.side_effect = [
            [msg],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        self.assertTrue(self.service.queue.empty())
        mock_metrics.KAFKA_ERRORS.labels.return_value.inc.assert_not_called()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_increments_kafka_error_metric(self, mock_metrics):
        """ Test Kafka errors increment Prometheus metrics. """
        err = MagicMock()
        err.code.return_value = KafkaError._UNKNOWN_PARTITION
        err.name.return_value = "UNKNOWN_PARTITION"
        err.fatal.return_value = False
        err.retriable.return_value = True

        msg = MagicMock()
        msg.error.return_value = err

        self.service.worker = AsyncMock()
        self.service.consumer.consume.side_effect = [
            [msg],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        mock_metrics.KAFKA_ERRORS.labels.assert_called_once_with(forwarder=self.service.asset_uuid)
        mock_metrics.KAFKA_ERRORS.labels.return_value.inc.assert_called_once()
        self.service.logger.error.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.os._exit")
    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_exits_on_fatal_kafka_error(self, mock_metrics, mock_exit):
        """ Test fatal Kafka errors terminate the process. """

        err = MagicMock()
        err.code.return_value = -1
        err.name.return_value = "FATAL_ERROR"
        err.fatal.return_value = True
        err.retriable.return_value = False

        msg = MagicMock()
        msg.error.return_value = err

        self.service.worker = AsyncMock()
        self.service.consumer.consume.side_effect = [
            [msg],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        mock_metrics.KAFKA_ERRORS.labels.return_value.inc.assert_called_once()
        mock_exit.assert_called_once_with(1)
        self.service.logger.critical.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.create_task")
    async def test_async_main_loop_starts_worker_and_watchdog(self, mock_create_task):
        """ Test worker and Kafka watchdog tasks are started. """

        def create_task_side_effect(coro):
            coro.close()  # Prevent "coroutine was never awaited" warnings.
            return MagicMock()

        mock_create_task.side_effect = create_task_side_effect

        self.service.consumer.consume.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        self.assertEqual(mock_create_task.call_count, 2)

        self.assertEqual(mock_create_task.call_args_list[0].args[0].cr_code.co_name, "worker")
        self.assertEqual(mock_create_task.call_args_list[1].args[0].cr_code.co_name, "kafka_watchdog")

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_async_main_loop_increments_consume_calls_metric(self, mock_metrics):
        """ Test Kafka consume call metric is incremented. """

        self.service.worker = AsyncMock()
        self.service.consumer.consume.side_effect = [
            [],
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.async_main_loop()

        self.assertEqual(mock_metrics.KAFKA_CONSUME_CALLS.labels.return_value.inc.call_count, 1)
