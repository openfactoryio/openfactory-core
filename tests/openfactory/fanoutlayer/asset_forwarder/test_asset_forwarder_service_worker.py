import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service import AssetForwarderService


class TestAssetForwarderWorker(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for AssetForwarderService worker logic.
    """

    def setUp(self):
        """ Create a service instance configured for worker testing. """
        self.service = AssetForwarderService(
            ksqlClient=MagicMock(),
            bootstrap_servers="localhost:9092",
            test_mode=True,
        )

        self.service.logger = MagicMock()
        self.service.consumer = MagicMock()
        self.service.queue = asyncio.Queue()

    async def test_worker_publishes_message_and_stores_offset(self):
        """ Test successful publish to NATS. """
        msg = MagicMock()
        msg.partition.return_value = 0
        msg.offset.return_value = 123
        msg.key.return_value = b"asset-1"

        cluster = MagicMock()
        cluster.name = "C1"
        cluster.publish = AsyncMock()

        self.service.build_nats_message = MagicMock(
            return_value=(cluster, "asset.123", b"payload")
        )

        await self.service.queue.put({
            "msg": msg,
            "value": {"ID": "123"},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        cluster.publish.assert_awaited_once_with("asset.123", b"payload")
        self.service.consumer.store_offsets.assert_called_once_with(message=msg)

    async def test_worker_missing_id_is_skipped(self):
        """ Test that messages without ID are skipped and offset stored. """
        msg = MagicMock()

        await self.service.queue.put({
            "msg": msg,
            "value": {},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        self.service.consumer.store_offsets.assert_called_once_with(message=msg)
        self.service.logger.warning.assert_called()

    async def test_worker_publish_failure_does_not_store_offset(self):
        """ Test publish failures do not store offsets. """
        msg = MagicMock()

        cluster = MagicMock()
        cluster.name = "C1"
        cluster.publish = AsyncMock(side_effect=Exception("NATS failure"))

        self.service.build_nats_message = MagicMock(
            return_value=(cluster, "asset.123", b"payload")
        )

        await self.service.queue.put({
            "msg": msg,
            "value": {"ID": "123"},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        cluster.publish.assert_awaited_once_with("asset.123", b"payload")
        self.service.consumer.store_offsets.assert_not_called()

    async def test_worker_calls_task_done_on_success(self):
        """ Test queue task_done is called after successful processing. """
        msg = MagicMock()

        cluster = MagicMock()
        cluster.name = "C1"
        cluster.publish = AsyncMock()

        self.service.build_nats_message = MagicMock(
            return_value=(cluster, "asset.123", b"payload")
        )

        original_task_done = self.service.queue.task_done
        self.service.queue.task_done = MagicMock(side_effect=original_task_done)

        await self.service.queue.put({
            "msg": msg,
            "value": {"ID": "123"},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        self.service.queue.task_done.assert_called_once()

    async def test_worker_calls_task_done_on_failure(self):
        """ Test queue task_done is called after processing failure. """
        msg = MagicMock()

        self.service.build_nats_message = MagicMock(
            side_effect=ValueError("ID missing")
        )

        original_task_done = self.service.queue.task_done
        self.service.queue.task_done = MagicMock(side_effect=original_task_done)

        await self.service.queue.put({
            "msg": msg,
            "value": {},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        self.service.queue.task_done.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_worker_increments_published_metric(self, mock_metrics):
        """ Test published metric is incremented on success. """
        msg = MagicMock()

        cluster = MagicMock()
        cluster.name = "C1"
        cluster.publish = AsyncMock()

        self.service.build_nats_message = MagicMock(
            return_value=(cluster, "asset.123", b"payload")
        )

        await self.service.queue.put({
            "msg": msg,
            "value": {"ID": "123"},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        mock_metrics.NATS_MESSAGES_PUBLISHED.labels.assert_called_once_with(cluster="C1")
        mock_metrics.NATS_MESSAGES_PUBLISHED.labels.return_value.inc.assert_called_once()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_worker_increments_invalid_message_metric(self, mock_metrics):
        """ Test invalid message metric is incremented. """
        msg = MagicMock()

        self.service.build_nats_message = MagicMock(side_effect=ValueError("ID missing"))

        await self.service.queue.put({
            "msg": msg,
            "value": {},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        mock_metrics.INVALID_MESSAGES.labels.return_value.inc.assert_called_once()
        mock_metrics.INVALID_MESSAGES.labels.assert_called_once_with(forwarder=self.service.asset_uuid)

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.forwarder_metrics")
    async def test_worker_increments_nats_failure_metric(self, mock_metrics):
        """ Test NATS failure metric is incremented. """
        msg = MagicMock()

        cluster = MagicMock()
        cluster.name = "C1"
        cluster.publish = AsyncMock(side_effect=Exception("NATS failure"))

        self.service.build_nats_message = MagicMock(
            return_value=(cluster, "asset.123", b"payload")
        )

        await self.service.queue.put({
            "msg": msg,
            "value": {"ID": "123"},
            "queued_at": asyncio.get_running_loop().time(),
        })

        worker_task = asyncio.create_task(self.service.worker())
        await self.service.queue.join()
        worker_task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await worker_task

        mock_metrics.NATS_PUBLISH_FAILURES.labels.assert_called_once_with(cluster="C1")
        mock_metrics.NATS_PUBLISH_FAILURES.labels.return_value.inc.assert_called_once()
