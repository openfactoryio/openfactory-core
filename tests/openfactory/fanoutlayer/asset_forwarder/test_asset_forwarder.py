import unittest
import asyncio
from unittest.mock import AsyncMock, patch
from openfactory.fanoutlayer.asset_forwarder.asset_forwarder import AssetForwarder


class TestAssetForwarder(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the AssetForwarder class.
    """

    def setUp(self):
        """ Create an AssetForwarder with mocked NATS clusters. """
        nats_clusters = {"C1": ["nats://localhost:4222"], "C2": ["nats://localhost:4223"]}
        kafka_config = {"bootstrap.servers": "localhost:9092"}

        # Patch NatsCluster to prevent real connections
        patcher = patch("openfactory.fanoutlayer.asset_forwarder.asset_forwarder.NatsCluster")
        self.addCleanup(patcher.stop)
        self.mock_nats_cls = patcher.start()
        self.mock_nc1 = AsyncMock()
        self.mock_nc2 = AsyncMock()
        self.mock_nats_cls.side_effect = [self.mock_nc1, self.mock_nc2]

        self.forwarder = AssetForwarder(
            kafka_config=kafka_config,
            kafka_topic="ofa_assets",
            nats_clusters=nats_clusters,
            group_id="test_group",
            nats_publish_concurrency=2,
            max_retries=2,
            consistent_replicas=10,
        )

    async def test_worker_routes_by_hash_ring(self):
        """ Test that the worker routes messages to the correct NATS cluster. """
        # Build a fake message
        msg = {
            "topic": "ofa_assets",
            "partition": 0,
            "offset": 1,
            "key": b"asset123",
            "value": b'{"ID": "msg1", "foo": "bar"}'
        }
        await self.forwarder.queue.put(msg)
        await self.forwarder.queue.put(None)  # sentinel to stop worker

        # Pick one worker and run it
        worker_task = asyncio.create_task(self.forwarder._worker(0))
        await self.forwarder.queue.join()
        worker_task.cancel()

        # Determine which cluster the key maps to
        cluster_name = self.forwarder.hash_ring.get(msg["key"])
        expected_subject = f"{msg['key'].decode()}.msg1"
        expected_payload = b'{"foo": "bar"}'

        # Verify correct publish call
        cluster_mock = self.forwarder.nats_clusters[cluster_name]
        cluster_mock.publish.assert_awaited_once_with(expected_subject, expected_payload)

    async def test_worker_skips_message_without_key(self):
        """Test that worker skips messages without a key."""
        msg = {"topic": "ofa_assets", "partition": 0, "offset": 1, "key": None, "value": b"{}"}
        await self.forwarder.queue.put(msg)
        await self.forwarder.queue.put(None)

        worker_task = asyncio.create_task(self.forwarder._worker(0))
        await self.forwarder.queue.join()
        worker_task.cancel()

        # No cluster should have been called
        for cluster in self.forwarder.nats_clusters.values():
            cluster.publish.assert_not_awaited()

    async def test_publish_with_retry_retries_on_failure(self):
        """Test that _publish_with_retry retries the correct number of times."""
        failing_cluster = AsyncMock()
        failing_cluster.publish.side_effect = Exception("fail")

        result = await self.forwarder._publish_with_retry(failing_cluster, "subj", b"payload")
        self.assertFalse(result)
        self.assertEqual(failing_cluster.publish.await_count, self.forwarder.max_retries + 1)

    async def test_payload_parsing_handles_invalid_json(self):
        """Test that invalid JSON in value does not raise and sets unknown ID."""
        msg = {
            "topic": "ofa_assets",
            "partition": 0,
            "offset": 1,
            "key": b"asset123",
            "value": b"not-json"
        }
        await self.forwarder.queue.put(msg)
        await self.forwarder.queue.put(None)

        worker_task = asyncio.create_task(self.forwarder._worker(0))
        await self.forwarder.queue.join()
        worker_task.cancel()

        # Cluster still called with "unknown" ID
        cluster_name = self.forwarder.hash_ring.get(msg["key"])
        expected_subject_start = f"{msg['key'].decode()}.unknown"
        cluster_mock = self.forwarder.nats_clusters[cluster_name]
        subject_arg = cluster_mock.publish.await_args[0][0]
        self.assertTrue(subject_arg.startswith(expected_subject_start))

    async def test_queue_done_called_even_on_skip(self):
        """Ensure queue.task_done is called even if worker skips message."""
        msg = {"topic": "ofa_assets", "partition": 0, "offset": 1, "key": None, "value": b"{}"}
        await self.forwarder.queue.put(msg)
        await self.forwarder.queue.put(None)

        # Patch queue.task_done to track calls
        with patch.object(self.forwarder.queue, "task_done", wraps=self.forwarder.queue.task_done) as mock_done:
            worker_task = asyncio.create_task(self.forwarder._worker(0))
            await self.forwarder.queue.join()
            worker_task.cancel()
            self.assertEqual(mock_done.call_count, 2)  # one for skipped + one for sentinel
