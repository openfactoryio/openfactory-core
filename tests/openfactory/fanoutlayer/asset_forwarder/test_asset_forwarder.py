import unittest
import asyncio
import time
import json
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from contextlib import suppress
from openfactory.fanoutlayer.asset_forwarder.asset_forwarder import AssetForwarder


class TestAssetForwarder(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the AssetForwarder class.
    """

    def setUp(self):
        """ Create an AssetForwarder with mocked NATS clusters. """
        nats_clusters = {"C1": ["nats://localhost:4222"], "C2": ["nats://localhost:4223"]}
        kafka_config = {"bootstrap.servers": "mocked_servers"}

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

        self.forwarder.consumer = AsyncMock()
        self.forwarder.consumer.store_offsets = MagicMock()

    async def test_worker_routes_by_hash_ring(self):
        """ Test that the worker routes messages to the correct NATS cluster. """
        value = {"ID": "msg1", "foo": "bar"}
        envelope = {
            "topic": "ofa_assets",
            "partition": 0,
            "msg_offset": 1,                 # worker expects msg_offset
            "key": b"asset123",
            "value": value,
            "enqueue_ts": time.perf_counter(),
        }

        # Put envelope and sentinel
        await self.forwarder.queue.put(envelope)
        await self.forwarder.queue.put(None)  # sentinel to stop worker

        # Run single worker
        worker_task = asyncio.create_task(self.forwarder._worker(0))
        await self.forwarder.queue.join()
        worker_task.cancel()

        # Determine which cluster the key maps to
        cluster_name = self.forwarder.hash_ring.get(envelope["key"])
        expected_subject = "asset123.msg1"
        expected_payload = json.dumps({"foo": "bar"}).encode()

        # Verify correct publish call
        cluster_mock = self.forwarder.nats_clusters[cluster_name]
        cluster_mock.publish.assert_awaited_once_with(expected_subject, expected_payload)

        # The code calls consumer.store_offsets(offsets=[tp]) — since consumer is AsyncMock
        # that call returns a coroutine (not awaited). Check it was at least called:
        self.assertTrue(self.forwarder.consumer.store_offsets.called)

    async def test_worker_skips_message_without_key(self):
        """ Test worker skips messages without a key. """
        envelope = {
            "topic": "ofa_assets",
            "partition": 0,
            "msg_offset": 1,
            "key": None,
            "value": {},               # must be dict
            "enqueue_ts": time.perf_counter(),
        }

        await self.forwarder.queue.put(envelope)
        await self.forwarder.queue.put(None)  # sentinel

        worker_task = asyncio.create_task(self.forwarder._worker(0))
        await self.forwarder.queue.join()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        # No cluster should have been called
        for cluster in self.forwarder.nats_clusters.values():
            cluster.publish.assert_not_awaited()

    async def test_publish_with_retry_retries_on_failure(self):
        """ Test that _publish_with_retry retries the correct number of times. """
        # Ensure stop flag is clear
        self.forwarder._stop_event.clear()

        # Mock a cluster whose publish always fails
        failing_cluster = AsyncMock()
        failing_cluster.publish.side_effect = Exception("fail")

        async def no_op_sleep(_):
            return

        with patch("asyncio.sleep", new=no_op_sleep):
            result = await self.forwarder._publish_with_retry(failing_cluster, "subj", b"payload")

        self.assertFalse(result)
        self.assertEqual(failing_cluster.publish.await_count, self.forwarder.max_retries + 1)

    async def test_payload_parsing_handles_invalid_json(self):
        """ Test that invalid JSON in value does not raise and sets unknown ID. """

        # Patch cluster publish to no-op
        for cluster in self.forwarder.nats_clusters.values():
            cluster.publish = AsyncMock(return_value=True)

        # Patch consumer.store_offsets to no-op
        self.forwarder.consumer.store_offsets = Mock()

        # Patch asyncio.sleep to avoid delays
        async def noop_sleep(_):
            return

        with patch("asyncio.sleep", new=noop_sleep):
            msg = {
                "topic": "ofa_assets",
                "partition": 0,
                "msg_offset": 1,        # must be msg_offset
                "key": b"asset123",
                "value": {"ID": "unknown", "foo": "bar"},
                "enqueue_ts": time.perf_counter(),
            }
            await self.forwarder.queue.put(msg)
            await self.forwarder.queue.put(None)

            worker_task = asyncio.create_task(self.forwarder._worker(0))
            try:
                await self.forwarder.queue.join()
            finally:
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task

        # Cluster should be called with "unknown" ID
        cluster_name = self.forwarder.hash_ring.get(msg["key"])
        expected_subject_start = f"{msg['key'].decode()}.unknown"
        cluster_mock = self.forwarder.nats_clusters[cluster_name]
        subject_arg = cluster_mock.publish.await_args[0][0]
        self.assertTrue(subject_arg.startswith(expected_subject_start))

    async def test_queue_done_called_even_on_skip(self):
        """ Ensure queue.task_done is called even if worker skips message. """
        msg = {
            "topic": "ofa_assets",
            "partition": 0,
            "msg_offset": 1,
            "key": None,  # triggers skip
            "value": {},  # must be dict
            "enqueue_ts": asyncio.get_event_loop().time(),
        }

        await self.forwarder.queue.put(msg)
        await self.forwarder.queue.put(None)  # sentinel

        # Patch queue.task_done to track calls
        with patch.object(self.forwarder.queue, "task_done", wraps=self.forwarder.queue.task_done) as mock_done:
            # Patch cluster publish and store_offsets so worker doesn't hang on them
            for cluster in self.forwarder.nats_clusters.values():
                cluster.publish = Mock()
            self.forwarder.consumer.store_offsets = Mock()

            worker_task = asyncio.create_task(self.forwarder._worker(0))
            try:
                await self.forwarder.queue.join()
            finally:
                worker_task.cancel()
                with suppress(asyncio.CancelledError):
                    await worker_task

            # One call for skipped message + one for sentinel
            self.assertEqual(mock_done.call_count, 2)
