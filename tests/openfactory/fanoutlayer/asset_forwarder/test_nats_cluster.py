import unittest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from openfactory.fanoutlayer.asset_forwarder.nats_cluster import NatsCluster


class TestNatsCluster(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the NatsCluster wrapper.
    """

    def setUp(self):
        """ Set up a NatsCluster instance with mock servers. """
        self.cluster_name = "C1"
        self.servers = ["nats://localhost:4222"]
        self.cluster = NatsCluster(name=self.cluster_name,
                                   servers=self.servers,
                                   reconnect_time_wait=1)

    async def test_init_sets_attributes(self):
        """ Test that attributes are set correctly on initialization. """
        self.assertEqual(self.cluster.name, self.cluster_name)
        self.assertEqual(self.cluster.servers, self.servers)
        self.assertEqual(self.cluster.reconnect_time_wait, 1)
        self.assertIsNone(self.cluster.nc)
        self.assertIsInstance(self.cluster._lock, asyncio.Lock)

    @patch("openfactory.fanoutlayer.asset_forwarder.nats_cluster.NATS", autospec=True)
    async def test_connect_calls_nats_connect(self, mock_nats_cls):
        """ Test that connect sets nc and calls NATS.connect with expected args. """
        mock_nc = mock_nats_cls.return_value
        mock_nc.connect = AsyncMock()
        await self.cluster.connect()
        mock_nc.connect.assert_awaited_once_with(servers=self.servers, reconnect_time_wait=1)
        self.assertEqual(self.cluster.nc, mock_nc)

    @patch("openfactory.fanoutlayer.asset_forwarder.nats_cluster.NATS", autospec=True)
    async def test_connect_does_not_reconnect_if_already_connected(self, mock_nats_cls):
        """ Test that connect does nothing if nc is already connected. """
        mock_nc = mock_nats_cls.return_value
        mock_nc.is_connected = True
        self.cluster.nc = mock_nc
        await self.cluster.connect()
        mock_nc.connect.assert_not_called()

    @patch("openfactory.fanoutlayer.asset_forwarder.nats_cluster.NATS.publish", new_callable=AsyncMock)
    async def test_publish_calls_connect_and_publish(self, mock_publish):
        """ Test that publish calls connect and nc.publish with correct arguments. """
        self.cluster.connect = AsyncMock()
        mock_nc = MagicMock()
        mock_nc.publish = mock_publish
        self.cluster.nc = mock_nc
        await self.cluster.publish("subject.test", b"payload")
        self.cluster.connect.assert_awaited_once()
        mock_publish.assert_awaited_once_with("subject.test", b"payload")

    async def test_publish_raises_runtime_error_if_nc_none(self):
        """ Test that publish raises RuntimeError if connect fails and nc is None. """
        self.cluster.connect = AsyncMock()
        self.cluster.nc = None
        with self.assertRaises(RuntimeError):
            await self.cluster.publish("subject.test", b"payload")

    async def test_close_calls_drain_and_close_and_resets_nc(self):
        """ Test that close calls nc.drain and nc.close, and sets nc to None. """
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        self.cluster.nc = mock_nc
        await self.cluster.close()
        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        self.assertIsNone(self.cluster.nc)

    async def test_close_safe_to_call_multiple_times(self):
        """ Test that close can be called multiple times without error. """
        self.cluster.nc = None
        await self.cluster.close()  # Should not raise
        await self.cluster.close()  # Still safe
