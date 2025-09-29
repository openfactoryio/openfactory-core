import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from openfactory.fanoutlayer.asset_forwarder.nats_cluster import NatsCluster
from openfactory.fanoutlayer.utils.hash_ring import ConsistentHashRing


class TestFanOutRouting(unittest.IsolatedAsyncioTestCase):
    """
    Integration-style tests for fan-out routing of asset messages across
    multiple NATS clusters using a consistent hash ring.
    """

    def setUp(self):
        """ Set up multiple mocked clusters and a hash ring. """
        # Create multiple clusters
        self.clusters = {
            "C1": NatsCluster(name="C1", servers=["nats://localhost:4222"]),
            "C2": NatsCluster(name="C2", servers=["nats://localhost:4223"]),
            "C3": NatsCluster(name="C3", servers=["nats://localhost:4224"]),
        }

        # Create hash ring from cluster names
        self.ring = ConsistentHashRing(nodes=list(self.clusters.keys()), replicas=10)

        # Patch NATS to prevent real connections
        self.nats_patch = patch("openfactory.fanoutlayer.asset_forwarder.nats_cluster.NATS", autospec=True)
        self.mock_nats_cls = self.nats_patch.start()

        # Assign a separate mocked NATS client per cluster
        for i, cluster in enumerate(self.clusters.values()):
            mock_nc = MagicMock()
            mock_nc.connect = AsyncMock()
            mock_nc.publish = AsyncMock()
            mock_nc.is_connected = True
            self.mock_nats_cls.side_effect = [mock_nc] * len(self.clusters)
            cluster.nc = mock_nc

    def tearDown(self):
        """ Stop patching. """
        self.nats_patch.stop()

    async def test_assets_routed_correctly(self):
        """ Test that multiple assets are routed consistently to the correct clusters. """
        # Simulate multiple assets
        assets = [f"asset-{i}" for i in range(500)]
        expected_mapping = {}

        # Determine expected cluster for each asset
        for asset in assets:
            cluster_name = self.ring.get(asset.encode())
            expected_mapping[asset] = cluster_name

        # Publish messages through clusters
        for asset in assets:
            cluster_name = self.ring.get(asset.encode())
            cluster = self.clusters[cluster_name]
            await cluster.publish(f"subject.{asset}", f"payload-{asset}".encode())

        # Verify that each cluster received the correct assets
        for asset, cluster_name in expected_mapping.items():
            cluster = self.clusters[cluster_name]
            cluster.nc.publish.assert_any_await(f"subject.{asset}", f"payload-{asset}".encode())

    async def test_routing_is_deterministic(self):
        """ Test that the same asset always maps to the same cluster. """
        assets = [f"asset-{i}" for i in range(500)]
        mapping_first = {asset: self.ring.get(asset.encode()) for asset in assets}
        mapping_second = {asset: self.ring.get(asset.encode()) for asset in assets}
        self.assertEqual(mapping_first, mapping_second)
