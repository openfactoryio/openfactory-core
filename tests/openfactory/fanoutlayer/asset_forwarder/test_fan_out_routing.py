import unittest
from unittest.mock import AsyncMock, MagicMock
from logging import Logger
from openfactory.fanoutlayer.asset_forwarder.src.nats_cluster import NatsCluster
from openfactory.fanoutlayer.utils.hash_ring import ConsistentHashRing


class TestFanOutRouting(unittest.IsolatedAsyncioTestCase):
    """
    Integration-style tests for fan-out routing of asset messages across
    multiple NATS clusters using a consistent hash ring.
    """

    def setUp(self):
        """ Set up multiple mocked clusters and a hash ring. """
        # Create multiple clusters
        self.logger = MagicMock(spec=Logger)
        self.clusters = {
            "C1": NatsCluster(name="C1", servers=["nats://localhost:4222"], logger=self.logger),
            "C2": NatsCluster(name="C2", servers=["nats://localhost:4223"], logger=self.logger),
            "C3": NatsCluster(name="C3", servers=["nats://localhost:4224"], logger=self.logger),
        }

        # Create hash ring from cluster names
        self.ring = ConsistentHashRing(nodes=list(self.clusters.keys()), replicas=10)

        # Assign a separate mocked NATS client per cluster
        for i, cluster in enumerate(self.clusters.values()):
            mock_nc = MagicMock()
            mock_nc.connect = AsyncMock()
            mock_nc.publish = AsyncMock()
            mock_nc.is_connected = True
            cluster.nc = mock_nc

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

    async def test_routing_uses_all_clusters(self):
        """ Test that assets are distributed across all configured clusters. """
        assets = [f"asset-{i}" for i in range(500)]

        used_clusters = {
            self.ring.get(asset.encode())
            for asset in assets
        }

        self.assertEqual(used_clusters, set(self.clusters.keys()))
