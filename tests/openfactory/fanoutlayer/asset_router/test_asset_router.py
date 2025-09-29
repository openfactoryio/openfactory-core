import os
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from openfactory.fanoutlayer.asset_router.asset_router import app, _create_hash_ring
from openfactory.fanoutlayer.utils.hash_ring import ConsistentHashRing


class TestAssetRouter(unittest.TestCase):
    """
    Unit tests for the Asset Router Service.
    """

    def setUp(self):
        """ Set up environment variables and test client. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats1:4222"
        os.environ["NATS_CLUSTER_C2"] = "nats://nats2-1:4222,nats://nats2-2:4222"
        os.environ["HASH_RING_REPLICAS"] = "10"

        # Re-create ring and clusters with these env vars
        from openfactory.fanoutlayer.asset_router import asset_router
        asset_router.ring, asset_router.nats_clusters = asset_router._create_hash_ring()

        self.client = TestClient(app)
        self.ring, self.clusters = _create_hash_ring()

    def tearDown(self):
        """ Clean up environment variables after each test. """
        for key in list(os.environ.keys()):
            if key.startswith("NATS_CLUSTER_") or key == "HASH_RING_REPLICAS":
                del os.environ[key]

    # ------------------------------
    # Tests for _create_hash_ring
    # ------------------------------
    def test_create_hash_ring_returns_ring_and_clusters(self):
        """ Test that the hash ring and clusters dict are returned correctly. """
        ring, clusters = _create_hash_ring()
        self.assertIn("C1", clusters)
        self.assertIn("C2", clusters)
        self.assertEqual(clusters["C1"], ["nats://nats1:4222"])
        self.assertEqual(clusters["C2"], ["nats://nats2-1:4222", "nats://nats2-2:4222"])
        self.assertIsInstance(ring, ConsistentHashRing)

    @patch("openfactory.fanoutlayer.asset_router.asset_router.ConsistentHashRing")
    def test_consistent_hash_ring_called_with_correct_args(self, mock_ring):
        """ Verify that _create_hash_ring calls ConsistentHashRing with expected nodes and replicas. """
        _create_hash_ring()

        # Expected call arguments
        expected_nodes = ["C1", "C2"]
        expected_replicas = 10

        mock_ring.assert_called_once_with(nodes=expected_nodes, replicas=expected_replicas)

    def test_create_hash_ring_no_clusters_raises(self):
        """ Test that a RuntimeError is raised if no clusters are configured. """
        for key in list(os.environ.keys()):
            if key.startswith("NATS_CLUSTER_"):
                del os.environ[key]
        with self.assertRaises(RuntimeError):
            _create_hash_ring()

    # ------------------------------
    # Tests for /asset/{asset_id} endpoint
    # ------------------------------
    def test_get_asset_nats_route_returns_expected_cluster(self):
        """ Test that endpoint returns the cluster and URL according to the hash ring. """
        asset_id = "testasset"
        expected_cluster = self.ring.get(asset_id.encode())
        expected_url = self.clusters[expected_cluster][0]

        response = self.client.get(f"/asset/{asset_id}")
        self.assertEqual(response.status_code, 200)
        json_data = response.json()

        self.assertEqual(json_data["asset_id"], asset_id)
        self.assertEqual(json_data["cluster_name"], expected_cluster)
        self.assertEqual(json_data["nats_url"], expected_url)

    def test_get_asset_nats_route_is_deterministic(self):
        """ Test that the same asset_id always maps to the same cluster. """
        asset_id = "myasset"
        response1 = self.client.get(f"/asset/{asset_id}")
        response2 = self.client.get(f"/asset/{asset_id}")

        self.assertEqual(response1.status_code, 200)
        self.assertEqual(response2.status_code, 200)
        self.assertEqual(response1.json(), response2.json())

    def test_get_asset_nats_route_raises_500_on_exception(self):
        """ Test that endpoint returns 500 if ring.get raises an exception. """
        with patch("openfactory.fanoutlayer.asset_router.asset_router.ring.get", side_effect=Exception("fail")):
            response = self.client.get("/asset/fail")
            self.assertEqual(response.status_code, 500)
            self.assertIn("fail", response.json()["detail"])
