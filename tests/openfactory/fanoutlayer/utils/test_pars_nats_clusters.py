import os
import unittest
from openfactory.fanoutlayer.utils.parse_nats_clusters import parse_nats_clusters


class TestParseNatsClusters(unittest.TestCase):
    """
    Tests for the parse_nats_clusters() function.
    """

    def setUp(self):
        """ Save the current environment variables before each test. """
        self._original_env = os.environ.copy()

    def tearDown(self):
        """ Restore the environment variables after each test. """
        os.environ.clear()
        os.environ.update(self._original_env)

    def test_single_cluster_single_server(self):
        """ Test that one cluster with a single server is parsed correctly. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats-c1-1:4222"
        clusters = parse_nats_clusters()
        self.assertEqual(clusters, {"C1": ["nats://nats-c1-1:4222"]})

    def test_single_cluster_multiple_servers(self):
        """ Test that one cluster with multiple servers is parsed into a list. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats-c1-1:4222, nats://nats-c1-2:4222"
        clusters = parse_nats_clusters()
        self.assertEqual(
            clusters,
            {"C1": ["nats://nats-c1-1:4222", "nats://nats-c1-2:4222"]}
        )

    def test_multiple_clusters(self):
        """ Test that multiple clusters are parsed into separate entries. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats-c1-1:4222"
        os.environ["NATS_CLUSTER_C2"] = "nats://nats-c2-1:4222,nats://nats-c2-2:4222"
        clusters = parse_nats_clusters()
        self.assertEqual(
            clusters,
            {
                "C1": ["nats://nats-c1-1:4222"],
                "C2": ["nats://nats-c2-1:4222", "nats://nats-c2-2:4222"],
            }
        )

    def test_ignores_non_cluster_variables(self):
        """ Test that unrelated environment variables are ignored. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats-c1-1:4222"
        os.environ["OTHER_VAR"] = "some_value"
        clusters = parse_nats_clusters()
        self.assertEqual(clusters, {"C1": ["nats://nats-c1-1:4222"]})
        self.assertNotIn("OTHER_VAR", clusters)

    def test_empty_servers_are_ignored(self):
        """ Test that empty entries in server list are ignored. """
        os.environ["NATS_CLUSTER_C1"] = "nats://nats-c1-1:4222, , "
        clusters = parse_nats_clusters()
        self.assertEqual(clusters, {"C1": ["nats://nats-c1-1:4222"]})

    def test_no_clusters_configured_raises(self):
        """ Test that RuntimeError is raised when no clusters are configured. """
        os.environ.pop("NATS_CLUSTER_C1", None)
        with self.assertRaises(RuntimeError) as ctx:
            parse_nats_clusters()
        self.assertIn("No NATS clusters configured", str(ctx.exception))
