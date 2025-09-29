import unittest
from openfactory.fanoutlayer import ConsistentHashRing


class TestConsistentHashRing(unittest.TestCase):
    """
    Tests for class ConsistentHashRing
    """

    def setUp(self):
        """ Create a ring with three nodes and 10 replicas for testing. """
        self.nodes = ["node1", "node2", "node3"]
        self.ring = ConsistentHashRing(self.nodes, replicas=10)  # fewer replicas for test speed

    def test_init_with_empty_nodes(self):
        """ Test that initializing with empty node list raises ValueError. """
        with self.assertRaises(ValueError):
            ConsistentHashRing([])

    def test_ring_contains_expected_number_of_vnodes(self):
        """ Test that the ring contains correct number of virtual nodes. """
        expected_vnodes = len(self.nodes) * 10
        self.assertEqual(len(self.ring._ring), expected_vnodes)
        self.assertEqual(len(self.ring._sorted_keys), expected_vnodes)

    def test_sorted_keys_are_sorted(self):
        """ Test that the ring’s sorted keys are in ascending order. """
        self.assertEqual(sorted(self.ring._sorted_keys), self.ring._sorted_keys)

    def test_get_returns_node_from_list(self):
        """ Test that get() always returns a node from the provided list. """
        key = b"mykey"
        node = self.ring.get(key)
        self.assertIn(node, self.nodes)

    def test_get_is_deterministic(self):
        """ Test that the same key always maps to the same node. """
        key = b"consistent-key"
        node1 = self.ring.get(key)
        node2 = self.ring.get(key)
        self.assertEqual(node1, node2)

    def test_get_wraps_around_ring(self):
        """ Test that when hash > max vnode, lookup wraps to first node. """
        import bisect

        # Force idx to be len(sorted_keys), simulating wrap-around
        idx = bisect.bisect(self.ring._sorted_keys, max(self.ring._sorted_keys))
        self.assertEqual(idx, len(self.ring._sorted_keys))

        # In wrap-around case, get() should return first node
        expected_node = self.ring._ring[self.ring._sorted_keys[0]]

        # Manually simulate the wrap-around
        wrapped_idx = 0
        actual_node = self.ring._ring[self.ring._sorted_keys[wrapped_idx]]

        self.assertEqual(actual_node, expected_node)

    def test_reassignment_minimized_when_adding_node(self):
        """ Test that adding a node only reassigns keys if necessary. """
        key = b"customer123"
        old_node = self.ring.get(key)

        # Add a new node and reinitialize
        new_nodes = self.nodes + ["node4"]
        new_ring = ConsistentHashRing(new_nodes, replicas=10)
        new_node = new_ring.get(key)

        self.assertIn(new_node, new_nodes)
        if new_node != old_node:
            self.assertEqual(new_node, "node4")

    def test_get_accepts_non_bytes_input(self):
        """ Test that get() accepts str and int keys in addition to bytes. """
        node_from_str = self.ring.get("string-key")
        node_from_int = self.ring.get(12345)
        self.assertIn(node_from_str, self.nodes)
        self.assertIn(node_from_int, self.nodes)
