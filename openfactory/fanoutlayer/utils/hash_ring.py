"""
Consistent Hash Ring
====================

This module provides the ``ConsistentHashRing`` class, which maps arbitrary
keys (e.g., asset UUIDs) to nodes (e.g., NATS clusters) using consistent hashing.

It uses MD5 hashing and virtual nodes to:

- Ensure an even distribution of keys across nodes.
- Minimize key reassignment when nodes are added or removed.

Usage
-----

.. code-block:: python

    from openfactory.fanoutlayer import ConsistentHashRing

    ring = ConsistentHashRing(nodes=["C1", "C2"], replicas=128)
    node = ring.get(b"some_key")

Features
--------

- Deterministic mapping of keys to nodes.
- Even key distribution via virtual nodes.
- Minimal reassignments on topology changes.
"""

import hashlib
from typing import Dict, List


class ConsistentHashRing:
    """
    Consistent Hash Ring for mapping keys to nodes.

    This implementation uses MD5 hashing and virtual nodes to distribute keys
    evenly across a set of nodes. It ensures that each key is always mapped to
    the same node unless the set of nodes changes, and minimizes reassignments
    when nodes are added or removed.

    Attributes:
        _ring (Dict[int, str]): Mapping of hash values to node identifiers.
        _sorted_keys (List[int]): Sorted list of hash values (the ring positions).
    """

    def __init__(self, nodes: List[str], replicas: int = 128) -> None:
        """
        Initialize the consistent hash ring.

        Args:
            nodes (List[str]): List of node identifiers (e.g., cluster names).
            replicas (int, optional): Number of virtual nodes per physical node.
                Defaults to 128. Higher values yield a more balanced distribution
                but require more memory.

        Raises:
            ValueError: If the provided node list is empty.
        """
        if not nodes:
            raise ValueError("nodes must not be empty")

        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []

        for node in nodes:
            for r in range(replicas):
                vnode_key = f"{node}::{r}".encode()
                h = int(hashlib.md5(vnode_key).hexdigest(), 16)
                self._ring[h] = node
                self._sorted_keys.append(h)

        self._sorted_keys.sort()

    def get(self, key: bytes) -> str:
        """
        Get the node responsible for the given key.

        Args:
            key (bytes): The key to hash. If not already bytes, it will be converted to a UTF-8 encoded string.

        Returns:
            str: The node identifier that the key maps to.
        """
        import bisect

        if not isinstance(key, (bytes, bytearray)):
            key = str(key).encode()

        h = int(hashlib.md5(key).hexdigest(), 16)
        idx = bisect.bisect(self._sorted_keys, h)
        if idx == len(self._sorted_keys):
            idx = 0

        return self._ring[self._sorted_keys[idx]]
