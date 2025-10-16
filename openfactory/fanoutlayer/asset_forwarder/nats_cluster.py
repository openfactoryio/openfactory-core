"""
NATS Cluster Wrapper
===================

This module provides the ``NatsCluster`` class, which wraps a connection
to a single NATS cluster for use by the OpenFactory Asset Forwarder.

The class handles:

- Connection management and reconnection logic.
- Safe publishing of messages to NATS subjects.
- Graceful shutdown and close of connections.

It is designed to ensure that asset messages are routed reliably to the
correct NATS cluster.

Usage
-----

.. code-block:: python

    from nats_cluster import NatsCluster
    import asyncio

    cluster = NatsCluster(name="C1", servers=["nats://localhost:4222"])
    asyncio.run(cluster.connect())
    asyncio.run(cluster.publish("asset.123", b"payload"))
    asyncio.run(cluster.close())

Attributes
----------

- ``name`` (str): Logical name of the NATS cluster (e.g., "C1").
- ``servers`` (List[str]): List of NATS server URLs.
- ``reconnect_time_wait`` (int): Seconds to wait between reconnect attempts.
- ``nc`` (Optional[NATS]): Active NATS client connection.
- ``_lock`` (asyncio.Lock): Ensures only one concurrent connection attempt.
"""

import asyncio
from typing import List, Optional
from nats.aio.client import Client as NATS
from .logger import logger


class NatsCluster:
    """
    Wrapper around a NATS cluster connection.

    This class manages a connection to a single NATS cluster, handling
    reconnection logic and providing safe publish/close operations.

    It is designed to be used by the Asset Forwarder to route asset messages
    consistently to the correct cluster.

    Attributes:
        name (str): Logical name of the NATS cluster (e.g., "C1").
        servers (List[str]): List of NATS server URLs for this cluster.
        reconnect_time_wait (int): Time in seconds to wait before reconnect attempts.
        nc (Optional[NATS]): Active NATS client connection, or None if not connected.
        _lock (asyncio.Lock): Ensures only one connection attempt runs at a time.
    """

    def __init__(self, name: str, servers: List[str], reconnect_time_wait: int = 2) -> None:
        """
        Initialize a NATS cluster wrapper.

        Args:
            name (str): Cluster name identifier (used for logging and routing).
            servers (List[str]): List of server URLs for connecting to this cluster.
            reconnect_time_wait (int, optional): Delay (in seconds) between reconnect
                attempts. Defaults to 2.
        """
        self.name = name
        self.servers = servers
        self.nc: Optional[NATS] = None
        self._lock = asyncio.Lock()
        self.reconnect_time_wait = reconnect_time_wait

    async def connect(self) -> None:
        """
        Establish a connection to the NATS cluster.

        If already connected, this method does nothing. Ensures that only
        one connection attempt is active at a time.

        Raises:
            Exception: If the connection attempt fails (NATS will retry internally).
        """
        async with self._lock:
            if self.nc:
                try:
                    if self.nc.is_connected:
                        return
                    # close dead client
                    await self.nc.close()
                except Exception:
                    pass

            async def disconnected_cb():
                logger.warning(f"NATS[{self.name}] disconnected — will attempt reconnect...")

            async def reconnected_cb():
                logger.info(f"NATS[{self.name}] reconnected successfully.")

            async def closed_cb():
                logger.warning(f"NATS[{self.name}] connection permanently closed.")

            async def error_handler(e):
                logger.warning("NATS[{self.name}] client error: %s", e)

            nc = NATS()
            logger.info(f"Connecting to NATS server {self.name} at {self.servers} ...")
            await nc.connect(
                servers=self.servers,
                reconnect_time_wait=self.reconnect_time_wait,
                max_reconnect_attempts=-1,  # retry indefinitely
                allow_reconnect=True,
                ping_interval=10,
                disconnected_cb=disconnected_cb,
                reconnected_cb=reconnected_cb,
                closed_cb=closed_cb,
                error_cb=error_handler,
                connect_timeout=5,  # don't block forever on connect
                verbose=False,
            )
            self.nc = nc
            logger.info(f"NATS server {self.name} connected")

    async def publish(self, subject: str, payload: bytes) -> None:
        """
        Publish a message to this NATS cluster.

        Ensures the cluster is connected before publishing.

        Args:
            subject (str): The NATS subject (topic) to publish to.
            payload (bytes): The raw message payload.

        Raises:
            RuntimeError: If unable to connect to the cluster.
        """
        await self.connect()
        if not self.nc:
            raise RuntimeError(f"NATS[{self.name}] not connected")
        await self.nc.publish(subject, payload)
        logger.debug(f"Published {payload} to {self.name} in subject {subject}")

    async def close(self) -> None:
        """
        Close the connection to the NATS cluster.

        Attempts a graceful drain first, then forces close if needed.
        Safe to call multiple times.
        """
        if self.nc:
            try:
                logger.info(f"Draining NATS server {self.name} ...")
                await self.nc.drain()
            except Exception:
                pass
            try:
                await self.nc.close()
                logger.info(f"Closed connection to NATS server {self.name}")
            except Exception:
                pass
            self.nc = None
