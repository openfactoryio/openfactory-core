"""
OpenFactory Asset Forwarder
===========================

This module implements the ``AssetForwarder`` class, which consumes messages
from a Kafka topic (``ofa_assets``) and forwards them to NATS clusters based
on a consistent hash of the asset UUID (Kafka key). It is designed to be
horizontally scalable, fault-tolerant, and run as a stateless service.

Main Components
---------------

- ``AssetForwarder``: Core class that manages Kafka consumption, NATS publishing, and worker tasks.
- Kafka Consumer: Runs in a background thread, polling Kafka and feeding messages into an asyncio queue.
- Async Workers: Consume messages from the queue, process them, and publish to NATS clusters with retry logic.
- Consistent Hash Ring: Determines which NATS cluster should receive a given asset UUID.
- NATS Cluster Connections: Managed via the ``NatsCluster`` abstraction.

Environment
-----------

- Expects Kafka and NATS connection details to be provided via configuration.
- Logging is handled via the module-level ``logger``.
- Log level can be set via the ``ASSET_FORWARDER_LOG_LEVEL`` environment variable.

Features
--------

- Graceful shutdown via stop event.
- Automatic retries for NATS publishing failures.
- Removes ``ID`` from message payload when forwarding, but uses it for the NATS subject.

Usage
-----

.. code-block:: python

    from asset_forwarder import AssetForwarder
    import asyncio

    kafka_config = {"bootstrap.servers": "localhost:9092", "group.id": "ofa_group"}
    nats_clusters = {"C1": ["nats://localhost:4222"]}

    forwarder = AssetForwarder(kafka_config, "ofa_assets", nats_clusters, group_id="ofa_group")

    asyncio.run(forwarder.start())
"""

import asyncio
import json
import threading
from typing import Dict, List, Optional
from confluent_kafka import Consumer, KafkaError, TopicPartition

from .logger import logger
from .hash_ring import ConsistentHashRing
from .nats_cluster import NatsCluster


class AssetForwarder:
    """
    Forward asset data from Kafka to NATS clusters.

    This component consumes messages from a Kafka topic (`ofa_assets`), and for
    each message it determines the correct NATS cluster using a consistent hash
    of the asset UUID (Kafka key). It then publishes the message to that NATS
    cluster.

    Multiple instances of this class can run in parallel (stateless workers),
    making it horizontally scalable and fault-tolerant.

    Attributes:
        kafka_topic (str): Kafka topic to consume from.
        kafka_config (dict): Kafka consumer configuration.
        consumer (Optional[Consumer]): Kafka consumer instance.
        queue (asyncio.Queue): Queue for passing Kafka messages to async workers.
        _stop_event (threading.Event): Event flag to signal shutdown.
        nats_clusters (Dict[str, NatsCluster]): NATS cluster connections keyed by name.
        hash_ring (ConsistentHashRing): Consistent hash ring for routing messages.
        concurrency (int): Number of async worker tasks.
        max_retries (int): Max retry attempts for publishing to NATS.
        _consumer_thread (Optional[threading.Thread]): Background Kafka polling thread.
        _loop (Optional[asyncio.AbstractEventLoop]): Async event loop reference.
    """

    def __init__(
        self,
        kafka_config: dict,
        kafka_topic: str,
        nats_clusters: Dict[str, List[str]],
        group_id: str,
        queue_maxsize: int = 10000,
        nats_publish_concurrency: int = 20,
        max_retries: int = 5,
        consistent_replicas: int = 128,
    ) -> None:
        """
        Initialize the asset forwarder.

        Args:
            kafka_config (dict): Base configuration for the Kafka consumer.
            kafka_topic (str): Kafka topic name (usually "ofa_assets").
            nats_clusters (Dict[str, List[str]]): Mapping of cluster name to NATS server URLs.
            group_id (str): Kafka consumer group ID.
            queue_maxsize (int, optional): Max items in the internal async queue. Defaults to 10000.
            nats_publish_concurrency (int, optional): Number of async workers for publishing. Defaults to 20.
            max_retries (int, optional): Max retries for publishing failures. Defaults to 5.
            consistent_replicas (int, optional): Number of virtual nodes per cluster in the hash ring. Defaults to 128.
        """
        self.kafka_topic = kafka_topic
        self.kafka_config = dict(kafka_config)
        self.kafka_config["group.id"] = group_id
        self.kafka_config["enable.auto.commit"] = False
        self.kafka_config["auto.offset.reset"] = self.kafka_config.get("auto.offset.reset", "earliest")

        self.consumer: Optional[Consumer] = None
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._stop_event = threading.Event()

        self.nats_clusters = {name: NatsCluster(name, servers) for name, servers in nats_clusters.items()}
        self.hash_ring = ConsistentHashRing(list(self.nats_clusters.keys()), replicas=consistent_replicas)

        self.concurrency = nats_publish_concurrency
        self.max_retries = max_retries
        self._consumer_thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # -------------------------
    # Kafka Consumer callbacks
    # -------------------------
    def _on_assign(self, consumer: Consumer, partitions) -> None:
        """ Callback when partitions are assigned. """
        logger.info("Assigned: %s", partitions)
        consumer.assign(partitions)

    def _on_revoke(self, consumer: Consumer, partitions) -> None:
        """ Callback when partitions are revoked. """
        logger.info("Revoked: %s", partitions)
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass
        consumer.unassign()

    # -------------------------
    # Kafka Consumer thread
    # -------------------------
    def start_consumer_thread(self) -> None:
        """ Start background thread that polls Kafka and pushes messages into the queue. """

        def run():
            consumer = Consumer(self.kafka_config)
            self.consumer = consumer
            consumer.subscribe([self.kafka_topic], on_assign=self._on_assign, on_revoke=self._on_revoke)

            while not self._stop_event.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                envelope = {
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "key": msg.key(),
                    "value": msg.value(),
                }

                try:
                    loop = self._loop
                    if loop and loop.is_running():
                        asyncio.run_coroutine_threadsafe(self.queue.put(envelope), loop)
                except Exception as e:
                    logger.error("Queue put failed: %s", e)

            try:
                consumer.commit(asynchronous=False)
            except Exception:
                pass
            consumer.close()

        t = threading.Thread(target=run, daemon=True)
        self._consumer_thread = t
        t.start()

    # -------------------------
    # Worker logic
    # -------------------------
    async def _publish_with_retry(self, cluster: "NatsCluster", subject: str, payload: bytes) -> bool:
        """
        Attempt to publish to NATS with retries.

        Args:
            cluster (NatsCluster): Target NATS cluster.
            subject (str): NATS subject to publish to.
            payload (bytes): Message payload.

        Returns:
            bool: True if publish succeeded, False if all retries failed.
        """
        attempt = 0
        while attempt <= self.max_retries:
            try:
                await cluster.publish(subject, payload)
                return True
            except Exception as e:
                attempt += 1
                logger.warning("Publish fail %s attempt %s: %s", cluster.name, attempt, e)
                await asyncio.sleep(min(2 ** attempt, 10))
        return False

    async def _worker(self, worker_id: int) -> None:
        """
        Worker task that consumes from queue and publishes to NATS.

        Args:
            worker_id (int): Unique worker identifier (for debugging/logging).
        """
        while True:
            envelope = await self.queue.get()
            if envelope is None:
                self.queue.task_done()
                break

            topic = envelope.get("topic")
            partition = envelope.get("partition")
            offset = envelope.get("offset")
            key = envelope.get("key")
            value = envelope.get("value")

            logger.debug(
                "Worker[%d] processing message %s:%s:%s",
                worker_id, topic, partition, offset
            )

            if not key:
                logger.warning("Message without key at %s:%s:%s", topic, partition, offset)
                self.queue.task_done()
                continue

            # Determine asset UUID and cluster
            asset_uuid = key if isinstance(key, (bytes, bytearray)) else str(key).encode()
            cluster_name = self.hash_ring.get(asset_uuid)
            cluster = self.nats_clusters[cluster_name]

            # Parse payload once
            try:
                payload_json = value if isinstance(value, (dict, list)) else json.loads(value.decode())
                message_id = payload_json.pop("ID", "unknown")  # remove ID for payload
            except Exception as e:
                logger.warning("Failed to parse message payload: %s", e)
                payload_json = {}
                message_id = "unknown"

            # Build subject
            subject = f"{asset_uuid.decode(errors='ignore')}.{message_id}"

            # Ensure payload is bytes
            payload_bytes = (
                json.dumps(payload_json).encode()
                if not isinstance(payload_json, (bytes, bytearray))
                else payload_json
            )

            # Publish with retry
            ok = await self._publish_with_retry(cluster, subject, payload_bytes)

            # Commit Kafka offset if publish succeeded
            if ok and self.consumer:
                try:
                    tp = TopicPartition(topic, partition, offset + 1)
                    self.consumer.commit(offsets=[tp], asynchronous=False)
                except Exception as e:
                    logger.error("Commit failed: %s", e)

            self.queue.task_done()

    # -------------------------
    # Control flow
    # -------------------------
    async def start(self) -> None:
        """ Start the forwarder: launch Kafka consumer and worker tasks. """
        logger.info("Starting forwarder")
        self._loop = asyncio.get_running_loop()
        self.start_consumer_thread()

        # Preconnect to NATS clusters
        for cluster in self.nats_clusters.values():
            try:
                await cluster.connect()
            except Exception as e:
                logger.warning("NATS connect failed: %s", e)

        workers = [asyncio.create_task(self._worker(i)) for i in range(self.concurrency)]

        # Run until stopped
        while not self._stop_event.is_set():
            await asyncio.sleep(0.5)

        # Graceful shutdown
        for _ in workers:
            await self.queue.put(None)
        await self.queue.join()
        for w in workers:
            w.cancel()

        for cluster in self.nats_clusters.values():
            await cluster.close()

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)

    def stop(self) -> None:
        """ Signal the forwarder to stop gracefully. """
        self._stop_event.set()
        try:
            if self.consumer:
                self.consumer.wakeup()
        except Exception:
            pass
