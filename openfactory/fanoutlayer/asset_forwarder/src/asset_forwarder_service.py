"""
This module implements the AssetForwarderService, an OpenFactory
application responsible for forwarding asset updates from Kafka to
NATS clusters.

The service consumes messages from the configured Kafka topic,
enriches them with forwarding metadata, determines the target
NATS cluster using consistent hashing of the asset UUID, and
publishes the message to the corresponding NATS subject.

Architecture
------------

The service is built on OpenFactoryFastAPIApp and provides:

- Kafka consumer integration.
- NATS cluster management.
- Consistent hash based routing.
- Prometheus metrics.
- Health and lifecycle management.
- Asynchronous worker processing.

Message Flow
------------

1. Consume asset messages from Kafka.
2. Decode and validate payloads.
3. Add Kafka and forwarder timestamps.
4. Queue messages for worker processing.
5. Determine destination NATS cluster.
6. Publish to the target NATS subject.
7. Store Kafka offsets after successful handling.

Routing
-------

Assets are routed using a ConsistentHashRing based on the
Kafka message key (asset UUID). This ensures stable routing
of a given asset to the same NATS cluster while minimizing
redistribution when clusters are added or removed.

Observability
-------------

The service exposes Prometheus metrics including:

- Kafka consumption rate.
- Kafka consumer errors.
- Invalid message count.
- NATS publish success rate.
- NATS publish failure rate.
- Queue backlog.
- Queue waiting time.
- Message processing latency.

The Prometheus endpoint is exposed through the OpenFactory
application framework.
"""

import os
import asyncio
import json
from datetime import datetime, timezone
from typing import Optional, Any
from confluent_kafka import Consumer, KafkaError, TopicPartition, Message
from openfactory.apps import OpenFactoryFastAPIApp
from openfactory.kafka import KSQLDBClient
from openfactory.fanoutlayer.utils.hash_ring import ConsistentHashRing
from openfactory.fanoutlayer.utils.parse_nats_clusters import parse_nats_clusters
from . import asset_forwarder_metrics as forwarder_metrics
from .nats_cluster import NatsCluster


PROMETHEUS_METRICS_PATH = "/metrics"


class AssetForwarderService(OpenFactoryFastAPIApp):

    def __init__(self, *args, **kwargs):
        """
        Initialize the AssetForwarderService.

        This constructor forwards all parameters to
        :class:`OpenFactoryFastAPIApp <openfactory.apps.ofa_fastapi_app.OpenFactoryFastAPIApp>`

        Args:
            ksqlClient: KSQL client instance.
            bootstrap_servers: Kafka bootstrap server address.
            asset_router_url: Asset Router URL.
            loglevel: Logging level (e.g., ``INFO``, ``DEBUG``).
            test_mode: Enables test mode (disables live Kafka/ksql interaction).

        See also:
            :class:`OpenFactoryFastAPIApp <openfactory.apps.ofa_fastapi_app.OpenFactoryFastAPIApp>` for full initialization
            details and environment variable handling.
        """
        super().__init__(*args, **kwargs)

        # forwarder queue
        self.queue = asyncio.Queue(maxsize=int(os.getenv("ASSET_FORWARDER_QUEUE_SIZE", "10000")))

        # setup NATS clusters
        self.setup_nats_clusters()

        # setup Kafka consumer
        self.consumer: Optional[Consumer] = None
        self.setup_consumer()

        forwarder_metrics.BUILD_INFO.info({
            "version": os.environ.get('APPLICATION_VERSION', 'UNKNOWN'),
            "swarm_node": os.environ.get('NODE_HOSTNAME', 'unknown'),
        })

        # Expose Prometheus metrics
        self.api.get(PROMETHEUS_METRICS_PATH)(forwarder_metrics.metrics_endpoint)

    def setup_consumer(self) -> None:
        """
        Build the Kafka consumer.

        Reads consumer configuration from environment variables and
        initializes the underlying confluent-kafka Consumer.

        Environment variables:
            KAFKA_TOPIC: Kafka topic to consume from.
            KAFKA_GROUP: Kafka consumer group.
            KAFKA_CONSUMER_BATCH_SIZE: Maximum messages to consume per poll.
            KAFKA_CONSUMER_TIME_OUT_MS: Poll timeout in milliseconds.
            KAFKA_CONSUMER_COMMIT_INTERVAL_MS: Auto commit interval.
            KAFKA_AUTO_OFFSET_RESET: Offset reset policy.

        Raises:
            KeyError: If KAFKA_BROKER is not defined.
        """
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "ofa_assets")
        self.kafka_group = os.getenv("KAFKA_GROUP", "ofa_fan_out_layer_group")

        self.KAFKA_CONSUMER_BATCH_SIZE = int(os.getenv("KAFKA_CONSUMER_BATCH_SIZE", "100"))
        self.KAFKA_CONSUMER_TIME_OUT_MS = float(os.getenv("KAFKA_CONSUMER_TIME_OUT_MS", "1"))

        self.kafka_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.kafka_group,
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "auto.commit.interval.ms": float(os.getenv("KAFKA_CONSUMER_COMMIT_INTERVAL_MS", "100")),
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
        }

        self.consumer = Consumer(self.kafka_config)

    def setup_nats_clusters(self) -> None:
        """
        Build NATS cluster connections and hash ring.

        Discovers NATS clusters from environment variables,
        creates NatsCluster instances, and initializes the
        consistent hash ring used for routing assets.
        """

        nats_clusters = parse_nats_clusters()
        self.nats_clusters = {
            name: NatsCluster(name, servers, self.logger)
            for name, servers in nats_clusters.items()
        }

        self.hash_ring = ConsistentHashRing(
            list(self.nats_clusters.keys()),
            replicas=int(os.getenv("HASH_RING_REPLICAS", "128"))
        )

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Handle Kafka partition assignment and register metrics.

        Called by the Kafka consumer during a rebalance when partitions
        are assigned to this consumer instance.
        """
        self.logger.info("Assigned: %s", partitions)

        if not partitions:
            self.logger.critical("Kafka consumer was assigned zero partitions. "
                                 "Are there too many forwarders running comapred to topic partions ?")
            os._exit(1)

        consumer.assign(partitions)

        # register Prometheus metrics
        self.register_prometheus_metrics(metrics_port=4000, metrics_path=PROMETHEUS_METRICS_PATH)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Handle Kafka partition revocation.

        Called by the Kafka consumer during a rebalance before partitions
        are revoked from this consumer instance. Pending offsets are
        committed before ownership is transferred to another consumer.
        """
        self.logger.info("Revoked: %s", partitions)
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass

    def decode_message_value(self, raw_value: bytes | bytearray | str) -> dict[str, Any]:
        """
        Decode a Kafka message payload from JSON.

        Args:
            raw_value: Raw payload returned by Kafka.

        Returns:
            Parsed JSON payload as a dictionary.

        Raises:
            ValueError: If the payload type is unsupported.
            json.JSONDecodeError: If the payload is not valid JSON.
        """
        if isinstance(raw_value, (bytes, bytearray)):
            value = json.loads(raw_value.decode("utf-8"))
        elif isinstance(raw_value, str):
            value = json.loads(raw_value)
        else:
            raise ValueError("Unsupported payload type")
        return value

    def add_timestamps(self, value: dict[str, Any], msg: Message) -> dict[str, Any]:
        """
        Add Kafka and forwarder timestamps to a message payload.

        Args:
            value: Decoded message payload.
            msg: Kafka message.

        Returns:
            Updated payload.
        """

        ts_type, ts = msg.timestamp()

        attrs = value.setdefault("attributes", {})
        attrs["asset_forwarder_timestamp"] = (datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")

        if ts is not None:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            attrs["kafka_timestamp"] = (dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z")

            if ts_type == 1:
                attrs["kafka_timestamp_type"] = "producer"
            elif ts_type == 2:
                attrs["kafka_timestamp_type"] = "broker"
            else:
                attrs["kafka_timestamp_type"] = "unknown"

        return value

    def build_nats_message(self, msg: Message, value: dict[str, Any]) -> tuple[NatsCluster, str, bytes]:
        """
        Determine the target NATS cluster and build a NATS message.

        Args:
            msg: Kafka message.
            value: Decoded message payload.

        Returns:
            Tuple containing:
                - Target NATS cluster
                - NATS subject
                - Serialized payload

        Raises:
            ValueError: If the asset UUID is missing or the message does not contain a valid ID field.
        """

        key = msg.key()
        if key is None:
            raise ValueError("Asset UUID missing")
        asset_uuid = key if isinstance(key, (bytes, bytearray)) else str(key).encode()

        cluster_name = self.hash_ring.get(asset_uuid)
        cluster = self.nats_clusters[cluster_name]

        id_key = next((k for k in value.keys() if k.lower() == "id"), None)
        message_id = value.pop(id_key, None) if id_key else None

        if not message_id:
            raise ValueError("ID missing")

        subject = f"{asset_uuid.decode(errors='ignore')}.{message_id}"
        payload_bytes = json.dumps(value).encode()

        return cluster, subject, payload_bytes

    async def worker(self) -> None:
        """
        Process queued Kafka messages and forward them to NATS.

        The worker validates messages, publishes them to the
        appropriate NATS cluster, updates Prometheus metrics,
        and stores Kafka offsets after successful processing.
        """
        self.logger.debug("Started Worker process")

        while True:
            item = await self.queue.get()
            processing_start = asyncio.get_running_loop().time()

            try:
                queue_time = asyncio.get_running_loop().time() - item["queued_at"]
                forwarder_metrics.QUEUE_TIME_SECONDS.labels(forwarder=self.asset_uuid).observe(queue_time)
                forwarder_metrics.QUEUE_SIZE_HISTOGRAM.labels(forwarder=self.asset_uuid).observe(self.queue.qsize())

                msg = item["msg"]
                value = item["value"]
                self.logger.debug(f"Worker id={id(msg)} partition={msg.partition()} offset={msg.offset()} key={msg.key()} {value.get("ID")}")

                try:
                    cluster, subject, payload = self.build_nats_message(msg, value)
                except Exception:
                    self.logger.warning("Invalid message. Skipping.", exc_info=True)
                    self.consumer.store_offsets(message=msg)
                    forwarder_metrics.INVALID_MESSAGES.labels(forwarder=self.asset_uuid).inc()
                    continue

                # Publish to NATS
                try:
                    await cluster.publish(subject, payload)
                except Exception:
                    self.logger.exception("Failed to publish to NATS")
                    forwarder_metrics.NATS_PUBLISH_FAILURES.labels(cluster=cluster.name).inc()
                    continue

                self.consumer.store_offsets(message=msg)
                forwarder_metrics.NATS_MESSAGES_PUBLISHED.labels(cluster=cluster.name).inc()

            except Exception:
                self.logger.exception("Unexpected worker failure")
                os._exit(1)

            finally:
                try:
                    processing_time = asyncio.get_running_loop().time() - processing_start
                    forwarder_metrics.MESSAGE_PROCESSING_LATENCY.labels(forwarder=self.asset_uuid).observe(processing_time)
                except Exception:
                    self.logger.exception("Failed to update processing latency metric")
                self.queue.task_done()

    async def async_main_loop(self) -> None:
        """
        Main service loop.

        Starts worker tasks, subscribes to Kafka, consumes messages,
        and queues them for asynchronous processing.
        """

        # worker task
        self.worker_task = asyncio.create_task(self.worker())

        def worker_done(task: asyncio.Task) -> None:
            """
            Log unexpected worker task failures.

            Args:
                task: Completed worker task.
            """
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                return
            if exc:
                self.logger.exception("Worker task crashed", exc_info=exc)

        self.worker_task.add_done_callback(worker_done)

        # Kafka subscription
        self.consumer.subscribe(
            [self.kafka_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        while True:
            msgs = await asyncio.to_thread(
                self.consumer.consume,
                num_messages=self.KAFKA_CONSUMER_BATCH_SIZE,
                timeout=self.KAFKA_CONSUMER_TIME_OUT_MS / 1000.0,
            )

            for msg in msgs:

                forwarder_metrics.KAFKA_MESSAGES_CONSUMED.labels(forwarder=self.asset_uuid).inc()

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    err = msg.error()
                    self.logger.error(
                        "Kafka error: code=%s name=%s fatal=%s retriable=%s message=%s",
                        err.code(),
                        err.name(),
                        err.fatal(),
                        err.retriable(),
                        err,
                    )
                    forwarder_metrics.KAFKA_ERRORS.labels(forwarder=self.asset_uuid).inc()

                    if err.fatal():
                        self.logger.critical("Fatal Kafka error: %s (name=%s)", err, err.name())
                        os._exit(1)

                    continue

                value = self.decode_message_value(msg.value())
                value = self.add_timestamps(value, msg)
                self.logger.debug(f"Main id={id(msg)}   partition={msg.partition()} offset={msg.offset()} key={msg.key()} {value.get("ID")}")
                await self.queue.put({
                    "msg": msg,
                    "value": value,
                    "queued_at": asyncio.get_running_loop().time(),
                })
                forwarder_metrics.QUEUE_SIZE.labels(forwarder=self.asset_uuid).set(self.queue.qsize())


if __name__ == "__main__":
    app = AssetForwarderService(
        ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        loglevel=os.getenv("LOG_LEVEL", "INFO")
    )

    app.run()
