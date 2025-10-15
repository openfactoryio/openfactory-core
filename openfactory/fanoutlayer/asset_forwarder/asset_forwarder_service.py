"""
OpenFactory Asset Forwarder Service

- Consumes from Kafka topic "ofa_assets" keyed by ASSET_UUID
- Routes messages to NATS clusters based on consistent hash of ASSET_UUID
- Runs as a Docker Swarm service
- Stateless, horizontally scalable

Configuration via environment variables:

Kafka:
  KAFKA_BROKER               e.g. "kafka:9092"
  KAFKA_GROUP                (default: "ofa_fan_out_layer_group")
  KAFKA_TOPIC                (default: "ofa_assets")
  KAFKA_AUTO_OFFSET_RESET    (default: "earliest")

NATS clusters (repeatable):
  NATS_CLUSTER_<NAME>=url1,url2
  Example:
    NATS_CLUSTER_C1="nats://nats-c1-1:4222,nats://nats-c1-2:4222"
    NATS_CLUSTER_C2="nats://nats-c2-1:4222"

Forwarder:
  ASSET_FORWARDER_LOG_LEVEL             (default: INFO)
  ASSET_FORWARDER_QUEUE_SIZE            (default: 10000)
  ASSET_FORWARDER_CONCURRENCY           (default: 20)
  ASSET_FORWARDER_MAX_RETRIES           (default: 5)
  HASH_RING_REPLICAS                    (default: 128)
"""

import asyncio
import os
import signal
import sys
from typing import Optional
from prometheus_client import start_http_server
from .logger import logger
from .asset_forwarder import AssetForwarder
from utils.parse_nats_clusters import parse_nats_clusters


class AssetForwarderService:
    """
    Service wrapper for running the Asset Forwarder in Docker Swarm.

    This class is responsible for:
        1. Parsing environment variables for Kafka and NATS configuration.
        2. Instantiating the AssetForwarder.
        3. Running the forwarder and handling graceful shutdown signals.

    Designed for stateless operation in a containerized environment.
    """

    def __init__(self) -> None:
        """ Initialize the service wrapper. """
        self.forwarder: Optional[AssetForwarder] = None

    def build_forwarder(self) -> None:
        """
        Construct the AssetForwarder using environment variables.

        Required environment variables:
            KAFKA_BROKER: Kafka bootstrap server(s)
            NATS_CLUSTER_<NAME>: NATS cluster URLs
        Optional environment variables:
            KAFKA_GROUP: Kafka consumer group (default: "ofa_fan_out_layer_group")
            KAFKA_TOPIC: Kafka topic (default: "ofa_assets")
            KAFKA_AUTO_OFFSET_RESET: (default: "earliest")
            ASSET_FORWARDER_QUEUE_SIZE: queue size (default: 10000)
            ASSET_FORWARDER_CONCURRENCY: number of async workers (default: 20)
            ASSET_FORWARDER_MAX_RETRIES: max retries for NATS publish (default: 5)
            HASH_RING_REPLICAS: consistent hash replicas (default: 128)
        """
        logger.info('Building forwarder')
        kafka_broker = os.environ["KAFKA_BROKER"]

        kafka_group = os.getenv("KAFKA_GROUP", "ofa_fan_out_layer_group")
        kafka_topic = os.getenv("KAFKA_TOPIC", "ofa_assets")

        kafka_conf = {
            "bootstrap.servers": kafka_broker,
            "group.id": kafka_group,
            "enable.auto.commit": False,
            "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        }

        nats_clusters = parse_nats_clusters()

        self.forwarder = AssetForwarder(
            kafka_config=kafka_conf,
            kafka_topic=kafka_topic,
            nats_clusters=nats_clusters,
            group_id=kafka_group,
            queue_maxsize=int(os.getenv("ASSET_FORWARDER_QUEUE_SIZE", "10000")),
            nats_publish_concurrency=int(os.getenv("ASSET_FORWARDER_CONCURRENCY", "20")),
            max_retries=int(os.getenv("ASSET_FORWARDER_MAX_RETRIES", "5")),
            consistent_replicas=int(os.getenv("HASH_RING_REPLICAS", "128")),
        )

    async def run(self) -> None:
        """
        Run the AssetForwarder and handle graceful shutdown signals.

        This method:
            1. Builds the forwarder if not already built.
            2. Sets up signal handlers for SIGINT and SIGTERM.
            3. Starts the forwarder and waits until a termination signal is received.
        """
        if not self.forwarder:
            self.build_forwarder()

        loop = asyncio.get_running_loop()
        stop = asyncio.Event()

        def _signal_handler() -> None:
            logger.info("Received termination signal")
            self.forwarder.stop()
            stop.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                # Fallback for Windows or restricted environments
                signal.signal(sig, lambda *_: _signal_handler())

        await self.forwarder.start()
        await stop.wait()


# -------------------------
# Entrypoint
# -------------------------
def main():
    # Start Prometheus metrics server on port 8000
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics available at :{metrics_port}/metrics")

    service = AssetForwarderService()
    try:
        asyncio.run(service.run())
    except Exception as e:
        logger.error("Service crashed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
