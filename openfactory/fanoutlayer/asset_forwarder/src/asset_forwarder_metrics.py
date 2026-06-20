"""
Prometheus metrics for the Asset Forwarder service.

Exported metrics
----------------

Build and service information:

* ``BUILD_INFO`` - Build information for the running service.

Kafka metrics:

* ``KAFKA_MESSAGES_CONSUMED`` - Total number of messages consumed from Kafka.
* ``KAFKA_ERRORS`` - Total number of Kafka consumer errors.
* ``INVALID_MESSAGES`` - Total number of invalid Kafka messages consumed.

NATS metrics:

* ``NATS_MESSAGES_PUBLISHED`` - Total number of messages successfully published to NATS.
* ``NATS_PUBLISH_FAILURES`` - Total number of failed publish attempts to NATS.

Queue metrics:

* ``QUEUE_TIME_SECONDS`` - Time messages spend waiting in the forwarder queue before processing.
* ``MESSAGE_PROCESSING_LATENCY`` - End-to-end worker processing latency.
* ``QUEUE_SIZE`` - Current number of messages waiting in the queue.
* ``QUEUE_SIZE_HISTOGRAM`` - Distribution of queue backlog size.

The ``metrics_endpoint()`` function exposes all registered metrics in
Prometheus text exposition format.
"""

from prometheus_client import Info, Counter, Histogram, Gauge
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

# Build info
BUILD_INFO = Info(
    "fan_out_layer_forwarder_build",
    "Build information for the Fan-out Layer Forwarder"
)

# Messages processed from Kafka
KAFKA_MESSAGES_CONSUMED = Counter(
    "asset_forwarder_kafka_messages_consumed_total",
    "Total number of messages consumed from Kafka",
    ["forwarder"]
)

# Kafka consumer errors
KAFKA_ERRORS = Counter(
    "asset_forwarder_kafka_errors_total",
    "Total number of Kafka errors from consumer",
    ["forwarder"]
)

# Invalid Kafka messages
INVALID_MESSAGES = Counter(
    "asset_forwarder_invalid_messages_total",
    "Total number of invalid Kafka messages consumed",
    ["forwarder"]
)

# Successful NATS publications
NATS_MESSAGES_PUBLISHED = Counter(
    "asset_forwarder_nats_messages_published_total",
    "Total number of messages successfully published to NATS",
    ["cluster"]
)

# NATS publish failures
NATS_PUBLISH_FAILURES = Counter(
    "asset_forwarder_nats_publish_failures_total",
    "Total number of failed publish attempts to NATS",
    ["cluster"]
)

# Time spent in waiting queue
QUEUE_TIME_SECONDS = Histogram(
    "asset_forwarder_queue_time_seconds",
    "Time messages spend waiting in the forwarder queue before being processed by a worker",
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, float("inf")),
    labelnames=("forwarder",),
)

# Processing latency in worker (Kafka to NATS)
MESSAGE_PROCESSING_LATENCY = Histogram(
    "asset_forwarder_message_processing_latency_seconds",
    "Time taken by a worker to process and forward a single message",
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, float("inf")),
    labelnames=("forwarder",),
)

# Current queue backlog
QUEUE_SIZE = Gauge(
    "asset_forwarder_queue_size",
    "Current number of messages waiting in the queue",
    ["forwarder"]
)

# Queue backlog
QUEUE_SIZE_HISTOGRAM = Histogram(
    "asset_forwarder_queue_size_histogram",
    "Distribution of messages in the queue",
    buckets=[0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2500, 5000, 10000, float("inf")],
    labelnames=("forwarder",),
)


def metrics_endpoint() -> Response:
    """
    Return Prometheus metrics in the text exposition format.

    Returns:
        Response: Prometheus metrics response.
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
