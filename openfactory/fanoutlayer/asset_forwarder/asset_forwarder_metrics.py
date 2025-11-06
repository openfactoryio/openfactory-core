from prometheus_client import Info, Counter, Histogram

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

# Processing latency in worker (Kafka → NATS)
MESSAGE_PROCESSING_LATENCY = Histogram(
    "asset_forwarder_message_processing_latency_seconds",
    "Time taken by a worker to process and forward a single message",
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, float("inf")),
    labelnames=("forwarder",),
)

# Queue backlog
QUEUE_SIZE_HISTOGRAM = Histogram(
    "asset_forwarder_queue_size",
    "Distribution of messages in the queue",
    buckets=[0, 1, 2, 5, 10, 20, 50, 100, 500, float("inf")],
    labelnames=("forwarder",),
)
