from prometheus_client import Counter, Gauge

# Messages processed from Kafka
KAFKA_MESSAGES_CONSUMED = Counter(
    "asset_forwarder_kafka_messages_consumed_total",
    "Total number of messages consumed from Kafka",
    ["topic"]
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

# Processing latency (Kafka → NATS)
MESSAGE_PROCESSING_LATENCY = Gauge(
    "asset_forwarder_message_processing_latency_seconds",
    "Time taken to process and forward a single message",
    ["cluster"]
)

# Queue backlog
QUEUE_SIZE = Gauge(
    "asset_forwarder_queue_size",
    "Current number of messages waiting in the queue"
)
