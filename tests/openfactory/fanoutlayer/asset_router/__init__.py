# Ensure parse_nats_clusters finds at least one cluster for all tests (otherwise imports will fail)
import os

os.environ.setdefault("NATS_CLUSTER_C1", "nats://localhost:4222")
