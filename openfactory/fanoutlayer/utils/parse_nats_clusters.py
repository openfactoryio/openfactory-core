import os
from typing import Dict, List


def parse_nats_clusters() -> Dict[str, List[str]]:
    """
    Parse NATS cluster URLs from environment variables.

    Environment variables must be prefixed with `NATS_CLUSTER_`, e.g.,
    `NATS_CLUSTER_C1="nats://nats-c1-1:4222,nats://nats-c1-2:4222"`.

    Returns:
        Dict[str, List[str]]: Mapping from cluster name to list of server URLs.

    Raises:
        RuntimeError: If no NATS clusters are configured.
    """
    nats_clusters: Dict[str, List[str]] = {}
    for key, value in os.environ.items():
        if key.startswith("NATS_CLUSTER_"):
            name = key[len("NATS_CLUSTER_"):]
            servers = [s.strip() for s in value.split(",") if s.strip()]
            if servers:
                nats_clusters[name] = servers

    if not nats_clusters:
        raise RuntimeError("No NATS clusters configured")

    return nats_clusters
