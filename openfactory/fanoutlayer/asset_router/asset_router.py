"""
Asset Router Service
====================

This service exposes a lightweight HTTP API that maps asset IDs to NATS clusters
using a consistent hash ring. It supports multiple NATS clusters and provides
deterministic routing for asset data.

Environment Variables
---------------------
- NATS_CLUSTER_<NAME> : Comma-separated URLs for a NATS cluster.
  Example: NATS_CLUSTER_C1="nats://nats-c1-1:4222,nats://nats-c1-2:4222"
- HASH_RING_REPLICAS   : Number of virtual nodes per physical node (default: 128)
"""

import os
from typing import Dict
from fastapi import FastAPI, HTTPException
from openfactory.fanoutlayer.utils.hash_ring import ConsistentHashRing
from openfactory.fanoutlayer.utils.parse_nats_clusters import parse_nats_clusters

app = FastAPI(title="Asset Router for NATS")


# ---------------------------------------------------------------------------------------
# Factory to create routing state
# ---------------------------------------------------------------------------------------
def _create_hash_ring() -> tuple[ConsistentHashRing, dict[str, list[str]]]:
    """
    Create a consistent hash ring from the configured NATS clusters.

    Returns:
        tuple: (ConsistentHashRing instance, clusters dict)
    """
    clusters = parse_nats_clusters()
    replicas = int(os.environ.get("HASH_RING_REPLICAS", 128))
    return ConsistentHashRing(nodes=list(clusters.keys()), replicas=replicas), clusters


# ---------------------------------------------------------------------------------------
# Initialize hash ring and clusters
# ---------------------------------------------------------------------------------------
ring, nats_clusters = _create_hash_ring()


@app.get("/asset/{asset_id}")
def get_asset_nats_route(asset_id: str) -> Dict[str, str]:
    """
    Return the NATS cluster URL responsible for the given asset ID.

    Args:
        asset_id (str): Unique asset identifier.

    Returns:
        Dict[str, str]: Dictionary containing the asset_id, cluster_name,
        and a NATS server URL to connect to.

    Raises:
        HTTPException: If routing fails for any reason.
    """
    try:
        cluster_name = ring.get(asset_id.encode())
        nats_url = nats_clusters[cluster_name][0]  # Return first server in cluster
        return {"asset_id": asset_id, "cluster_name": cluster_name, "nats_url": nats_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------------------
# Entry point
#
# Run it with (from the project root)
#
#  python -m openfactory.fanoutlayer.asset_router.asset_router
#
# ---------------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "openfactory.fanoutlayer.asset_router.asset_router:app",
        host="0.0.0.0",
        port=8002,
    )
