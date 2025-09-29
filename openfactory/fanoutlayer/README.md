# OpenFactory Asset Fan-out Layer Architecture

The **Asset Fan-out Layer** is responsible for delivering live asset data from the Kafka-based backend to many clients in a scalable and reliable manner.

It ensures:  
- **Scalability with assets (N)** via Kafka partitions and forwarders
- **Scalability with clients (M)** via NATS clusters
- **Consistent routing**: each asset’s messages always go to the same NATS cluster, even if forwarders rebalance
- **Fault tolerant by design**: handles Kafka broker, Asset forwarder, and NATS node failures transparently

## Architecture Diagram

```
                        Kafka Cluster
                  +------------------------+
                  | Topic: ofa_assets      |
                  | Keys: ASSET_UUID (N)   |
                  +------------------------+
                              |
        +--------------+-------------+-------------+
        |              |             |             |
      Asset          Asset         Asset         Asset
   Forwarder 1    Forwarder 2   Forwarder 3   Forwarder 4
   (subset F1)    (subset F2)   (subset F3)   (subset F4)
  (can read any  (can read any (can read any (can read any
    partition)     partition)    partition)    partition)
                              |
               +-----------------------------+
               |    Hash-based routing by    |
               |  ASSET_UUID → NATS cluster  |
               +-----------------------------+
                             |
                   +---------+---------+
                   |                   |
        +-----------------+     +-----------------+
        | NATS Cluster C1 |     | NATS Cluster C2 |
        |  +----------+   |     |  +----------+   |
        |  | Node 1   |   |     |  | Node 1   |   |
        |  | Node 2   |   |     |  | Node 2   |   |
        |  +----------+   |     |  +----------+   |
        +-----------------+     +-----------------+
                |                       |
         Clients subscribe       Clients subscribe
          to assets with          to assets with
        ASSET_UUID % 2 = 0      ASSET_UUID % 2 = 1
```

## How It Works

1. **Asset Forwarders**
   - Consume subsets of partitions from the Kafka `ofa_assets` topic
   - Stateless: can handle any partition; rebalancing is transparent
   - Horizontally scalable to handle large number of assets

2. **Hash-based Routing**
   - Hash-based routing ensures all messages for a given `ASSET_UUID` go to the same NATS cluster
   - Enables consistent client subscriptions

3. **NATS Clusters**
   - Each cluster handles messages for a subset of assets
   - Stateless nodes: any node can handle clients and forward messages
   - Horizontally scalable to handle large numbers of assets (by adding more clusters) and clients (by adding more nodes per cluster)

4. **Clients**
   - Connect to the appropriate NATS cluster
   - Receive real-time updates only for the assets they are interested in

This architecture allows **OpenFactory** to scale both with **millions of assets** and **millions of subscribing clients**, while keeping the fan-out layer efficient and predictable.

## Deployment Considerations

While the Asset Fan-out Layer is stateless and horizontally scalable, proper deployment is critical to ensure high availability:

- **Asset Forwarders**
  - Stateless, but multiple forwarders should be distributed across different Swarm hosts of the OpenFactory cluster
  - Avoid placing all forwarders that consume the same Kafka partitions on a single host
  - This prevents a single host failure from taking down a large portion of the forwarding capacity

- **NATS Clusters**
  - Each cluster’s nodes must be distributed across different Swarm hosts of the OpenFactory cluster
  - Avoid placing all nodes of a single cluster on the same host
  - Ensures the cluster continues functioning if a host fails or is updated
  - Clients can reconnect to any surviving node without losing transient messages

> **Rule of thumb:** Spread stateless components (forwarders and NATS nodes) across multiple physical or virtual hosts to maintain continuous operation under node failures or updates.
