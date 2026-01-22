# OpenFactory Architecture

This document provides a **shared architectural understanding** of OpenFactory for developers, integrators, and operators.
It focuses on core design principles, data flow, and the main building blocks of the platform.
High-level vision, asset semantics, and deployment perspectives are described in separate overview documentation.

---

## Architectural Principles

### Event streaming

All data in OpenFactory is handled as **streams of events**.
Events are immutable, append-only, and time-ordered.

Each event represents a **time-stamped observation or state change** related to a single asset.
Streams are the primary integration primitive of the platform; all higher-level behavior is derived from them.

---

### Kappa architecture

OpenFactory follows a **Kappa architecture**:

- a single streaming path
- no separate batch layer
- all state is derived from streams

Kafka is the **only system of record**.
All states, views, and derived representations can be rebuilt from the streams at any time.

---

### Assets as first-class entities

The core abstraction in OpenFactory is the **asset**, in the sense of **RAMI 4.0**.

Assets represent physical or logical entities in the manufacturing environment and are the primary unit of identity throughout the platform.
All data, state, and control logic is expressed in relation to assets.

---

### No point-to-point integrations

Components in OpenFactory do not communicate directly with each other.

All interactions—including telemetry, derived state, and control actions—happen through **streams**, ensuring:

- loose coupling
- independent scaling
- clear responsibility boundaries between components
- full traceability and replayability of system behavior

---

## High-Level Data Flow

1. Physical devices emit data
2. Data is ingested through protocol-specific gateways (also referred to as *connectors* in higher-level documentation)
3. Normalized events are written to the asset stream in Kafka
4. Asset states are computed from streams
5. Data is served to users and applications through a scalable fan-out layer

---

## Core Concepts

### Asset stream

- All assets are represented in a **single Kafka topic** (the *asset stream*)
- Each asset corresponds to a **key** in that topic
- Events are **normalized at ingestion time** by the gateways

This design ensures:

- strict ordering per asset
- a simple and uniform data model
- scalable and deterministic stream processing

---

### State computation

Asset states are computed using **ksqlDB** and stream-processing services.

These states are:

- derived from the asset stream
- reproducible at any time
- not considered the system of record

They exist solely to support querying, monitoring, control logic, and higher-level applications.

---

## Main Components

```{mermaid}
flowchart TB
    %% =====================
    %% Physical Layer
    %% =====================
    Device1["**Physical Devices**"]
    Device2["**Sensors / Actuators**"]
    Device3["**Applications**"]

    %% =====================
    %% Ingestion Edge / Gateways
    %% =====================
    Gateway["**Gateways / Connectors**<br>(protocol-specific,<br>xN replicas)"]

    %% =====================
    %% Streaming Backbone
    %% =====================
    Streaming_Backbone["**Streaming Backbone**
    Kafka (Asset Stream)
    ksqlDB (Derived State)
    Stream Processing Apps"]

    %% =====================
    %% Serving Layer
    %% =====================
    Forwarder["**Forwarders**<br>(partition-aligned,<br>xM replicas)"]
    NATS["**NATS Cluster**<br>Fan-out"]

    %% =====================
    %% Consumers / Users
    %% =====================
    UserApps["**User Apps**<br>Dashboards / APIs"]

    %% =====================
    %% Connections / Data Flow
    %% =====================
    Device1 --> Gateway
    Device2 --> Gateway
    Device3 --> Gateway

    Gateway --> Streaming_Backbone
    Streaming_Backbone --> Forwarder
    Forwarder --> NATS
    NATS --> UserApps
```

### Gateways

Gateways connect physical devices to OpenFactory.

- Each gateway supports a **single communication protocol** (e.g. OPC UA)
- A gateway manages a **set of assigned devices**
- Gateways are **stateless** and can be replicated for horizontal scalability
- Gateways push normalized events directly into Kafka
- Gateways do not retain historical data or compute long-term state

Gateways form the ingestion edge of the platform and isolate protocol-specific concerns from the core architecture.

---

### Streaming platform

The streaming platform consists of:

- **Kafka** for event storage, ordering, and distribution
- **ksqlDB** for stream processing and state computation

This layer forms the backbone of OpenFactory and is the authoritative source of all data.

---

### Serving layer

The serving layer exposes asset data to consumers.

- Forwarders consume from the Kafka asset stream
- Forwarders are replicated to match the number of topic partitions
- Data is forwarded to a **NATS cluster**
- NATS provides a scalable fan-out mechanism for user-facing consumption

This decouples **user access patterns** from the streaming backbone and prevents Kafka from becoming a direct user-facing API.
It enables fine-grained subscriptions to asset-level data without impacting core stream processing.

---

## Scalability Model

OpenFactory scales horizontally at every layer:

- gateways scale by replication
- Kafka scales via topic partitions
- forwarders scale with Kafka partitions
- user consumption scales through NATS fan-out

Each layer can be scaled independently based on workload characteristics and usage patterns.
