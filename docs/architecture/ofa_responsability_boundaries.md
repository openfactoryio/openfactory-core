# OpenFactory Architectural Responsibility Model

## A Data-Centric, Streaming-First Industrial Runtime Framework

OpenFactory is a modular software framework designed to build and operate data-centric, event-driven industrial systems. It virtualizes physical assets and orchestrates their behavior through a persistent streaming backbone, enabling real-time monitoring, analytics, and control within a Kappa architecture.

This document formalizes the architectural responsibility boundaries of OpenFactory. It clarifies which concerns are owned by OpenFactory and which are deliberately delegated to external infrastructure and platform layers. These boundaries ensure long-term architectural clarity, scalability, and maintainability.

OpenFactory orchestrates **industrial semantics and services**, not **infrastructure primitives**.

## Architectural Context

Industrial digital systems can be structured into four logical layers:

1. **Physical Asset Layer**
   Machines, sensors, actuators, and controllers.

2. **Streaming Backbone Layer**
   Persistent event streams enabling ordered, replayable data flow.

3. **Semantic Industrial Runtime Layer (OpenFactory)**
   Asset virtualization, industrial logic, stream processing, and control loops.

4. **Infrastructure & Platform Layer**
   Compute, container runtime, networking, storage, and orchestration substrate.

OpenFactory operates in the **semantic industrial runtime layer**, leveraging the streaming backbone and infrastructure layers without assuming ownership of their internal mechanics.

## Foundational Principle

OpenFactory owns:

> Industrial semantics, asset virtualization, stream-based control logic, service composition, and system-level resilience through architectural design.

OpenFactory does not own:

> Infrastructure provisioning, transport guarantees, hardware lifecycle management, or low-level platform security.

This principle governs all feature evolution and architectural decisions.

## Layered Responsibility Model

### Physical Asset Layer (External)

This layer includes:

* Industrial machines and production systems
* Sensors and actuators
* PLCs and embedded controllers

Assets are treated as **data producers and consumers**.

OpenFactory interacts with assets exclusively through protocol-specific connectors.
It does not manage device firmware, hardware lifecycle, or physical availability.

### Streaming Backbone Layer (Delegated but Central)

The Kafka cluster forms the persistent data plane of OpenFactory.

Responsibilities of the streaming backbone include:

* Event persistence
* Ordering guarantees
* Replication
* High throughput and low latency
* Stream replayability

OpenFactory assumes a reliable stream abstraction and focuses on the **meaning, transformation, and control logic applied to events**, rather than their transport implementation.

### Semantic Industrial Runtime (Owned by OpenFactory)

This is the core of OpenFactory.

It includes:

* Asset virtualization
* Unified Namespace (UNS) modeling
* Deterministic asset identity
* Event-driven data ingestion
* Stream-based control loops
* Semantic command definition
* Identity-bound command authenticity
* Stream processing (ksqlDB and Python services)
* Service composition and deployment definitions

OpenFactory guarantees:

* Coherent industrial meaning
* Deterministic asset identity
* Replayable asset behavior
* Semantic consistency across services
* Clear separation between data, logic, and control
* Scalable service deployment
* Service-level fault tolerance when deployed in orchestrated mode

## Scalability and Fault Tolerance Model

OpenFactory is architected for horizontal scalability and resilient operation.

When deployed using Docker Swarm (or a compatible orchestrator), OpenFactory:

* Supports horizontal scaling of microservices
* Enables automatic service redeployment upon failure
* Allows distributed execution across heterogeneous nodes
* Maintains system continuity through persistent Kafka streams
* Preserves industrial state via replayable event logs

Fault tolerance in OpenFactory emerges from:

* Stateless or stream-derived service design
* Persistent event sourcing (Kafka)
* Orchestrator-driven replication and restart
* Version-controlled deployment definitions

OpenFactory therefore guarantees:

> Resilient industrial behavior through architectural composition.

It does not guarantee:

* Infrastructure invulnerability
* Immunity to total network collapse
* Physical node survival

Resilience is systemic and architectural, not hardware-based.

## Infrastructure & Platform Layer (External Responsibility)

This layer includes:

* Docker runtime
* Docker Swarm orchestration substrate
* Network creation and routing
* Storage provisioning (e.g., NFS, volumes)
* Resource allocation and scheduling
* Host-level security policies

OpenFactory deploys services onto this layer but does not manage its primitives.

For example:

```yaml
networks:
  - factory-net
```

This expresses service-level attachment intent.

It does not imply:

* Network creation
* Driver configuration
* IP address management
* Routing enforcement
* Infrastructure topology reconciliation

Infrastructure lifecycle and topology remain external concerns.

## Service Orchestration vs Infrastructure Orchestration

OpenFactory orchestrates:

* Which services exist
* How services consume and produce streams
* How control loops are composed
* How asset semantics are enforced
* How runtime configurations are versioned

OpenFactory does not orchestrate:

* Infrastructure topology
* Cluster state reconciliation
* Network policy enforcement
* Host lifecycle management

This distinction preserves deployment portability and platform independence.

## Runtime Binding Configuration

Configuration elements such as:

* `image`
* `environment`
* `storage`
* `networks`

are classified as:

> Runtime binding parameters.

They describe how services bind to an execution environment.

They do not describe:

* Infrastructure creation
* Topology enforcement
* Platform lifecycle ownership

If a feature requires infrastructure state reconciliation or lifecycle management, it lies outside OpenFactory’s scope.

## Explicit Non-Guarantees

OpenFactory does not guarantee:

* Message delivery under total network failure
* Protection against infrastructure-level flooding
* Physical hardware availability
* Host-level security hardening
* Container scheduling fairness beyond orchestrator capabilities

These concerns belong to infrastructure and platform operators.

## Governance Rule for Feature Evolution

Before introducing new functionality, the following question must be answered:

> Does this define industrial meaning or service-level behavior?
> Or does it manage infrastructure mechanics?

If it manages infrastructure mechanics, it must remain outside OpenFactory.

This rule preserves architectural integrity and prevents functional drift.

## Strategic Implications

By maintaining strict responsibility boundaries, OpenFactory:

* Remains streaming-first and data-centric
* Scales from edge deployments to cloud clusters
* Integrates cleanly with existing IT/OT platforms
* Avoids becoming a monolithic infrastructure platform
* Enables reproducible industrial behavior via version-controlled configuration
* Supports resilient operation without owning infrastructure internals

## Conclusion

OpenFactory is a **semantic, streaming-first industrial runtime framework**.

It virtualizes physical assets, orchestrates industrial services, and implements event-driven control loops over a persistent data backbone.

Its scalability and fault tolerance arise from architectural composition with streaming systems and container orchestration platforms.

It deliberately avoids assuming responsibilities that belong to infrastructure provisioning, transport reliability, or platform governance.

This separation is a foundational design choice that ensures clarity, scalability, and long-term sustainability in industrial digital systems.
