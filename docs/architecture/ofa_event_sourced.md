# OpenFactory as an Event-Sourced Industrial Control System

## From Traditional Control to Event-Sourced Control

Traditional industrial control architectures are typically:

* State-based
* PLC-centric
* Imperative and opaque
* Difficult to replay or audit
* Hard to scale horizontally

State changes occur inside controllers and are rarely persisted as a complete historical record.

OpenFactory adopts a fundamentally different paradigm:

> Industrial systems are modeled as streams of immutable events.

Every observable asset interaction — telemetry emission, state transition, or control command — is represented as an event appended to a persistent log.

This aligns OpenFactory with the principles of **event sourcing**.

## Core Event-Sourcing Principles Applied to Industry

OpenFactory implements the following event-sourcing concepts:

### Immutable Event Log

All asset interactions pass through Kafka topics.
Events are:

* Ordered
* Persistent
* Replayable
* Versioned implicitly by time

The event log becomes the single source of truth.

### State as a Derived View

System state is not stored as a primary artifact.

Instead:

* Current asset state
* Aggregated metrics
* Control decisions
* Derived process indicators

are computed from event streams using:

* ksqlDB queries
* Stream processors
* Stateless or stream-stateful services

State is therefore:

> A projection of history.

### Deterministic Replayability

Because all events are retained:

* Entire industrial sessions can be replayed
* Stream processors can be rebuilt from zero
* Bugs can be diagnosed from historical reconstruction
* New analytics models can be applied retroactively

Replayability enables:

* Forensic analysis
* Continuous improvement
* Model retraining
* Process optimization

### Control as a Data Stream Loop

In OpenFactory:

1. Assets emit telemetry → event stream
2. Stream processors derive insights
3. Control decisions are emitted as command events
4. Connectors translate command events into protocol-level actions
5. Assets react and emit new telemetry

This creates a closed-loop architecture:

> Control is implemented as a transformation over event streams.

This decouples:

* Physical control mechanisms
* Industrial logic
* Infrastructure mechanics

Control becomes transparent, observable, and replayable.

### Industrial Advantages of Event Sourcing

OpenFactory’s event-sourced architecture enables:

- Full Observability

  Every asset interaction is recorded.

- Time Travel Debugging

  Past system states can be reconstructed.

- Safe Evolution of Logic

  New processors can consume historical data without disrupting operations.

- Scalable Control Logic

  Logic scales horizontally via stream partitioning.

- Decoupled Services

  Services communicate only through streams, not shared state.

### Event Sourcing and Fault Tolerance

Event sourcing strengthens resilience:

* If a service crashes → it is redeployed
* It reprocesses events from the log
* Its state is reconstructed deterministically

Combined with:

* Docker Swarm service redeployment
* Kafka persistence and replication

OpenFactory achieves:

> System-level fault tolerance through deterministic event reconstruction.

No service holds exclusive authoritative state.

The log is the authority.

### Industrial Implications

This architecture transforms industrial systems from:

Stateful black-box controllers

into:

Transparent, replayable, distributed control systems.

It enables:

* Continuous optimization
* ML-driven control
* Digital twins derived from real execution history
* Cross-site analytics
* Edge-to-cloud synchronization

OpenFactory thus represents:

> An event-sourced industrial control paradigm.
