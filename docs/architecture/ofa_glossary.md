# OpenFactory Terminology Glossary

This document defines the formal terminology used throughout OpenFactory documentation.
Terms defined here are used consistently across architecture, implementation, and operational documents.

---

## Asset

An **asset** is a physical or logical entity in the manufacturing environment.

Assets may represent machines, sensors, actuators, production units, or virtualized process elements.
In OpenFactory, assets are modeled following Industry 4.0 principles (e.g. RAMI 4.0) and are the primary unit of identity.

---

## Asset Stream

The **asset stream** is a Kafka topic that contains events for all assets managed by OpenFactory.

Each asset is represented by a unique key within the topic.
The asset stream is the authoritative source of all asset-related data.

---

## Event

An **event** is an immutable, time-ordered record representing a time-stamped observation or state change related to a single asset.

Events are append-only and cannot be modified once written.
All processing and state derivation in OpenFactory is based on events.

---

## Gateway (Connector)

A **gateway** (also referred to as a *connector* in higher-level documentation) is a stateless ingestion service that interfaces with physical devices.

Gateways translate protocol-specific communication (e.g. OPC UA, Modbus) into normalized OpenFactory events and publish them to the asset stream.

---

## Kappa Architecture

A **Kappa architecture** is a streaming architecture in which all data flows through a single event stream.

In OpenFactory:
- there is no separate batch layer
- all state is derived from streams
- Kafka is the only system of record

---

## Kafka (System of Record)

[**Kafka**](https://kafka.apache.org/) is the central streaming platform and the only system of record in OpenFactory.

All events, derived states, and control signals pass through Kafka topics.
Any materialized view or state can be rebuilt by replaying streams.

---

## State

A **state** is a derived representation computed from one or more streams.

States are reproducible, ephemeral, and not considered authoritative.
They exist to support querying, monitoring, control logic, and higher-level applications.

---

## ksqlDB

**ksqlDB** is a stream-processing engine used in OpenFactory to compute derived states from Kafka streams.

It enables declarative stream processing using a SQL-like syntax.
In OpenFactory, ksqlDB is treated as an interchangeable implementation choice rather than a foundational component.

Reference implementation: [https://github.com/confluentinc/ksql](https://github.com/confluentinc/ksql)

---

## Forwarder

A **forwarder** is a stateless service that consumes events from the asset stream and forwards them to the serving layer.

Forwarders are replicated according to Kafka topic partitions to preserve ordering and scalability.

---

## Serving Layer

The **serving layer** provides user-facing access to asset data.

It decouples consumer access patterns from the streaming backbone and enables scalable fan-out without exposing Kafka directly.

---

## NATS

[**NATS**](https://nats.io/) is a messaging system used as the fan-out mechanism in the serving layer.

It supports high-throughput, low-latency distribution of asset-level data to many concurrent consumers.

---

## System of Record

The **system of record** is the authoritative source of truth for data.

In OpenFactory, Kafka is the only system of record.
All other data representations are derived and non-authoritative.

