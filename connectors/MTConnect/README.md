# OpenFactory MTConnect Producer

This folder contains the files needed to build the Docker image `kafka-mtc-producer`.

The image runs a Kafka producer that ingests MTConnect data from an MTConnect agent and pushes it to the Kafka cluster.

---

## Docker Image

The GitHub workflow [`kafka_mtc_producer.yml`](../../.github/workflows/kafka_mtc_producer.yml) builds this image and publishes it to [GitHub Container Registry (GHCR)](https://github.com/openfactoryio).

The image is used by OpenFactory to deploy MTConnect producers for specific devices.

### 🔧 Image Configuration in OpenFactory

In the OpenFactory configuration file [`openfactory.yml`](../../openfactory/config/openfactory.yml), the image is referenced using the variable:

```yaml
MTCONNECT_PRODUCER_IMAGE: ghcr.io/openfactoryio/kafka-mtc-producer
```

---

## Configuration via Environment Variables

The MTConnect producer is configured via the following environment variables:

| Variable               | Required | Description                                                   |
|------------------------|----------|---------------------------------------------------------------|
| `KAFKA_BROKER`         | ✅       | Address of the Kafka broker (e.g. `broker:29092`)             |
| `MTC_AGENT`            | ✅       | URL of the MTConnect agent (e.g. `http://192.168.1.10:5000`)  |
| `KAFKA_PRODUCER_UUID`  | ✅       | Unique identifier for this producer instance                  |
| `KAFKA_TOPIC_ASSETS`   | ❌       | Kafka topic to send MTConnect data to (default: `ofa_assets`) |

> `KAFKA_TOPIC_ASSETS` is optional. Use it only if you need to override the default topic (`ofa_assets`).  
> OpenFactory standardizes topic names, so overriding this should be avoided unless explicitly needed.

When deployed on the OpenFactory platform, OpenFactory automatically injects these environment variables into the container.

### Example

#### 🧪 Run Locally (for Development)

If you have the required Python environment set up, you can run the producer directly from the shell:

```bash
export KAFKA_BROKER=localhost:9092
export MTC_AGENT=http://192.168.1.10:5000
export KAFKA_PRODUCER_UUID=device-001-producer

python kafka_producer.py
```

> ✅ Ensure the Kafka development cluster is running locally and an MTConnect agent is accessible.

---

#### 🐳 Run via Docker

You can run the MTConnect producer in a container, either from a locally built image or the published one from GHCR.

---

##### 🔧 Build the Docker Image Locally

For development and testing:

```bash
docker build -t kafka-mtc-producer .
````

Then run it:

```bash
docker run --network factory-net \
  -e KAFKA_BROKER=broker:29092 \
  -e MTC_AGENT=https://demo.mtconnect.org \
  -e KAFKA_PRODUCER_UUID=device-001-producer \
  kafka-mtc-producer
```
> 🧠 The `broker:29092` endpoint refers to the internal Kafka listener exposed inside the `factory-net` Docker network.
>
> 🧠 https://demo.mtconnect.org is a demo agent run by the MTConnect institute.

---

##### 🛰️ Run the Published Image (via GitHub Container Registry)

For use in CI or production deployments:

```bash
docker run --network factory-net \
  -e KAFKA_BROKER=broker:29092 \
  -e MTC_AGENT=https://demo.mtconnect.org \
  -e KAFKA_PRODUCER_UUID=device-001-producer \
  ghcr.io/openfactoryio/kafka-mtc-producer
```
> 🧠 The `broker:29092` endpoint refers to the internal Kafka listener exposed inside the `factory-net` Docker network.

---

## Behavior

On startup, the producer:

1. Connects to the configured MTConnect agent.
2. Streams real-time data items from the agent.
3. Publishes messages to the Kafka topic `ofa_assets`.
4. Registers availability via Kafka metadata.

---

## Graceful Shutdown

The producer listens for shutdown signals (`SIGINT`, `SIGTERM`) and marks itself as **unavailable** before exiting, to support state-aware stream processing.
