# OpenFactory OPC UA Producer

This folder contains the files needed to build the Docker image `opcua-producer`.

The image runs a Kafka producer that ingests data from an OPC UA servr and pushes it to the Kafka cluster.

---

## Docker Image

The GitHub workflow [`opcua-producer.yml`](../../.github/workflows/opcua_producer.yml) builds this image and publishes it to [GitHub Container Registry (GHCR)](https://github.com/openfactoryio).

The image is used by OpenFactory to deploy OPC UA producers for specific devices.

### 🔧 Image Configuration in OpenFactory

In the OpenFactory configuration file [`openfactory.yml`](../../openfactory/config/openfactory.yml), the image is referenced using the variable:

```yaml
OPCUA_PRODUCER_IMAGE: ghcr.io/openfactoryio/opcua-producer
```

---

## Configuration via Environment Variables

The OPC UA producer is configured via the following environment variables:

| Variable               | Required | Description                                                          |
|------------------------|----------|----------------------------------------------------------------------|
| `KAFKA_BROKER`         | ✅       | Address of the Kafka broker (e.g. `broker:29092`)                    |
| `KSQLDB_URL`           | ✅       | URL of the KSQLDB (e.g. `http://ksql:8088`)                          |
| `OPCUA_PRODUCER_UUID`  | ✅       | Unique identifier for this producer instance                         |
| `DEVICE_UUID`          | ✅       | UUID of the OpenFactory device that this producer is collecting data |
| `OPCUA_CONNECTOR`      | ✅       | JSON string describing the OPC UA connector configuration            |

When deployed on the OpenFactory platform, OpenFactory automatically injects these environment variables into the container.

### Example

#### 🐳 Run via Docker

You can run the OPCUA producer in a container, either from a locally built image or the published one from GHCR.

---

##### 🔧 Run from a local Docker Image

Build the Docker Image Locally (from the project root folder):
```bash
docker build -t opcua-producer -f connectors/OPCUA/Dockerfile .
```
In the OpenFactory configuration file [`openfactory.yml`](../../openfactory/config/openfactory.yml), set:
```yaml
OPCUA_PRODUCER_IMAGE: opcua-producer
```

Then run:
```bash
ofa device up device_config.yml
```
> 🧠 The `device_config.yml` must contain the proper configuration of an OPC UA device

---

##### 🛰️ Run the Published Image (via GitHub Container Registry)

In the OpenFactory configuration file [`openfactory.yml`](../../openfactory/config/openfactory.yml), set:
```yaml
OPCUA_PRODUCER_IMAGE: ghcr.io/openfactoryio/opcua-producer:${OPENFACTORY_VERSION}
```

Then run:
```bash
ofa device up device_config.yml
```
> 🧠 The `device_config.yml` must contain the proper configuration of an OPC UA device

---

## Behavior

On startup, the producer:

1. Connects to the configured OPC UA server.
2. Streams real-time data items from the server.
3. Publishes messages to OpenFactory
4. Registers availability via Kafka metadata.
