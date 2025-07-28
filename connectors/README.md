# OpenFactory Connectors

This folder contains Kafka data connectors used in the OpenFactory platform. These connectors run as Docker containers and push data into the Kafka cluster for further processing and streaming via ksqlDB.

## Available Connectors

- [**MTConnect**](MTConnect):  
  Dockerized Kafka producer that connects to an MTConnect agent and streams machine data into the Kafka cluster.

---

## Usage

Each connector has its own `README.md` explaining how to build and run the container, along with configuration details.

---

## Adding New Connectors

To add a new connector:

1. Create a subdirectory under `connectors/`
2. Add:

   * `Dockerfile`
   * Source code (e.g., Python script)
   * `README.md` with usage instructions
