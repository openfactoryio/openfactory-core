# Kafka Cluster (Development)

Docker Compose setup for running a **single-broker Kafka cluster in KRaft mode**, with [ksqlDB](https://ksqldb.io/) for stream processing and Prometheus JMX exporter enabled for metrics.

This setup is intended for **development purposes only**.

---

## Overview

- **Kafka broker** running in [KRaft mode](https://kafka.apache.org/documentation/#kraft)
- **ksqlDB Server** and CLI
- **JMX Exporter** for Prometheus metrics
- Exposes:
  - Kafka broker on `9092` (host)
  - Kafka internal port `29092` (inside Docker network)
  - ksqlDB on `8088`
  - JMX metrics on:
    - Kafka broker: `9101`
    - ksqlDB: `9102`

---

## OpenFactory Integration

In OpenFactory, the variable `KAFKA_BROKER` in the configuration file [`openfactory.yml`](../openfactory/config/openfactory.yml) should point to the cluster:

```yaml
KAFKA_BROKER: ${KAFKA_BROKER}
````

Where the environment variable `KAFKA_BROKER` holds the IP address or hostname of the Kafka broker.

---

## Running the Cluster

Start the services with:

```bash
docker compose up -d
```

To wait for all services to become **healthy**, use:

```bash
docker compose up --wait
```

> Note: `--wait` ensures that the broker is not only running but also ready to accept messages.

---

## Connecting Other Services

Other services in Docker containers can connect to Kafka over `broker:29092` if they are part of the same Docker network.

In their `docker-compose.yml`:

```yaml
networks:
  factory-net:
    name: factory-net
    driver: overlay
    external: true
```

Attach services to the network:

```yaml
services:
  my-service:
    ...
    networks:
      - factory-net
```

---

## Prometheus / JMX Exporter

Kafka and ksqlDB expose metrics via [Prometheus JMX Exporter](https://github.com/prometheus/jmx_exporter).

* **Kafka JMX exporter** is available at `http://localhost:9101/metrics`
* **ksqlDB JMX exporter** is available at `http://localhost:9102/metrics`

To scrape these endpoints in your Prometheus configuration:

```yaml
scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['host.docker.internal:9101']

  - job_name: 'ksqldb'
    static_configs:
      - targets: ['host.docker.internal:9102']
```

> Replace `host.docker.internal` with your host IP or use network aliases if scraping from another container.

JMX Exporter is automatically downloaded into a shared volume (`jmx-agent`) and mounted in both Kafka and ksqlDB containers.

---

### 📈 Monitoring Stack (Prometheus + Grafana)

A full monitoring pipeline is included under [`Monitoring/`](./Monitoring), using:

* [**Prometheus**](https://prometheus.io/) for metrics collection
* [**Grafana**](https://grafana.com/) for dashboards and visualizations

Preconfigured dashboards visualize metrics from Kafka and ksqlDB collected via JMX exporters.

To start the monitoring stack:

```bash
cd Monitoring
docker compose up -d
```

Then open:

* **Grafana UI**: [http://localhost:9300](http://localhost:9300)
* **Prometheus UI**: [http://localhost:9090](http://localhost:9090)

Grafana will automatically load dashboards for:

* Kafka cluster (KRaft mode)
* Topics and throughput
* Transactions
* ksqlDB processing

> Default Grafana login: `admin / admin` (you’ll be prompted to change it)

For more details, see the [Monitoring/README.md](./Monitoring/README.md).

---

## Using ksqlDB

Launch the `ksql` CLI inside the container:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Sample commands inside the `ksql` prompt:

* List topics:

  ```sql
  list topics;
  ```

* Read all messages from a topic:

  ```sql
  print ofa_assets from beginning;
  ```

* Exit:

  ```sql
  exit
  ```

---

## Cluster ID (Optional)

Kafka running in KRaft mode uses a `CLUSTER_ID` to manage metadata.

To generate a new one if desired:

```bash
docker run --rm confluentinc/cp-kafka:7.7.0 kafka-storage random-uuid
```

You can replace the existing value in `docker-compose.yml` if you are rebuilding the cluster from scratch.

---

## Ports Summary

| Port  | Service       | Description                 |
| ----- | ------------- | --------------------------- |
| 9092  | Kafka         | Host access to Kafka broker |
| 29092 | Kafka         | Internal container access   |
| 9101  | Kafka         | JMX metrics (Prometheus)    |
| 8088  | ksqlDB Server | HTTP API / CLI interface    |
| 9102  | ksqlDB Server | JMX metrics (Prometheus)    |

---

## Cleanup

To stop and remove all services:

```bash
docker compose down
```

To clear all data and start fresh:
```bash
docker compose down -v
docker volume rm kafka-data kafka-metadata jmx-agent
```

---

## References

* [Kafka Quickstart (Docker)](https://developer.confluent.io/quickstart/kafka-docker/)
* [Confluent cp-all-in-one (KRaft)](https://github.com/confluentinc/cp-all-in-one/blob/7.5.0-post/cp-all-in-one-kraft/docker-compose.yml)
* [Monitoring Kafka with Prometheus](https://docs.confluent.io/platform/current/installation/docker/operations/monitoring.html)
* [ksqlDB Documentation](https://docs.ksqldb.io/)
