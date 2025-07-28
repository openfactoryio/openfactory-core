## Metrics Monitoring

This project includes a Docker Compose setup to monitor the [Development Kafka Cluster](..) using [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/).

Metrics from Kafka and ksqlDB are collected via a [JMX Exporter](https://github.com/prometheus/jmx_exporter), which is already configured and included in the [Kafka development cluster](..) Docker Compose setup.

The monitoring stack includes:

- **Prometheus** – collects metrics from Kafka and ksqlDB JMX exporters.
- **Grafana** – visualizes the metrics via pre-configured dashboards.

---

### 🔧 Getting Started

Navigate to the `Monitoring/` directory and start the monitoring services:

```bash
cd Monitoring
docker compose up -d
````

---

### 🔍 Accessing the Tools

* **Prometheus UI:** [http://localhost:9090](http://localhost:9090)
* **Grafana UI:** [http://localhost:9300](http://localhost:9300)

Grafana default login:

* **Username:** `admin`
* **Password:** `admin` (you'll be asked to change it on first login)

---

### 📊 Dashboards

Grafana automatically loads a set of pre-configured dashboards from:

```
Monitoring/grafana/dashboards/
```

These include:

* Kafka cluster (KRaft) overview
* Topic throughput
* Transaction coordinator metrics
* ksqlDB processing
* Broker internals (JMX)

> Dashboards are provisioned via `grafana/provisioning/dashboards/dashboard.yml`.

---

### 🔗 Prometheus Scrape Targets

Prometheus is configured to scrape metrics from:

* **Kafka Broker:** `http://broker:9101/metrics`
* **ksqlDB Server:** `http://ksqldb-server:9102/metrics`

Configuration is located in:

```
Monitoring/prometheus/prometheus.yml
```

> Both Kafka and ksqlDB expose these metrics using the JMX exporter defined in the Kafka cluster setup.

---

### 🛠 Integration Details

All services run on the shared `factory-net` Docker network, allowing Prometheus and Grafana to communicate with Kafka and ksqlDB containers seamlessly.

> No additional setup is needed to expose JMX metrics — the main Kafka Docker Compose file already mounts and enables the JMX agent in both Kafka and ksqlDB.

---

### 🧹 Stopping & Cleaning Up

To stop the monitoring stack:

```bash
docker compose down
```

To remove all data volumes (Grafana state, etc.):

```bash
docker compose down -v
```