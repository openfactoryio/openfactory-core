import json
import os
from typing import Annotated
from openfactory.kafka import KSQLDBClient
from openfactory.apps import OpenFactoryFastAPIApp, ofa_method


class MetricsRegistry(OpenFactoryFastAPIApp):
    """
    OpenFactory Metrics Registry.

    This service acts as a bridge between OpenFactory applications and
    Prometheus HTTP Service Discovery.

    Applications register and deregister Prometheus metrics endpoints
    through OpenFactory methods. Registered endpoints are persisted in
    Kafka and materialized into a KSQLDB table.

    Prometheus periodically queries the HTTP discovery endpoint to obtain
    the list of metrics targets to scrape.

    By default, the discovery endpoint is exposed at::

      /prometheus/targets

    This can be overridden using the environment variable ``PROMETHEUS_SD_ENDPOINT``.

    The registry itself does not store any state in memory. Kafka and
    KSQLDB are the source of truth for all registered metrics endpoints.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the MetricsRegistry.

        This constructor forwards all parameters to
        :class:`OpenFactoryFastAPIApp <openfactory.apps.ofa_fastapi_app.OpenFactoryFastAPIApp>`

        Environment Variables:
            PROMETHEUS_SD_ENDPOINT:
                HTTP endpoint exposing the Prometheus HTTP Service Discovery target list.

                Default:
                    ``/prometheus/targets``

        Args:
            ksqlClient: KSQL client instance.
            bootstrap_servers: Kafka bootstrap server address.
            asset_router_url: Asset Router URL.
            loglevel: Logging level (e.g., ``INFO``, ``DEBUG``).
            test_mode: Enables test mode (disables live Kafka/ksql interaction).

        See also:
            :class:`OpenFactoryFastAPIApp <openfactory.apps.ofa_fastapi_app.OpenFactoryFastAPIApp>`
            for full initialization details and environment variable handling.
        """
        super().__init__(*args, **kwargs)

        self.api.state.ofa_app = self

        # redefine the Asset type
        if not getattr(self, "_test_mode", False):
            self.wait_until(attribute_id='AssetType', value='OpenFactoryApp')
        self.AssetType = 'Prometheus.Registry'

        self.create_metrics_table()

        # Prometheus discovery endpoint
        PROMETHEUS_SD_ENDPOINT = os.getenv("PROMETHEUS_SD_ENDPOINT", "/prometheus/targets")
        self.api.get(PROMETHEUS_SD_ENDPOINT)(
            self.prometheus_targets
        )

    @property
    def metrics_topic(self):
        return "metrics_targets"

    @property
    def metrics_source_table(self) -> str:
        return "METRICS_TARGETS_SOURCE"

    @property
    def metrics_table(self) -> str:
        return "METRICS_TARGETS"

    def create_metrics_table(self):
        """
        Create the KSQLDB tables used to store metrics targets.

        Creates a source table backed by the Kafka topic and a materialized
        table used for querying currently registered metrics endpoints.

        The source table receives registration and deregistration events
        emitted through the registry's OFA methods.

        Raises:
            Exception: If KSQLDB rejects table creation.
        """

        self.logger.info("Creating metrics target tables if they do not exist.")

        # Source table
        self.ksql.statement_query(f"""
        CREATE TABLE IF NOT EXISTS {self.metrics_source_table} (
            APPLICATION_UUID STRING PRIMARY KEY,
            HOST STRING,
            PORT INTEGER,
            PATH STRING
        ) WITH (
            KAFKA_TOPIC='{self.metrics_topic}',
            VALUE_FORMAT='JSON',
            PARTITIONS=1
        );
        """)

        # Materialized table
        self.ksql.statement_query(f"""
        CREATE TABLE IF NOT EXISTS {self.metrics_table} AS
            SELECT
                APPLICATION_UUID,
                HOST,
                PORT,
                PATH
            FROM {self.metrics_source_table}
            EMIT CHANGES;
        """)

    @ofa_method()
    def register_target(
        self,
        application_uuid: Annotated[str, "OpenFactory Application UUID"],
        host: Annotated[str, "Hostname or service name exposing metrics"],
        port: Annotated[int, "TCP port exposing metrics"],
        path: Annotated[str, "HTTP path exposing metrics"] = "/metrics",
    ):
        """ Register a Prometheus metrics endpoint """

        self.logger.info(f"Registering metrics endpoint {host}:{port}{path}")

        self.producer.produce(
            topic=self.metrics_topic,
            key=application_uuid,
            value=json.dumps({
                "HOST": host,
                "PORT": port,
                "PATH": path
            })
        )

    @ofa_method()
    def deregister_target(
        self,
        application_uuid: Annotated[str, "OpenFactory Application UUID"],
    ):
        """ Deregister a Prometheus metrics endpoint """

        self.logger.info(f"Deregistering metrics endpoint for {application_uuid}")

        self.producer.produce(
            topic=self.metrics_topic,
            key=application_uuid,
            value=None
        )

    def prometheus_targets(self):
        """
        Return metrics targets using the Prometheus HTTP Service Discovery format.

        Prometheus periodically invokes this endpoint to discover metrics
        endpoints exposed by OpenFactory applications.

        Returns:
            list[dict]: List of Prometheus target definitions.

        Example:

        .. code-block:: yaml

            [
              {
                "targets": ["shdr-gateway:4000"],
                "labels": {
                   "__metrics_path__": "/metrics"
                }
              }
            ]
        """

        rows = self.ksql.query(f"""
            SELECT
                APPLICATION_UUID,
                HOST,
                PORT,
                PATH
            FROM {self.metrics_table};
        """)

        targets = []
        for row in rows:
            targets.append(
                {
                    "targets": [
                        f"{row['HOST']}:{row['PORT']}"
                    ],
                    "labels": {
                        "__metrics_path__": row["PATH"]
                    }
                }
            )
        return targets


if __name__ == "__main__":
    app = MetricsRegistry(
        ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        loglevel=os.getenv("LOG_LEVEL", "DEBUG")
    )

    app.run()
