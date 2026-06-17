.. _monitoring-index:

OpenFactory Monitoring
======================

OpenFactory provides native support for Prometheus monitoring.

Applications can declare a Prometheus metrics endpoint as part of their
deployment configuration. The OpenFactory deployment system
automatically registers these endpoints with the Metrics Registry,
allowing Prometheus to discover and scrape deployed applications
without requiring manual target configuration.

This approach eliminates the need to maintain Prometheus target lists
and ensures that monitoring automatically follows application
deployments, updates, and removals.

-------------------------------------------------------------------------------

Core Concepts
-------------

OpenFactory monitoring is based on four main concepts:

- **OpenFactory Applications** exposing Prometheus metrics
- **OpenFactory Deployment Manager** automatically registering metrics endpoints
- **Metrics Registry** maintaining the list of active targets
- **Prometheus** discovering targets through HTTP Service Discovery

The monitoring subsystem provides a deployment-independent discovery
mechanism that works identically for Docker and Docker Swarm
deployments.

-------------------------------------------------------------------------------

Metrics Declaration
~~~~~~~~~~~~~~~~~~~

OpenFactory applications expose metrics by defining a ``metrics`` section in their
deployment configuration.

.. admonition:: Metrics Declaration Example

   .. code-block:: yaml

      apps:

        demo-app:

          uuid: DEMO-APP
          image: my-image

          metrics:
            port: 4000
            path: /metrics

The presence of the ``metrics`` section automatically enables
registration with the Metrics Registry.

-------------------------------------------------------------------------------

Metrics Registry
~~~~~~~~~~~~~~~~

The :ref:`Metrics Registry <monitoring-prometheus-registry>` acts as the bridge between OpenFactory deployments
and Prometheus.

The registry:

- Receives metrics registrations through OpenFactory methods
- Persists registrations in Kafka
- Maintains a queryable view using KSQLDB
- Exposes targets using Prometheus HTTP Service Discovery

The registry itself maintains no in-memory state. Kafka and KSQLDB
remain the source of truth for all registered metrics endpoints.

The registry is an OpenFactory Application. A typical deployment configuration 
file is given below:

.. admonition:: OpenFactory Prometheus Metrics Registry Declaration Example

   .. code-block:: yaml

      apps:

        prometheus_metrics_registry:
          uuid: PROMETHEUS-METRICS-REGISTRY
          image: ghcr.io/openfactoryio/prometheus-metrics-registry:${OPENFACTORY_VERSION}
          environment:
            - LOG_LEVEL=${PROMETHEUS_METRICS_REGISTRY_LOG_LEVEL:-INFO}
          networks:
            - factory-net

-------------------------------------------------------------------------------

Application Lifecycle
~~~~~~~~~~~~~~~~~~~~~

When an application is deployed:

1. The OpenFactory deployment manager detects the ``metrics`` section.
2. The metrics endpoint is registered with the Metrics Registry.
3. The registry stores the endpoint information in Kafka.
4. KSQLDB materializes the current list of active targets.
5. Prometheus discovers the new target during its next refresh cycle.

When an application is removed:

1. The OpenFactory deployment manager deregisters the metrics endpoint.
2. The registry publishes a tombstone record to Kafka.
3. KSQLDB removes the endpoint from the materialized table.
4. Prometheus automatically stops scraping the target.

-------------------------------------------------------------------------------

Prometheus Integration
----------------------

Prometheus discovers OpenFactory applications through the Metrics
Registry.

By default, the registry exposes a Prometheus HTTP Service Discovery
endpoint at:

::

   /prometheus/targets

The endpoint can be customized using the
``PROMETHEUS_SD_ENDPOINT`` environment variable.

A minimal Prometheus configuration is shown below.

.. code-block:: yaml

   scrape_configs:

     - job_name: openfactory

       http_sd_configs:
         - url: http://prometheus-metrics-registry:4000/prometheus/targets

Prometheus periodically queries the Metrics Registry and automatically
discovers all registered OpenFactory applications.

.. note::

  - If the registry endpoint has been customized, update the URL accordingly.
  - The host name (``prometheus-metrics-registry`` in the example) depends 
    on the name choosen during the deployment of the OpenFactory Prometheus registry

-------------------------------------------------------------------------------

Benefits
---------

Compared to manually maintained Prometheus target lists:

- Automatic registration of deployed applications
- Automatic deregistration of removed applications
- No static target lists to maintain
- No Docker-specific service discovery required
- Identical behavior for Docker and Docker Swarm deployments
- Centralized monitoring configuration

.. note::

   The current implementation assumes a single Metrics Registry
   instance and a single Prometheus instance consuming the registry.

-------------------------------------------------------------------------------

See Also
--------

.. toctree::
   :maxdepth: 1

   registry
   utils
