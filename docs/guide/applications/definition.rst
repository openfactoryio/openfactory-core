Application Definition
======================

OpenFactory applications are a particular type of OpenFacotry Assets. 
They are defined declaratively using YAML.

Each application describes:
    - which container image to run
    - how it integrates into the Unified Namespace (UNS)
    - how it is configured and deployed
    - how it is optionally exposed externally

.. admonition:: Minimal Example

    .. code-block:: yaml

        apps:
          scheduler:
            uuid: "app-scheduler"
            image: ghcr.io/openfactoryio/scheduler:v1.0.0


Schema Overview
---------------

Each application supports the following sections:

- ``uuid`` — Asset UUID identifier of the application
- ``image`` — Docker image to run
- ``uns`` — Unified Namespace metadata
- ``environment`` — environment variables
- ``storage`` — storage backend configuration
- ``routing`` — HTTP exposure configuration
- ``networks`` — Docker networks
- ``deploy`` — deployment configuration

UUID
----

Each application must define a unique identifier:

.. code-block:: yaml

    uuid: app-scheduler

This value is used by OpenFactory as Asset UUID.

Image
-----

The Docker image defines the application runtime:

.. code-block:: yaml

    image: ghcr.io/openfactoryio/scheduler:v1.0.3

UNS (Unified Namespace)
-----------------------

The ``uns`` section defines how the application is represented in the
OpenFactory Unified Namespace.

.. code-block:: yaml

    uns:
      location: building-a
      workcenter: scheduler

The structure must comply with the configured ``UNSSchema``.

Environment Variables
---------------------

Environment variables can be passed to the container:

.. code-block:: yaml

    environment:
      - ENV=production
      - LOG_LEVEL=info

Storage
-------

Applications can mount persistent storage using a configured backend.

Example (NFS):

.. code-block:: yaml

    storage:
      type: nfs
      server: deskfab.openfactory.com
      remote_path: /nfs/deskfab
      mount_point: /mnt
      mount_options:
        - ro

Multiple storage backends are supported.

.. seealso::

   :ref:`filelayer-index`.

Routing (HTTP Exposure)
-----------------------

Applications can be exposed externally via HTTP.

.. code-block:: yaml

    routing:
      expose: true
      port: 8000
      hostname: dashboard

Hostnames
~~~~~~~~~

When routing is enabled, OpenFactory generates:

- a **canonical hostname** (always present):

  .. code-block:: text

      <app-name>-<uuid>.<base-domain>

- an optional **alias hostname** (if available):

  .. code-block:: text

      <hostname>.<base-domain>

Example:

.. code-block:: text

    scheduler-a1b2.example.com
    dashboard.example.com

Networks
--------

Applications can be connected to one or more Docker networks:

.. code-block:: yaml

    networks:
      - factory-net
      - monitoring-net

All networks must exist prior to deployment.

Deployment
----------

The ``deploy`` section defines how the application is deployed.

.. code-block:: yaml

    deploy:
      replicas: 2

      resources:
        reservations:
          cpus: 0.5
          memory: "512Mi"
        limits:
          cpus: 1.0
          memory: "1Gi"

      placement:
        constraints:
          - node.labels.zone == building-a

.. admonition:: Complete Example

    .. code-block:: yaml

        apps:
          scheduler:
            uuid: "app-scheduler"
              image: ghcr.io/openfactoryio/scheduler:v1.0.0

            uns:
              location: building-a
              workcenter: scheduler

            environment:
              - ENV=production

            storage:
              type: nfs
              server: deskfab.openfactory.com
              remote_path: /nfs/deskfab
              mount_point: /mnt
              mount_options:
                - ro

            routing:
              expose: true
              port: 8000
              hostname: dashboard

            networks:
              - factory-net
              - monitoring-net

            deploy:
              replicas: 2

              resources:
                reservations:
                  cpus: 0.5
                  memory: "512Mi"
                limits:
                  cpus: 1.0
                  memory: "1Gi"

              placement:
                constraints:
                  - node.labels.zone == building-a

.. note::
    - Configuration is strictly validated (unknown fields are rejected)
    - UNS metadata must match the configured schema
    - Hostnames are normalized and validated to comply with DNS rules
    - A canonical hostname is always generated when routing is enabled

.. seealso::
    - The schema of OpenFactory Apps is :class:`openfactory.schemas.apps.OpenFactoryAppSchema`.
    - The runtime class of OpenFactory Apps is :class:`openfactory.apps.ofaapp.OpenFactoryApp`.
