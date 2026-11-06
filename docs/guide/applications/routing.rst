Application Routing & Networking
=================================

Overview
--------

OpenFactory uses **Traefik** as its HTTP reverse proxy to expose applications running in Docker containers.

Rather than publishing individual application ports on the host, OpenFactory uses host-based routing. Each exposed application receives its own hostname, and Traefik forwards incoming HTTP requests to the appropriate application container.

This architecture provides:

* A single HTTP entry point for all applications.
* Automatic application discovery through Docker labels.
* Hostname-based routing.
* No need to publish individual application ports.
* Independent deployment and lifecycle management of applications.

Architecture
------------

The routing architecture consists of three components:

1. **Application containers** – Run individual OpenFactory applications and expose their HTTP services on an internal container port.
2. **Traefik reverse proxy** – Discovers exposed applications through Docker labels and routes HTTP requests.
3. **Hostname resolution** – Resolves application hostnames to the IP address of the Docker host.

Request flow
~~~~~~~~~~~~

A typical HTTP request follows this path::

    Browser
       |
       | http://paint-hmi.openfactory.local
       |
       v
    DNS / Hosts file
       |
       v
    Docker Host :80
       |
       v
    Traefik Reverse Proxy
       |
       | Host-based routing
       |
       v
    Application Container :4000

Traefik uses the HTTP ``Host`` header to select the appropriate application router.

Traefik Reverse Proxy
---------------------

Traefik runs as a dedicated Docker service and connects to the shared Docker network used by OpenFactory applications.

A typical configuration uses:

* HTTP entrypoint on port 80.
* HTTPS entrypoint on port 443 (when TLS is configured).
* Docker provider for automatic service discovery.
* ``exposedByDefault=false`` to prevent unintended exposure.
* Docker labels to configure application routers and services.

Applications must be connected to the network used by Traefik (typically ``factory-net``) to be reachable.

The Traefik Docker provider discovers containers and their labels automatically.

The OpenFactory repository provides example Docker Compose configurations for deploying Traefik in the ``traefik/`` directory:

* ``docker-compose.traefik.yml`` for Docker Compose deployments.
* ``docker-compose.traefik.swarm.yml`` for Docker Swarm deployments.

.. warning::

   These configurations are intended as starting points for development and testing. They are not production-ready and may require additional configuration, particularly regarding TLS, authentication, and network security.


Application routing
~~~~~~~~~~~~~~~~~~~

OpenFactory automatically configures application routing based on the
application's routing definition. For details on how to configure application
exposure, hostnames, and routing parameters, see
:ref:`application-exposure`.

Each exposed application is assigned a Traefik router matching its hostname
and a service pointing to its internal HTTP port.

For example, the following labels illustrate the Traefik configuration
generated for an application:

.. code-block:: yaml

    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.paint-hmi.rule=Host(`paint-hmi.openfactory.local`)"
      - "traefik.http.routers.paint-hmi.entrypoints=web"
      - "traefik.http.services.paint-hmi.loadbalancer.server.port=4000"

In this example:

* ``paint-hmi.openfactory.local`` is the application hostname.
* ``web`` identifies the HTTP entrypoint.
* ``4000`` is the internal port on which the application listens.

The application must listen on ``0.0.0.0``, rather than ``127.0.0.1``,
so that Traefik can reach it through the Docker network.

.. note::

   The application port is an internal Docker network port. It does not
   need to be published on the host.


.. _application-exposure:

Application Exposure
--------------------

Application exposure is configured through the application's ``routing`` definition.

For details on application-level routing configuration, see
:ref:`Application Definition - Routing (HTTP Exposure) <routing-http-exposure>`.

Example:

.. code-block:: yaml

    routing:
      expose: true
      port: 4000
      hostname: paint-hmi

When routing is enabled, OpenFactory generates the appropriate Traefik
configuration.


OpenFactory Base Domain
~~~~~~~~~~~~~~~~~~~~~~~

The OpenFacotry base domain is used to construct application hostnames.

For example, with the OpenFacotry base domain configured to
``openfactory.local``, an application with UUID ``paint-hmi`` can be accessed using::

    http://paint-hmi.openfactory.local/

The domain suffix used for application hostnames is configurable through
the ``OPENFACTORY_BASE_DOMAIN`` setting.

It can be configured in two ways:

**1. OpenFactory configuration file**

Set ``OPENFACTORY_BASE_DOMAIN`` in the ``openfactory.yml`` configuration file:

.. code-block:: yaml

    OPENFACTORY_BASE_DOMAIN: openfactory.local

**2. Environment variable**

Alternatively, configure ``OPENFACTORY_BASE_DOMAIN`` as an environment
variable in the environment where OpenFactory is deployed.

For example:

.. code-block:: bash

    export OPENFACTORY_BASE_DOMAIN=openfactory.local

Hostname Generation
~~~~~~~~~~~~~~~~~~~

OpenFactory generates two types of hostnames for exposed applications:

* **Canonical hostname**: ``<uuid>.<base-domain>`` (always generated).
* **Alias hostname**: ``<hostname>.<base-domain>`` (generated when a hostname is configured).

For example, with ``OPENFACTORY_BASE_DOMAIN`` set to
``openfactory.local``, an application configured with the hostname
``paint-hmi`` can be accessed using::

    http://<uuid>.openfactory.local/
    http://paint-hmi.openfactory.local/

The canonical hostname is always available, while the alias provides
a human-readable alternative.

.. note::

   Changing ``OPENFACTORY_BASE_DOMAIN`` changes the generated application
   hostnames. DNS records or client hosts-file entries must therefore
   resolve the new hostnames to the Docker host IP address.

Applications that are not exposed do not receive an externally accessible
Traefik route.

Hostname Resolution
-------------------

Application hostnames are generated using the configured
:ref:`OpenFactory base domain <application-exposure>`.

Clients must resolve these hostnames to the IP address of the Docker host.
This can be achieved using DNS or local hosts-file entries.

DNS
~~~

In a production environment, configure DNS records pointing application hostnames to the Docker host.

For example::

    paint-hmi.openfactory.local    -> 192.168.0.100
    asset-router.openfactory.local -> 192.168.0.100

All application hostnames can point to the same IP address.

Traefik determines the destination application using the HTTP ``Host`` header.

Local hosts file
~~~~~~~~~~~~~~~~

For development or small installations, hostnames can be configured manually.

On Linux:

.. code-block:: console

    sudo nano /etc/hosts

On Windows, edit the following file with administrator privileges::

    C:\Windows\System32\drivers\etc\hosts

Example entries::

    192.168.0.100 traefik.openfactory.local
    192.168.0.100 paint-hmi.openfactory.local
    192.168.0.100 asset-router.openfactory.local
    192.168.0.100 monitoring.openfactory.local

Replace ``192.168.0.100`` with the IP address of the Docker host.

All applications can share the same IP address because Traefik distinguishes them using their hostnames.

Remote Development with VSCode
------------------------------

When developing remotely using VSCode Remote SSH, the Docker host may not be directly accessible from the developer's laptop.

VSCode provides port forwarding to solve this problem.

For example, forwarding remote port 80 makes the Traefik HTTP entrypoint available locally::

    Laptop localhost:80 -> Remote Docker host:80

However, accessing an application using::

    http://localhost/

sends the HTTP ``Host`` header ``localhost``.

This will not match an application router expecting::

    paint-hmi.openfactory.local

There are two ways to access applications in this scenario.

Option 1: Use a local hosts file entry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add the following entry to the laptop's hosts file::

    127.0.0.1 paint-hmi.openfactory.local

Now the application can be accessed using::

    http://paint-hmi.openfactory.local/

The request resolves to the local machine, passes through the VSCode forwarded port, and reaches Traefik on the remote Docker host.

The original hostname is preserved in the HTTP ``Host`` header, allowing Traefik to select the correct application.

Additional applications can be configured in the same way::

    127.0.0.1 paint-hmi.openfactory.local
    127.0.0.1 asset-router.openfactory.local
    127.0.0.1 monitoring.openfactory.local

Only one VSCode port forwarding configuration (port 80) is required.

Option 2: Access the Docker host directly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the laptop can reach the Docker host over the LAN or VPN, the hosts file can point directly to the Docker host IP::

    192.168.0.100 paint-hmi.openfactory.local
    192.168.0.100 asset-router.openfactory.local
    192.168.0.100 monitoring.openfactory.local

Applications can then be accessed directly::

    http://paint-hmi.openfactory.local/

VSCode port forwarding is no longer required.

The Docker host firewall must allow incoming connections on the configured HTTP port.

Troubleshooting
---------------

Traefik returns 404 Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Traefik 404 usually indicates that no router matched the incoming request.

Check:

* Is the requested hostname correct?
* Does the HTTP ``Host`` header match the router rule?
* Is the application exposed through Traefik?
* Is the container connected to the Traefik Docker network?

For example, these requests have different Host headers:

.. code-block:: console

    curl http://localhost/

.. code-block:: console

    curl -H "Host: paint-hmi.openfactory.local" http://localhost/

The second request explicitly supplies the hostname expected by the application router.

Traefik returns 502 Bad Gateway
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A 502 generally indicates that Traefik matched the router but could not successfully communicate with the backend.

Check:

* Is the application container running?
* Is the application listening on the configured port?
* Is it listening on ``0.0.0.0``?
* Are the application and Traefik connected to the same Docker network?

Application is accessible locally but not remotely
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check:

* Is the Docker host IP correct?
* Is the host firewall allowing incoming connections?
* Does the client resolve the application hostname to the correct IP?
* Is the network reachable from the client?

Security Considerations
-----------------------

The Traefik dashboard and API should not be exposed publicly without appropriate protection.

For production installations:

* Configure HTTPS.
* Protect administrative endpoints with authentication.
* Restrict access to the Traefik dashboard.
* Expose only applications that explicitly require HTTP access.
* Apply appropriate firewall rules.
