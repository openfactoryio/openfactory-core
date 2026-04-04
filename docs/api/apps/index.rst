OpenFactory Application Framework
=================================

This module provides the core building blocks for implementing an
OpenFactory application.

It combines three main concepts:

1. Declarative attributes (via :class:`AttributeField <openfactory.apps.attributefield.AttributeField>` and subclasses)
2. Callable methods (via `@ofa_method <ofa_method.html>`_ decorator)
3. Runtime integration with OpenFactory infrastructure (Kafka, ksqlDB, Asset model)

-------------------------------------------------------------------------------

Core Concepts
-------------

An :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` is an
:class:`Asset <openfactory.assets.asset_class.Asset>` in OpenFactory.

As such, it exposes:

- **Attributes** — representing state, telemetry, or metadata of the application
- **Methods** — representing actions that can be triggered by other Assets

This module provides a declarative and structured way to define both.

Declarative Attributes
~~~~~~~~~~~~~~~~~~~~~~
Attributes are defined as class-level fields using subclasses of
:class:`AttributeField <openfactory.apps.attributefield.AttributeField>`
(e.g., :class:`EventAttribute <openfactory.apps.attributefield.EventAttribute>`, :class:`SampleAttribute <openfactory.apps.attributefield.SampleAttribute>`).

These are automatically collected by the :class:`OpenFactoryAppMeta <openfactory.apps.attributefield.OpenFactoryAppMeta>` metaclass
and registered as OpenFactory :class:`AssetAttribute <openfactory.assets.utils.assetattribute.AssetAttribute>` instances during initialization.

.. admonition:: Declarative Attributes Usage Example

   .. code-block:: python

      class MyApp(OpenFactoryApp):
         status = EventAttribute(value="idle", tag="App.Status")
         temperature = SampleAttribute(tag="Sensor.Temperature")

These become runtime attributes of the OpenFactory asset.

-------------------------------------------------------------------------------

Callable Methods
~~~~~~~~~~~~~~~~
Instance methods can be exposed as remotely callable OpenFactory commands
using the `@ofa_method <ofa_method.html>`_ decorator.

The decorator:

- Validates the method signature
- Extracts parameter metadata
- Registers a command interface automatically

.. admonition:: Callable Methods Usage Example

   .. code-block:: python

      class MyApp(OpenFactoryApp):

         @ofa_method(description="Move axis to position")
         def move(self, x: float, y: float):
               self.logger.info(f"Moving to ({x}, {y})")

At runtime:

- A command attribute ``move`` is created
- A corresponding ``move_CMD`` attribute is subscribed to
- Incoming messages trigger method execution

-------------------------------------------------------------------------------

Application Lifecycle
~~~~~~~~~~~~~~~~~~~~~
An OpenFactory app is a subclass of :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`.

Typical lifecycle:

1. Instantiate the app with Kafka + ksqlDB configuration
2. Attributes and methods are automatically registered
3. Call :meth:`OpenFactoryApp.run <openfactory.apps.ofaapp.OpenFactoryApp.run>` or :meth:`OpenFactoryApp.async_run <openfactory.apps.ofaapp.OpenFactoryApp.run>`
4. Implement :meth:`OpenFactoryApp.main_loop <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>` (or :meth:`OpenFactoryApp.async_main_loop <openfactory.apps.ofaapp.OpenFactoryApp.async_main_loop>`)

.. admonition:: Running an OpenFactoryApp

   .. code-block:: python

      app = MyApp(
         ksqlClient=KSQLDBClient("http://localhost:8088"),
         bootstrap_servers="localhost:9092"
      )
      app.run()

-------------------------------------------------------------------------------

What Happens Automatically
--------------------------

When an app is instantiated:

- All declarative attributes are converted into :class:`AssetAttribute <openfactory.assets.utils.assetattribute.AssetAttribute>` objects
- All `@ofa_method <ofa_method.html>`_ methods are:

  - Registered as callable OpenFactory methods
  - Subscribed to their corresponding ``*_CMD`` attributes
- Logging is configured with the app UUID
- Storage backend is mounted (if configured via ``STORAGE`` env)
- Signal handlers are installed for graceful shutdown

-------------------------------------------------------------------------------

.. admonition:: Minimal Example

   .. code-block:: python

      import os
      from openfactory.apps import OpenFactoryApp, EventAttribute, ofa_method

      class DemoApp(OpenFactoryApp):

         status = EventAttribute(value="idle", tag="App.Status")

         @ofa_method()
         def ping(self):
               """Simple health check."""
               self.logger.info("pong")

         def main_loop(self):
               self.status = 'Running'
               while True:
                  self.logger.info("Running...")
                  time.sleep(5)

      app = DemoApp(
         ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
         bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092")
      )
      app.run()

.. note::
  - The application UUID is injected via the ``APP_UUID`` environment variable during deployment.
  - The environment variables ``KSQLDB_URL`` and ``KAFKA_BROKER`` are
    automatically provided in cluster deployments. The default values can be used for local development.
  - Either ``main_loop`` or ``async_main_loop`` must be implemented by subclasses.
  - Attribute and method registration is fully declarative and happens at class creation time.

-------------------------------------------------------------------------------

See Also
--------

.. toctree::
   :maxdepth: 2

   ofa_app
   ofa_attributes
   ofa_method
