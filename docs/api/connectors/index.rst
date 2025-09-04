.. _connectors-index:

OpenFactory Connectors
======================

OpenFactory defines Connectors which translate between industrial protocols
(e.g., OPC UA, MTConnect) and the OpenFactory data model.

All Connectors derive from :class:`openfactory.connectors.base_connector.Connector` class.
Each Connector has a corresponding schema.

.. seealso::

   :ref:`connectors-schemas-index`.

So far the following Connectors are implemented:

.. toctree::
   :maxdepth: 1

   connector_base
   mtc_connector
   opcua_connector
