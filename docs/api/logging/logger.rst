OpenFactory Logger API
======================

OpenFactory provides two classes for contextual logging:

- :class:`openfactory.logging.loggers.OpenFactoryLogger` is the logger
  available through ``self.logger`` in every OpenFactory Application.
- :class:`openfactory.logging.loggers.OpenFactoryLoggerAdapter` is
  returned by :meth:`OpenFactoryLogger.with_context <openfactory.logging.loggers.OpenFactoryLogger.with_context>`
  and represents a logger with additional persistent context.

OpenFactoryLogger
-----------------

.. autoclass:: openfactory.logging.loggers.OpenFactoryLogger
   :members:
   :show-inheritance:

OpenFactoryLoggerAdapter
------------------------

.. autoclass:: openfactory.logging.loggers.OpenFactoryLoggerAdapter
   :members:
   :show-inheritance:
