"""
Logging adapters for OpenFactory.
"""
from __future__ import annotations
import logging
from typing import Any, override


class OpenFactoryLoggerAdapter(logging.LoggerAdapter):
    """
    Logger that automatically adds contextual information to log entries.

    Instances are created using :meth:`OpenFactoryLogger.with_context`
    and may themselves be extended with additional context.
    """

    @override
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[object, dict[str, Any]]:
        """
        Process a logging call.

        Merges the adapter context with any ``extra`` values supplied for
        the individual logging call. If both define the same key, the
        adapter context takes precedence.

        Args:
            msg (str): Log message.
            kwargs (dict): Keyword arguments passed to the logging call.

        Returns:
            tuple[str, dict]: Processed message and keyword arguments.
        """
        extra = kwargs.get("extra", {}).copy()
        extra.update(self.extra)

        kwargs["extra"] = extra

        return msg, kwargs

    def with_context(self, **kwargs: Any) -> OpenFactoryLoggerAdapter:
        """
        Create a new logger with additional context.

        The existing context is preserved.

        Args:
            **kwargs: Additional context fields.

        Returns:
            OpenFactoryLoggerAdapter: Logger with the combined context.

        .. admonition:: Usage Example

            .. code-block:: python

                gateway_logger = self.logger.with_context(
                    gateway="opcua",
                )

                device_logger = gateway_logger.with_context(
                    asset_uuid=device.uuid,
                )

                device_logger.info("Connected")
        """
        context = self.extra.copy()
        context.update(kwargs)

        return OpenFactoryLoggerAdapter(self.logger, context)


class OpenFactoryLogger(logging.Logger):
    """ OpenFactory logger with support for contextual logging. """

    def with_context(self, **kwargs: Any) -> OpenFactoryLoggerAdapter:
        """
        Create a logger that automatically adds context to every log entry.

        This is useful when a component repeatedly logs information about
        the same object. For example, a device monitor can attach its
        ``asset_uuid`` once instead of passing it to every logging call.

        Args:
            **kwargs: Context fields to include in every log entry.

        Returns:
            OpenFactoryLoggerAdapter: Logger with the additional context.

        .. admonition:: Usage Example

            .. code-block:: python

                logger = self.logger.with_context(
                    asset_uuid=device.uuid,
                )

                logger.info("Connected")
        """
        return OpenFactoryLoggerAdapter(self, kwargs)
