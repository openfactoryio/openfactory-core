""" NATS releated methods. """

import json
import nats
from typing import Protocol
from .async_loop import AsyncLoopThread


class AssetNATSCallback(Protocol):
    """
    Interface for callback used to handle NATS asset messages.

    Args:
        msg_subject (str): The NATS subject of the message (e.g., 'OPCUA-SENSOR-001.*').
        msg_value (dict): The JSON-decoded payload of the message.
    """
    def __call__(self, msg_subject: str, msg_value: dict) -> None:
        """ Method to handle incoming NATS asset messages. """
        ...


class NATSSubscriber:
    """
    Represents a NATS subscriber that runs in a dedicated asyncio event loop thread.

    This class connects to a NATS server, subscribes to a subject, and calls
    the provided callback for each received message.
    """

    def __init__(self, loop_thread: AsyncLoopThread,
                 servers: str | list[str], subject: str,
                 on_message: AssetNATSCallback) -> None:
        """
        Initializes the NATS subscriber.

        Args:
            loop_thread (AsyncLoopThread): Event loop thread used to run async operations.
            servers (str | list[str]): NATS server URL(s).
            subject (str): NATS subject to subscribe to.
            on_message (AssetNATSCallback): Callback to invoke when a message is received.
        """
        self.loop_thread = loop_thread
        self.subject = subject
        self.on_message = on_message
        self.servers = servers
        self.nc = None
        self.sub = None

    async def _connect_and_subscribe(self) -> None:
        """ Connects to NATS and subscribes to the subject with an internal handler. """
        self.nc = await nats.connect(self.servers)

        async def _handler(msg):
            data = json.loads(msg.data.decode())
            self.on_message(msg.subject, data)

        self.sub = await self.nc.subscribe(self.subject, cb=_handler)

    def start(self) -> None:
        """ Starts the NATS subscriber in the background event loop. """
        self.loop_thread.run_coro(self._connect_and_subscribe())

    def stop(self) -> None:
        """ Stops the NATS subscription and closes the connection. """
        if self.sub:
            self.loop_thread.run_coro(self.sub.unsubscribe())
        if self.nc:
            self.loop_thread.run_coro(self.nc.close())
