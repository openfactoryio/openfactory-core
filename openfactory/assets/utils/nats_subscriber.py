""" NATS releated methods. """

import json
import nats
from typing import Protocol
from .async_loop import AsyncLoopThread


class AssetNATSCallback(Protocol):
    """
    Interface for callback used to handle NATS asset messages.

    Args:
        msg_subject (str): The NATS subject of the message (e.g., ``OPCUA-SENSOR-001.*``).
        msg_value (dict): The JSON-decoded payload of the message.

    Note:
        The ``msg_value`` dictionary has the following structure:

        .. code-block:: python

            {
                "VALUE": 24.3,
                "TAG": "Temperature",
                "TYPE": "Samples",
                "attributes": {
                    "timestamp": "2026-01-27T15:37:03.871Z",
                    "ingestion_timestamp": "2026-01-27T15:37:03.882Z",
                    "asset_forwarder_timestamp": "2026-01-27T15:37:03.886Z",
                    "kafka_timestamp": "2026-01-27T15:37:03.882Z",
                    "kafka_timestamp_type": "producer"
                }
            }

        The ``VALUE``, ``TAG``, ``TYPE``, and ``attributes`` fields are always present.
        The ``attributes`` dictionary may contain additional keys depending on the
        monitored asset.
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
        self._closing = False

    async def _connect_and_subscribe(self) -> None:
        """ Connects to NATS and subscribes to the subject with an internal handler. """

        async def error_handler(e):
            print("NATS client error:", e)

        async def disconnected_cb():
            if not self._closing:
                print("NATS disconnected — will attempt reconnect...")

        async def reconnected_cb():
            print("NATS reconnected successfully.")

        self.nc = await nats.connect(
            servers=self.servers,
            max_reconnect_attempts=-1,
            connect_timeout=2,
            reconnect_time_wait=1,
            error_cb=error_handler,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        async def _handler(msg):
            data = json.loads(msg.data.decode())
            self.on_message(msg.subject, data)

        self.sub = await self.nc.subscribe(self.subject, cb=_handler)

    def start(self) -> None:
        """ Starts the NATS subscriber in the background event loop. """
        future = self.loop_thread.run_coro(self._connect_and_subscribe())
        try:
            future.result()  # <-- this will raise the exception if connection fails
        except nats.errors.NoServersError as e:
            print(f"❌ Failed to connect to NATS servers {self.servers}: {e}")
        except Exception as e:
            print(f"❌ Unexpected error connecting to NATS: {e}")

    def stop(self) -> None:
        """ Stops the NATS subscription and closes the connection. """
        self._closing = True

        async def _stop_all():
            if self.sub:
                try:
                    await self.sub.unsubscribe()
                except Exception as e:
                    print(f"Warning: unsub error: {e}")
            if self.nc:
                try:
                    await self.nc.close()
                except Exception as e:
                    print(f"Warning: nc close error: {e}")

        # Schedule the coroutine on the loop thread and wait
        try:
            self.loop_thread.run_coro(_stop_all()).result()
        except Exception as e:
            print(f"Warning: NATS stop failed: {e}")
