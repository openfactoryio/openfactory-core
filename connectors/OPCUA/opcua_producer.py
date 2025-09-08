"""
OPC UA Kafka Producer for OpenFactory

This module implements an OpenFactory producer that connects to an OPC UA server,
subscribes to configured device variables and events, and forwards updates to OpenFactory.

Configuration
-------------
The producer is configured via environment variables:

- OPCUA_CONNECTOR (str, required)
    JSON string describing the OPC UA connector configuration. Must conform to
    `OPCUAConnectorSchema`.

- OPCUA_PRODUCER_UUID (str, required)
    UUID for this producer instance.

- KSQLDB_URL (str, required)
    URL of the KSQLDB server for OpenFactory.

- KAFKA_BROKER (str, required)
    Address of the Kafka broker.

- DEVICE_UUID (str, required)
    UUID of the OpenFactory device that this producer is collecting data.
"""

import os
import asyncio
import json
import logging
import traceback
from numbers import Number
from datetime import datetime
from asyncua import Client, ua
from asyncua.common.node import Node
from asyncua.common.subscription import Subscription
from typing import Optional, Any
from openfactory.assets import Asset, AssetAttribute
from openfactory.assets.utils import openfactory_timestamp, current_timestamp
from openfactory.apps import OpenFactoryApp
from openfactory.kafka import KSQLDBClient
from openfactory.schemas.connectors.opcua import OPCUAConnectorSchema


def opcua_data_timestamp(data: ua.DataValue) -> str:
    """
    Extract the most relevant timestamp from an OPC UA DataValue.

    Priority order:
        1. SourceTimestamp (preferred, device time)
        2. ServerTimestamp (fallback, server processing time)
        3. Current system time (last resort)

    Args:
        data (ua.DataValue): The OPC UA DataValue to extract timestamps from.

    Returns:
        str: OpenFactory-formatted timestamp string.
    """
    if data.SourceTimestamp:
        return openfactory_timestamp(data.SourceTimestamp)
    if data.ServerTimestamp:
        return openfactory_timestamp(data.ServerTimestamp)
    return current_timestamp()


def opcua_event_timestamp(event: Any) -> str:
    """
    Extract the most relevant timestamp from an OPC UA event.

    Priority order:
        1. event.Time
        2. event.ReceiveTime
        3. Current UTC time

    Args:
        event (Any): OPC UA event object.

    Returns:
        str: OpenFactory-formatted timestamp string.
    """
    event_time = getattr(event, "Time", None)
    receive_time = getattr(event, "ReceiveTime", None)

    if isinstance(event_time, datetime):
        return openfactory_timestamp(event_time)
    if isinstance(receive_time, datetime):
        return openfactory_timestamp(receive_time)
    return current_timestamp()


class SubscriptionHandler:
    """
    Handler for OPC UA subscriptions, managing both data changes and events.

    This class provides callback methods for OPC UA client subscriptions:
      - `datachange_notification` for monitored variable changes.
      - `event_notification` for OPC UA events such as alarms or conditions.

    It enriches the notifications with metadata (local name, browse name,
    timestamps) and forwards them to the associated OpenFactory device.
    """

    def __init__(self, opcua_device: Asset, logger: logging.Logger):
        """
        Initialize the SubscriptionHandler.

        Args:
            opcua_device (Asset): The OpenFactory device object where attributes will be forwarded.
            logger (logging.Logger): Logger instance for debug and error messages.

        Attributes:
            node_map (Dict[Node, Dict[str, str]]): Cache mapping OPC UA Node objects to metadata.
        """
        self.opcua_device = opcua_device
        self.logger = logger

        # Cache mapping: Node -> {"local_name": str, "browse_name": str}
        self.node_map: dict = {}

    async def datachange_notification(self, node: Node, val: object, data: ua.DataChangeNotification) -> None:
        """
        Handle OPC UA data change notifications.

        This method is called by the OPC UA client subscription handler whenever
        a monitored variable's value changes. It enriches the notification with
        metadata (local name, browse name, and timestamp) and forwards the update
        into the OpenFactory pipeline.

        Numeric values are sent as 'Samples', non-numeric as 'Events'.
        Values with bad or uncertain StatusCode are reported as 'UNAVAILABLE'.

        Args:
            node (Node): The OPC UA node that triggered the data change.
            val (object): The new value of the variable.
            data (ua.DataChangeNotification): The notification containing the
                DataValue, including timestamps and status information.

        Returns:
            None
        """

        # Extract variable name
        info = self.node_map.get(node, {})
        local_name = info.get("local_name", "<unknown>")
        browse_name = info.get("browse_name", "<unknown>")

        # Extract DataValue
        data_value: ua.DataValue = data.monitored_item.Value

        # Determine OpenFactory type based on value type
        if isinstance(val, Number):
            ofa_type = "Samples"
        else:
            ofa_type = "Events"

        # Determine if value is valid
        if not data_value.StatusCode.is_good():
            self.logger.warning(
                f"Received bad or uncertain value for {local_name} ({browse_name}): "
                f"StatusCode={data_value.StatusCode}"
            )
            val = 'UNAVAILABLE'

        self.logger.debug(f"DataChange: {local_name}:({browse_name}) -> {val}")
        self.opcua_device.add_attribute(
            attribute_id=local_name,
            asset_attribute=AssetAttribute(
                value=val,
                type=ofa_type,
                tag=browse_name,
                timestamp=opcua_data_timestamp(data.monitored_item.Value)
            )
        )

    async def event_notification(self, event: Any) -> None:
        """
        Handle OPC UA event notifications.

        This callback is invoked when the client receives an event notification
        from the server (e.g., alarms, conditions, or system events). It extracts
        key fields such as message, severity, active state, source, and timestamp,
        and forwards them as an OpenFactory attribute.

        Args:
            event (Any): The OPC UA event object delivered by the subscription.

        Returns:
            None
        """
        try:
            message = getattr(event, "Message", None)
            severity = getattr(event, "Severity", None)
            active = getattr(event, "ActiveState", None)
            source = getattr(event, "SourceName", None)

            # Message is usually a LocalizedText, so extract .Text
            if message and hasattr(message, "Text"):
                message_text = message.Text
            else:
                message_text = str(message)

            # Add severity to message
            if severity:
                message_text = f"{message_text} (Severity: {severity})"

            # Determine Condtion tag based on active state
            tag = 'Fault'
            if active and hasattr(active, "EffectiveDisplayName"):
                if active.EffectiveDisplayName != "Active":
                    tag = 'Normal'

            self.logger.debug(f"Event from {source} (severity {severity}): {message_text}")

        except Exception as e:
            self.logger.error(f"Error parsing event: {e}, raw event={event}")
            message_text = "UNAVAILABLE"

        self.opcua_device.add_attribute(
            attribute_id='alarm',
            asset_attribute=AssetAttribute(
                value=message_text,
                type='Condition',
                tag=tag,
                timestamp=opcua_event_timestamp(event)

            )
        )


class OPCUAProducer(OpenFactoryApp):
    """
    OPC UA Producer for OpenFactory.

    This producer connects to an OPC UA server, subscribes to the configured
    device's variables and events, and streams updates into the OpenFactory
    pipeline. The configuration is provided via an environment
    variable `OPCUA_CONNECTOR` in JSON format, validated against
    `OPCUAConnectorSchema`.
    """

    def welcome_banner(self) -> None:
        """
        Welcome banner printed to stdout.
        """
        connector_env = os.environ.get("OPCUA_CONNECTOR")
        connector_dict = json.loads(connector_env)
        schema = OPCUAConnectorSchema(**connector_dict)
        url = schema.server.uri
        ns = schema.server.namespace_uri

        print("--------------------------------------------------------------")
        print(f"Starting Kafka Producer {self.asset_uuid}")
        print("--------------------------------------------------------------")
        print("OPC UA connection configuration")
        print("URL:", url)
        print("URI:", ns)
        print("--------------------------------------------------------------")

    async def async_main_loop(self) -> None:
        """
        Main asynchronous loop for the OPC UA producer.

        Steps:
            1. Load connector configuration from the environment.
            2. Connect to the OPC UA server.
            3. Resolve the device node via path or NodeId.
            4. Subscribe to all configured variables and events.
            5. Keep the connection alive and automatically reconnect on errors.

        Raises:
            ValueError: If the configuration is invalid (e.g., missing path/node_id).
            Exception: Any runtime OPC UA client errors will be caught and logged, with automatic retries after a short delay.
        """
        connector_env = os.environ.get("OPCUA_CONNECTOR")
        connector_dict = json.loads(connector_env)
        schema = OPCUAConnectorSchema(**connector_dict)

        self.opcua_device = Asset(
            asset_uuid=os.environ.get("DEVICE_UUID"),
            ksqlClient=self.ksql,
            bootstrap_servers=os.environ.get("KAFKA_BROKER"))

        while True:
            sub: Optional[Subscription] = None
            try:
                async with Client(schema.server.uri) as client:

                    # Resolve namespace index
                    idx = await client.get_namespace_index(schema.server.namespace_uri)

                    # Resolve device node
                    if schema.device.path:
                        path_parts = [f"{idx}:{p}" for p in schema.device.path.split("/")]
                        device_node = await client.nodes.objects.get_child(path_parts)
                    else:
                        # Using NodeId directly
                        device_node = client.get_node(schema.device.node_id)

                    # Prepare subscription
                    handler = SubscriptionHandler(self.opcua_device, self.logger)
                    sub = await client.create_subscription(500, handler)

                    # Subscribe to OPC UA variables
                    if schema.device.variables:
                        for local_name, browse_name in schema.device.variables.items():
                            var_node = await device_node.get_child([f"{idx}:{browse_name}"])
                            await sub.subscribe_data_change(var_node)

                            qname = await var_node.read_browse_name()
                            handler.node_map[var_node] = {
                                "local_name": local_name,
                                "browse_name": qname.Name,
                            }

                            self.logger.info(f"Subscribed to variable {local_name} ({browse_name})")

                    # Subscribe to OPC UA events
                    await sub.subscribe_events(device_node)
                    self.logger.info("Subscribed to events on device node")

                    # Tag OPC UA device as available
                    self.logger.info(f"Connected to OPC UA server at {schema.server.uri}")
                    self.opcua_device.add_attribute('avail', AssetAttribute(
                        value='AVAILABLE',
                        tag='Availability',
                        type='Events'
                    ))

                    # Keep connection alive
                    while True:
                        await asyncio.sleep(1)
                        # simple read to detect disconnection
                        await device_node.read_display_name()

            except Exception as e:
                self.logger.error(f"OPC UA client error: {type(e).__name__}: {e}")
                tb = traceback.format_exc()
                self.logger.debug(f"Traceback:\n{tb}")
                self.opcua_device.add_attribute('avail', AssetAttribute(
                        value='UNAVAILABLE',
                        tag='Availability',
                        type='Events'
                    ))
                await asyncio.sleep(2)


if __name__ == "__main__":
    producer = OPCUAProducer(
        app_uuid=os.getenv("OPCUA_PRODUCER_UUID"),
        ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL")),
        bootstrap_servers=os.getenv("KAFKA_BROKER"),
        loglevel=os.getenv("OPCUA_PRODUCER_LOG_LEVEL", "INFO")
    )
    asyncio.run(producer.async_run())
