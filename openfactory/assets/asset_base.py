""" OpenFactory Assets Base class. """

from __future__ import annotations
import json
import re
import time
import uuid
import threading
import asyncio
from typing import Literal, List, Dict, Any, Callable, Self
import openfactory.config as config
from openfactory.exceptions import OFAException
from openfactory.kafka import KSQLDBClient, KafkaAssetConsumer, KafkaAssetUNSConsumer, AssetProducer, CaseInsensitiveDict
from openfactory.assets.utils import AssetAttribute, AssetNATSCallback, AsyncLoopThread, NATSSubscriber, get_nats_cluster_url


class BaseAsset:
    """
    Base class for OpenFactory Assets.

    Warning:
        This is an abstract class not intented to be used.
        From this class, two classes are derived (`Asset` and `AssetUNS`) for actual usage.

    It can interact with the Kafka topic of the OpenFactory assets or the ksqlDB streams
    and state tables.

    Note:
        - All write operations to the asset take place in the `assets` stream.
        - NATS subscribers allow filtering messages by TYPE ('Samples', 'Events', 'Condition').

    Attributes:
        KSQL_ASSET_TABLE (str): Name of ksqlDB table of asset states (`assets` or `assets_uns`).
        KSQL_ASSET_ID (str): ksqlDB ID used to identify the asset (`asset_uuid` or `uns_id`) in the KSQL_ASSET_TABLE.
        ASSET_ID (str): Value of the identifier of the asset (asset_uuid or uns_id) used in the KSQL_ASSET_TABLE.
        ksql (KSQLDBClient): Client for interacting with ksqlDB.
        bootstrap_servers (str): Kafka bootstrap server address.
        ASSET_CONSUMER_CLASS (KafkaAssetConsumer|KafkaAssetUNSConsumer): Kafka consumer class for reading messages from asset stream.
        producer (AssetProducer): Shared Kafka producer instance used to publish asset messages (singleton across all BaseAsset subclasses).
        loop_thread (AsyncLoopThread): Async event loop thread used for NATS subscriptions.
        subscribers (dict): Mapping of subscription keys to NATSSubscriber instances.
    """

    _shared_producer: AssetProducer = None   # class-level singleton producer

    KSQL_ASSET_TABLE = None
    KSQL_ASSET_ID = None
    ASSET_ID = None
    ASSET_CONSUMER_CLASS = None

    def __init__(self, ksqlClient: KSQLDBClient, bootstrap_servers: str = config.KAFKA_BROKER) -> None:
        """
        Initializes the Asset with metadata.

        Args:
            ksqlClient (KSQLDBClient): Client for interacting with ksqlDB.
            bootstrap_servers (str): Kafka bootstrap server address. Defaults to config setting.
        """
        if not hasattr(self, 'KSQL_ASSET_TABLE') or self.KSQL_ASSET_TABLE is None:
            raise ValueError("KSQL_ASSET_TABLE must be set before initializing the Asset.")
        if not hasattr(self, 'KSQL_ASSET_ID') or self.KSQL_ASSET_ID is None:
            raise ValueError("KSQL_ASSET_ID must be set before initializing the Asset.")
        if not hasattr(self, 'ASSET_ID') or self.ASSET_ID is None:
            raise ValueError("ASSET_ID must be set before initializing the Asset like so `object.__setattr__(self, 'ASSET_ID', <your value>)`")
        if not hasattr(self, 'ASSET_CONSUMER_CLASS') or self.ASSET_CONSUMER_CLASS is None:
            raise ValueError("ASSET_CONSUMER_CLASS must be set before initializing the Asset.")
        if not issubclass(self.ASSET_CONSUMER_CLASS, (KafkaAssetConsumer, KafkaAssetUNSConsumer)):
            raise TypeError("ASSET_CONSUMER_CLASS must be a subclass of KafkaAssetConsumer or KafkaAssetUNSConsumer.")

        super().__setattr__('ksql', ksqlClient)
        super().__setattr__('bootstrap_servers', bootstrap_servers)
        super().__setattr__('loop_thread', AsyncLoopThread())
        super().__setattr__('subscribers', {})

        # Initialize the shared producer once
        if BaseAsset._shared_producer is None:
            BaseAsset._shared_producer = AssetProducer(
                ksqlClient=ksqlClient,
                bootstrap_servers=bootstrap_servers
            )

        # Use shared producer
        super().__setattr__('producer', BaseAsset._shared_producer)

    def close(self):
        """
        Gracefully closes the Asset and frees ressources.

        Steps performed:
            1. Stops all NATS subscribers (unsubscribe + close NATS connection).
            2. Cancels any remaining tasks in the AsyncLoopThread.
            3. Stops the AsyncLoopThread and joins the thread.

        .. warning::
            After calling this method, the Asset instance should not be used again.
        """
        # Stop all NATS subscribers
        for key, sub in list(self.subscribers.items()):
            try:
                sub.stop()
            except Exception as e:
                print(f"Warning: failed to close NATS subscriber {key}: {e}")
        self.subscribers.clear()

        # Cancel any remaining pending tasks in the loop
        loop = self.loop_thread.loop
        pending = asyncio.all_tasks(loop=loop)
        for task in pending:
            task.cancel()
        if pending:
            try:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass  # ignore exceptions from cancelled tasks

        # Stop the AsyncLoopThread
        self.loop_thread.stop()

    @property
    def asset_uuid(self) -> str:
        """
        Returns the asset UUID.

        Important:
            This property must be implemented by subclasses. It is expected to return
            the current asset UUID dynamically, based on runtime state.

        Returns:
            str: The asset's UUID.

        Raises:
            NotImplementedError: If the property is not implemented in a subclass.
        """
        raise NotImplementedError("Subclasses must implement asset_uuid property")

    @property
    def type(self) -> Literal['Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE']:
        """
        Retrieves the type of the asset from ksqlDB.

        Executes a SQL query to fetch the asset type. If the query returns no result,
        the method defaults to 'UNAVAILABLE'.

        Returns:
            Literal['Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE']:
                The asset type as stored in the `assets_type` table, or 'UNAVAILABLE' if not found.
        """
        query = f"SELECT TYPE FROM assets_type WHERE ASSET_UUID='{self.asset_uuid}';"
        result = self.ksql.query(query)

        if not result:  # empty list
            return 'UNAVAILABLE'

        return result[0]['TYPE']

    def attributes(self) -> List[str]:
        """
        Returns all non-'Method' attribute IDs associated with this asset.

        Queries `KSQL_ASSET_TABLE` for all attribute IDs of the asset where the type is not 'Method'.

        Returns:
            List[str]: A list of attribute IDs.
        """
        query = f"SELECT ID FROM {self.KSQL_ASSET_TABLE} WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}' AND TYPE != 'Method';"
        result = self.ksql.query(query)
        return [row['ID'] for row in result]

    def _get_attributes_by_type(self, attr_type: str) -> List[Dict[str, Any]]:
        """
        Generic method to retrieve all attributes from the `KSQL_ASSET_TABLE` of a given TYPE.

        Args:
            attr_type (str): The type of the asset attribute ('Samples', 'Events', 'Condition').

        Returns:
            List[Dict]: A list of dictionaries containing 'ID', 'VALUE', and cleaned 'TAG'.
        """
        query = f"SELECT ID, VALUE, TAG, TYPE FROM {self.KSQL_ASSET_TABLE} WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}' AND TYPE='{attr_type}';"
        result = self.ksql.query(query)
        return [
            {
                "ID": row["ID"],
                "VALUE": row["VALUE"],
                "TAG": re.sub(r'\{.*?\}', '', row["TAG"]).strip()
            }
            for row in result
        ]

    def samples(self) -> List[Dict[str, Any]]:
        """
        Returns all sample-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - "ID" (str): The attribute ID.
                - "VALUE" (Any): The value of the sample.
                - "TAG" (str): The cleaned tag name with placeholders removed.
        """
        return self._get_attributes_by_type('Samples')

    def events(self) -> List[Dict[str, Any]]:
        """
        Returns all event-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - "ID" (str): The attribute ID.
                - "VALUE" (Any): The value of the event.
                - "TAG" (str): The cleaned tag name with placeholders removed.
        """
        return self._get_attributes_by_type('Events')

    def conditions(self) -> List[Dict[str, Any]]:
        """
        Returns all condition-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - "ID" (str): The attribute ID.
                - "VALUE" (Any): The value of the condition.
                - "TAG" (str): The condition tag ('Normal', 'Warning', 'Fault')
        """
        return self._get_attributes_by_type('Condition')

    def methods(self) -> Dict[str, Any]:
        """
        Returns method-type attributes for this asset.

        Queries `KSQL_ASSET_TABLE` for entries where `TYPE = 'Method'` for the asset.

        Returns:
            Dict: A dictionary where keys are method attribute IDs and values are the corresponding method values.
        """
        query = f"SELECT ID, VALUE, TYPE FROM {self.KSQL_ASSET_TABLE} WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}' AND TYPE='Method';"
        result = self.ksql.query(query)
        return {row["ID"]: row["VALUE"] for row in result}

    def method(self, method: str, args: str = "") -> None:
        """
        Requests the execution of a method for the asset by sending a command to the Kafka stream.

        Constructs a message with the provided method name and optional arguments, and sends
        it to the `CMDS_STREAM` Kafka topic for processing.

        Args:
            method (str): The name of the method to be executed.
            args (str): Arguments for the method, if any. Defaults to an empty string.
        """
        msg = {
            "CMD": method,
            "ARGS": args
        }
        self.producer.produce(topic=self.ksql.get_kafka_topic('CMDS_STREAM'),
                              key=self.asset_uuid,
                              value=json.dumps(msg))
        self.producer.flush()

    def __getattr__(self, attribute_id: str) -> AssetAttribute | Callable[..., Any]:
        """
        Allows access to samples, events, conditions, and methods as attributes.

        Dynamically retrieves asset attributes (e.g. events, conditions, or methods)
        based on the `attribute_id` and returns them as an `AssetAttribute`.
        If the attribute is a method, it returns a callable function to execute that method.

        Args:
            attribute_id (str): The ID of the attribute being accessed.

        Returns:
            AssetAttribute/Callable:
                - If the attribute is a sample, event, or condition, returns an AssetAttribute.
                - If the attribute is a method, returns a callable method caller function.
        """
        query = f"SELECT VALUE, TYPE, TAG, TIMESTAMP FROM {self.KSQL_ASSET_TABLE} WHERE key='{self.ASSET_ID}|{attribute_id}';"
        result = self.ksql.query(query)

        if not result:
            return AssetAttribute(
                id=attribute_id,
                value='UNAVAILABLE',
                type='UNAVAILABLE',
                tag='UNAVAILABLE',
                timestamp='UNAVAILABLE'
            )

        first_row = result[0]

        if first_row['TYPE'] == 'Method':
            def method_caller(*args, **kwargs):
                args_str = " ".join(map(str, args))
                return self.method(attribute_id, args_str)
            return method_caller

        return AssetAttribute(
            id=attribute_id,
            value=float(first_row['VALUE']) if first_row['TYPE'] == 'Samples' and first_row['VALUE'] != 'UNAVAILABLE' else first_row['VALUE'],
            type=first_row['TYPE'],
            tag=first_row['TAG'],
            timestamp=first_row['TIMESTAMP']
        )

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Sets attributes on the Asset object and sends updates to Kafka.

        Overrides the default attribute setting behavior. If the attribute name
        exists in the asset's defined attributes (`self.attributes()`), it updates the attribute's
        value and sends the update to Kafka using the asset's producer.

        If the attribute is **not** a defined Asset attribute:
        - It is treated as a regular class attribute and set normally.
        - If the value is an instance of `AssetAttribute`, an exception is raised to prevent
        accidentally setting asset-specific attributes outside the defined schema.

        If the attribute **is** a defined Asset attribute:
        - If the value is an `AssetAttribute`, it is sent directly.
        - If the value is a raw value (e.g., int, str, etc.), it wraps the value in an
        `AssetAttribute` using the current attribute’s metadata (tag, type) and sends it.

        **Notes**:
            If a new class attribute has to be defined in the constructor of the child class, one has to use
            ```python
            object.__setattr__(self, 'new_class_attribute', <some value>)
            ```
            to avoid `RecursionError`

        Args:
            name (str): The name of the attribute being set.
            value (Any): The value to assign to the attribute. This can be a raw value or an `AssetAttribute`.

        Raises:
            OFAException: If the attribute is not defined in the asset but the value is an `AssetAttribute`.
        """
        # if not an Asset attributes, handle it as a class attribute
        if name not in self.attributes():
            if isinstance(value, AssetAttribute):
                raise OFAException(f"The attribute {name} is not defined in the asset {self.ASSET_ID}. Use the `add_attribute` method to define a new asset attribute.")
            super().__setattr__(name, value)
            return

        # check if value is of type AssetAttribute
        if isinstance(value, AssetAttribute):
            if value.id != name:
                raise OFAException(f"The AssetAttribute.id {value.id} does not match the attribute {name}")
            self.producer.send_asset_attribute(self.asset_uuid, value)
            return

        # send kafka message
        attr = self.__getattr__(name)
        self.producer.send_asset_attribute(
            self.asset_uuid,
            AssetAttribute(
                id=name,
                value=value,
                tag=attr.tag,
                type=attr.type)
            )

    def add_attribute(self, asset_attribute: AssetAttribute) -> None:
        """
        Adds a new attribute to the asset.

        Args:
            asset_attribute (AssetAttribute): The attribute to be added.
        """
        self.producer.send_asset_attribute(self.asset_uuid, asset_attribute)

    def _get_reference_list(self, direction: str, as_assets: bool = False) -> List[str | Self]:
        """
        Retrieves a list of asset-references (identifiers or asset objects) in the given direction.

        Important:
            This method must be implemented by subclasses.

        Args:
            direction (str): Either 'above' or 'below', indicating reference direction.
            as_assets (bool): If True, returns asset instances instead of asset-references.

        Returns:
            List: List of asset-references or asset objects.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError("Subclasses must implement _get_reference_list()")

    def references_above_uuid(self) -> List[str]:
        """
        Retrieves a list of asset-references of assets above the current asset.

        Returns:
            List[str]: A list of asset-references (as strings) that are above the current asset.
        """
        return self._get_reference_list(direction="above", as_assets=False)

    def references_above(self) -> List[Self]:
        """
        Retrieves a list of assets above the current asset.

        Returns:
            List[Self]: A list of asset objects that are above the current asset.
        """
        return self._get_reference_list(direction="above", as_assets=True)

    def references_below_uuid(self) -> List[str]:
        """
        Retrieves a list of asset-references below the current asset.

        Returns:
            List[str]: A list of asset-references (as strings) that are below the current asset.
        """
        return self._get_reference_list(direction="below", as_assets=False)

    def references_below(self) -> List[Self]:
        """
        Retrieves a list of assets below the current asset.

        Returns:
            List[Self]: A list of asset objects that are below the current asset.
        """
        return self._get_reference_list(direction="below", as_assets=True)

    def _add_reference(self, direction: str, new_reference: str) -> None:
        """
        Adds a reference to another asset in the specified direction.

        Args:
            direction (str): Either "above" or "below".
            new_reference (str): identifier of the asset to add as a reference.
        """
        key = f"{self.asset_uuid}|references_{direction}"
        query = f"SELECT VALUE FROM assets WHERE key='{key}';"
        result = self.ksql.query(query)  # list of dicts

        # Determine existing references
        if not result or not result[0].get("VALUE", "").strip():
            references = new_reference
        else:
            references = f"{new_reference}, {result[0]['VALUE'].strip()}"

        self.producer.send_asset_attribute(
            self.asset_uuid,
            AssetAttribute(
                id=f"references_{direction}",
                value=references,
                tag="AssetsReferences",
                type="OpenFactory"
            )
        )

    def add_reference_above(self, above_asset_reference: str) -> None:
        """
        Adds a reference to an asset above the current asset.

        Args:
            above_asset_reference (str): The asset-reference of the asset above the current one to be added.
        """
        self._add_reference(direction="above", new_reference=above_asset_reference)

    def add_reference_below(self, below_asset_reference: str) -> None:
        """
        Adds a reference to an asset below the current asset.

        Args:
            below_asset_reference (str): The asset-reference of the asset below the current one to be added.
        """
        self._add_reference(direction="below", new_reference=below_asset_reference)

    def wait_until(self, attribute_id: str, value: Any, timeout: int = 30, use_ksqlDB: bool = False) -> bool:
        """
        Waits until the asset attribute has a specific value or times out.

        Monitors either the NATS cluster or ksqlDB to check if the attribute value matches the expected value.
        Returns True if the value is found within the given timeout, False otherwise.

        Args:
            attribute_id (str): The attribute ID of the asset to monitor.
            value (Any): The value to wait for the attribute to match.
            timeout (int): The maximum time to wait, in seconds. Default is 30 seconds.
            use_ksqlDB (bool): If `True`, uses ksqlDB instead of NATS to check the attribute value. Default is `False`.

        Returns:
            bool: `True` if the attribute value matches the expected value within the timeout, `False` otherwise.
        """
        # First, check the current attribute value
        if self.__getattr__(attribute_id).value == value:
            return True

        start_time = time.time()

        if use_ksqlDB:
            while True:
                # Check for timeout
                if (time.time() - start_time) > timeout:
                    return False
                if self.__getattr__(attribute_id).value == value:
                    return True
                time.sleep(0.1)

        event = threading.Event()
        result = {"found": False}

        def on_message(subject: str, msg_value: dict):
            msg_value = CaseInsensitiveDict(msg_value)
            if msg_value.get("type") == "Samples" and msg_value.get("value") != "UNAVAILABLE":
                try:
                    if float(msg_value["value"]) == value:
                        result["found"] = True
                        event.set()
                except ValueError:
                    pass
            else:
                if msg_value.get("value") == value:
                    result["found"] = True
                    event.set()

        sub_key = f"wait_{attribute_id}_{uuid.uuid4()}"
        self.__start_nats_consumer(f"{self.asset_uuid.upper()}.{attribute_id}", on_message, sub_key=sub_key)

        finished = event.wait(timeout=timeout)

        self.__stop_subscription(sub_key)

        return finished and result["found"]

    def __start_nats_consumer(self, subject: str, on_message, sub_key: str):
        """
        Starts a NATS subscriber and stores it in self.subscribers with a unique key.
        """
        sub = NATSSubscriber(self.loop_thread, get_nats_cluster_url(self.asset_uuid), subject, on_message)
        sub.start()
        self.subscribers[sub_key] = sub

    def __stop_subscription(self, subject: str) -> None:
        """
        Stops a NATS subscription and cleans up associated resources.

        Args:
            subject (str): Subject of NATS subsription to stop
        """
        sub = self.subscribers.pop(subject, None)
        if sub:
            sub.stop()

    def subscribe_to_attribute(self, attribute_id: str, on_message: AssetNATSCallback) -> None:
        """
        Subscribes to changes of an asset attribute using a NATS consumer.

        Args:
            attribute_id (str): The attribute ID to monitor.
            on_message (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict) and handles messages.
        """
        subject = f"{self.asset_uuid.upper()}.{attribute_id}"
        sub_key = f"subscribe_to_attribute_{attribute_id}"
        self.__start_nats_consumer(subject, on_message, sub_key=sub_key)

    def subscribe_to_messages(self, on_message: AssetNATSCallback) -> None:
        """
        Subscribes to asset messages using a NATS consumer.

        Args:
            on_message (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict) and handles messages.
        """
        subject = f"{self.asset_uuid.upper()}.*"
        self.__start_nats_consumer(subject, on_message, sub_key="messages")

    def stop_attribute_subscription(self, attribute_id: str) -> None:
        """
        Stops the NATS consumer and gracefully shuts down the subscription.

        Args:
            attribute_id (str): The attribute ID to for which to stop the subscription.
        """
        self.__stop_subscription(f"subscribe_to_attribute_{attribute_id}")

    def stop_messages_subscription(self) -> None:
        """ Stops the NATS consumer and gracefully shuts down the subscription. """
        self.__stop_subscription("messages")

    def subscribe_to_samples(self, on_sample: AssetNATSCallback) -> None:
        """
        Subscribes to asset samples using a NATS consumer.
        Only messages with TYPE == 'Samples' are forwarded to the callback.

        Args:
            on_sample (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        subject = f"{self.asset_uuid.upper()}.*"

        def _filter_samples(msg_subject: str, msg_value: dict):
            if msg_value.get("TYPE") == "Samples":
                on_sample(msg_subject, msg_value)

        self.__start_nats_consumer(subject, _filter_samples, sub_key="samples")

    def stop_samples_subscription(self) -> None:
        """ Stops the NATS consumer and gracefully shuts down the subscription for samples. """
        self.__stop_subscription("samples")

    def subscribe_to_events(self, on_event: AssetNATSCallback) -> None:
        """
        Subscribes to asset events using a NATS consumer.
        Only messages with TYPE == 'Events' are forwarded to the callback.

        Args:
            on_event (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        subject = f"{self.asset_uuid.upper()}.*"

        def _filter_events(msg_subject: str, msg_value: dict):
            if msg_value.get("TYPE") == "Events":
                on_event(msg_subject, msg_value)

        self.__start_nats_consumer(subject, _filter_events, sub_key="events")

    def stop_events_subscription(self) -> None:
        """ Stops the NATS consumer and gracefully shuts down the subscription for events. """
        self.__stop_subscription("events")

    def subscribe_to_conditions(self, on_condition: AssetNATSCallback) -> None:
        """
        Subscribes to asset conditions using a NATS consumer.
        Only messages with TYPE == 'Condition' are forwarded to the callback.

        Args:
            on_condition (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        subject = f"{self.asset_uuid.upper()}.*"

        def _filter_conditions(msg_subject: str, msg_value: dict):
            if msg_value.get("TYPE") == "Condition":
                on_condition(msg_subject, msg_value)

        self.__start_nats_consumer(subject, _filter_conditions, sub_key="conditions")

    def stop_conditions_subscription(self) -> None:
        """ Stops the NATS consumer and gracefully shuts down the subscription for conditions. """
        self.__stop_subscription("conditions")
