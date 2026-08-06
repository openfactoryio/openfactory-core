""" OpenFactory Assets Base class. """

from __future__ import annotations
import os
import json
import re
import time
import threading
import asyncio
from operator import eq
from uuid import uuid4
from typing import Literal, List, Dict, Any, Callable, Self
from openfactory.exceptions import OFAException
from openfactory.kafka import KSQLDBClient, KafkaAssetConsumer, KafkaAssetUNSConsumer, AssetProducer, CaseInsensitiveDict
from openfactory.assets.utils import AssetAttribute, AssetNATSCallback, AsyncLoopThread, NATSSubscriber, get_nats_cluster_url
from openfactory.schemas.command_header import CommandEnvelope, CommandHeader


class BaseAsset:
    """
    Base class for OpenFactory Assets.

    Warning:
        This is an abstract base class and should not be instantiated directly.
        From this class, two classes are derived (:class:`Asset <openfactory.assets.asset_class.Asset>`
        and :class:`AssetUNS <openfactory.assets.asset_uns_class.AssetUNS>`) for actual usage.

    It can interact with the Kafka topic of the OpenFactory assets or the ksqlDB streams
    and state tables.

    Note:
        - All write operations to the asset take place in the ``assets`` stream.
        - A single NATS subscriber keeps the asset state synchronized and dispatches registered callbacks.

    Attributes:
        KSQL_ASSET_TABLE (str): Name of ksqlDB table of asset states (``assets`` or ``assets_uns``).
        KSQL_ASSET_ID (str): ksqlDB ID used to identify the asset (``asset_uuid`` or ``uns_id``) in the ``KSQL_ASSET_TABLE``.
        ASSET_ID (str): Value of the identifier of the asset (``asset_uuid`` or ``uns_id``) used in the ``KSQL_ASSET_TABLE``.
        ksql (KSQLDBClient): Client for interacting with ksqlDB.
        bootstrap_servers (str): Kafka bootstrap server address.
        asset_router_url (str): Asset Router URL from the OpenFactory Fan-Out-Layer.
        ASSET_CONSUMER_CLASS (KafkaAssetConsumer|KafkaAssetUNSConsumer): Kafka consumer class for reading messages from asset stream.
        producer (AssetProducer): Shared Kafka producer instance used to publish asset messages (singleton across all BaseAsset subclasses).
        loop_thread (AsyncLoopThread): Async event loop thread used for NATS subscriptions.
        ofa_attributes (Dict[str, AssetAttribute]): Dictionary mapping attribute IDs to their current AssetAttribute.
        ofa_methods (Dict[str, dict | None]): Dictionary mapping method IDs to their parsed method contracts.
        _condition (threading.Condition): Condition variable used by wait_until() to wait for attribute updates.
        _subscriber (NATSSubscriber): Permanent NATS subscriber responsible for synchronizing the internal state.
        _attribute_callbacks (Dict[str, AssetNATSCallback]): Registered callbacks invoked when a specific attribute changes.
        _messages_callback (AssetNATSCallback | None): Callback invoked for every received asset message.
        _samples_callback (AssetNATSCallback | None): Callback invoked for every received sample message.
        _events_callback (AssetNATSCallback | None): Callback invoked for every received event message.
        _conditions_callback (AssetNATSCallback | None): Callback invoked for every received condition message.
    """

    # Instance attributes
    ksql: KSQLDBClient
    bootstrap_servers: str | None
    asset_router_url: str | None
    producer: AssetProducer | None
    loop_thread: AsyncLoopThread | None

    _test_mode: bool
    _mocked_attributes: list[str]
    _shared_producer: AssetProducer = None   # class-level singleton producer

    KSQL_ASSET_TABLE = None
    KSQL_ASSET_ID = None
    ASSET_ID = None
    ASSET_CONSUMER_CLASS = None

    def __init__(
            self,
            ksqlClient: KSQLDBClient,
            bootstrap_servers: str | None = None,
            asset_router_url: str | None = None,
            test_mode: bool = False
            ) -> None:
        """
        Initializes the Asset with metadata.

        Args:
            ksqlClient (KSQLDBClient): Client for interacting with ksqlDB.
            bootstrap_servers (str | None): Kafka bootstrap server address.
            asset_router_url (str | None): Asset Router URL from the OpenFactory Fan-Out-Layer.
            test_mode (bool): If True, disables live Kafka/ksql interaction (useful for unit tests).

        Raises:
            ValueError: If any of the class-level attributes
                (``KSQL_ASSET_TABLE``, ``KSQL_ASSET_ID``, ``ASSET_ID``, ``ASSET_CONSUMER_CLASS``)
                are missing or invalid.
            TypeError: If ``ASSET_CONSUMER_CLASS`` is not a subclass of
                ``KafkaAssetConsumer`` or ``KafkaAssetUNSConsumer``.
            OFAException: If ``bootstrap_servers`` is not provided and the
                ``KAFKA_BROKER`` environment variable is not set.
            OFAException: If ``asset_router_url`` is not provided and the
                ``ASSET_ROUTER_URL`` environment variable is not set.

        Note:
          - If ``bootstrap_servers`` is not explicitly provided, the constructor will attempt to read it from the ``KAFKA_BROKER`` environment variable.
          - If ``asset_router_url`` is not explicitly provided, the constructor will attempt to read it from the ``ASSET_ROUTER_URL`` environment variable.
          - When used in an :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` deployed on the OpenFactory cluster, the environment variables ``KAFKA_BROKER`` and ``ASSET_ROUTER_URL`` will be set.
        """
        super().__setattr__('ofa_attributes', {})
        super().__setattr__('ofa_methods', {})
        self._condition: threading.Condition = threading.Condition()
        self._attribute_callbacks: dict[str, AssetNATSCallback] = {}
        self._messages_callback: AssetNATSCallback | None = None
        self._samples_callback: AssetNATSCallback | None = None
        self._events_callback: AssetNATSCallback | None = None
        self._conditions_callback: AssetNATSCallback | None = None
        self._test_mode = test_mode

        # If in test mode, skip all runtime checks and producer setup
        if test_mode:
            self.ksql = ksqlClient
            self.loop_thread = None
            self.bootstrap_servers = bootstrap_servers
            self.asset_router_url = asset_router_url
            self.producer = None
            return

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

        self.ksql = ksqlClient
        self.loop_thread = AsyncLoopThread()

        if bootstrap_servers is None:
            bootstrap_servers = os.getenv("KAFKA_BROKER")
        if not bootstrap_servers:
            raise OFAException(
                "OpenFactory BaseAsset requires 'bootstrap_servers' to be provided "
                "either explicitly or via the KAFKA_BROKER environment variable."
            )
        self.bootstrap_servers = bootstrap_servers

        if asset_router_url is None:
            asset_router_url = os.getenv("ASSET_ROUTER_URL")
        if not asset_router_url:
            raise OFAException(
                "OpenFactory BaseAsset requires 'asset_router_url' to be provided "
                "either explicitly or via the ASSET_ROUTER_URL environment variable."
            )
        self.asset_router_url = asset_router_url

        # Initialize the shared producer once
        if BaseAsset._shared_producer is None:
            BaseAsset._shared_producer = AssetProducer(
                ksqlClient=ksqlClient,
                bootstrap_servers=bootstrap_servers
            )

        # Use shared producer
        self.producer = BaseAsset._shared_producer

        # Start NATS subscription to update internal state
        self.__start_nats_consumer()

        # Retrieve current state from ksqlDB
        self.resync_state()

    def resync_state(self) -> None:
        """
        Resynchronizes the local asset state with the distributed state stored in ksqlDB.

        This method reloads all asset attributes and methods from ksqlDB and updates
        the local cache accordingly. Existing entries are overwritten with the latest
        values, while newly discovered attributes and methods are added. Existing
        entries that are not present in the retrieved state are left unchanged.

        This method is useful for recovering from missed updates or forcing the local
        cache to resynchronize with the distributed asset state.

        Note:
            This method does not restart the NATS subscription. It only refreshes
            the local cache from the current state materialized in ksqlDB.
        """
        self._fetch_attributes()
        self._fetch_methods()

    def close(self):
        """
        Closes the permanent NATS subscription and releases all resources owned by this BaseAsset.

        Steps performed:
            1. Stops the asset's NATS subscriber (unsubscribe + close NATS connection).
            2. Cancels any remaining tasks in the AsyncLoopThread.
            3. Stops the AsyncLoopThread and joins the thread.

        .. warning::
            After calling this method, the Asset instance should not be used again.
        """
        if self._test_mode:
            return

        # Stop NATS subscriber
        try:
            self._subscriber.stop()
        except Exception as e:
            print(f"Warning: failed to close NATS subscriber: {e}")

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
        if self.loop_thread:
            self.loop_thread.stop()

    def _parse_method_value(self, value: Any) -> Any:
        """
        Parses the VALUE field of a Method attribute.

        Method contracts are stored as JSON strings in ksqlDB and converted
        to their Python dictionary representation.
        """

        if value is None:
            return None

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        return value

    def _on_message(self, subject: str, msg: dict[str, Any]) -> None:
        """
        Processes an incoming NATS message.

        Updates the internal asset state, notifies waiting threads, and
        dispatches all registered callbacks.
        """
        msg = CaseInsensitiveDict(msg)
        attribute_id = subject.split(".", 1)[1]

        if msg['TYPE'] in {'Samples', 'Condition', 'Events', 'OpenFactory'}:
            attr = AssetAttribute(
                id=attribute_id,
                value=msg['VALUE'],
                type=msg['TYPE'],
                tag=msg['TAG'],
                timestamp=msg['attributes']['timestamp']
            )
            with self._condition:
                self.ofa_attributes[attribute_id] = attr
                self._condition.notify_all()

            # Attribute callback
            callback = self._attribute_callbacks.get(attribute_id)
            if callback is not None:
                callback(subject, msg)

            # Global callback
            if self._messages_callback is not None:
                self._messages_callback(subject, msg)

            # Samples callback
            if (self._samples_callback is not None and msg["TYPE"] == "Samples"):
                self._samples_callback(subject, msg)

            # Events callback
            if (self._events_callback is not None and msg["TYPE"] == "Events"):
                self._events_callback(subject, msg)

            # Conditions callback
            if (self._conditions_callback is not None and msg["TYPE"] == "Condition"):
                self._conditions_callback(subject, msg)

        if msg["TYPE"] == "Method":
            self.ofa_methods[attribute_id] = self._parse_method_value(msg["VALUE"])

    def _fetch_attributes(self):
        """ Retrieves all non-method attributes from ksqlDB and initializes the internal ``ofa_attributes`` dictionary. """
        # test_mode
        if getattr(self, "_test_mode", False):
            return

        # in production query ksqlDB
        query = f"SELECT ID, VALUE, TYPE, TAG, TIMESTAMP FROM {self.KSQL_ASSET_TABLE} WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}' AND TYPE != 'Method';"
        result = self.ksql.query(query)
        for row in result:
            attr_value = row["VALUE"]
            if row["TYPE"] == "Samples" and attr_value is not None:
                try:
                    attr_value = float(attr_value)
                except (TypeError, ValueError):
                    pass

            attr = AssetAttribute(
                id=row['ID'],
                value=attr_value,
                type=row['TYPE'],
                tag=row['TAG'],
                timestamp=row['TIMESTAMP']
            )
            self.ofa_attributes[row['ID']] = attr

    def _fetch_methods(self):
        """ Retrieves the current methods of the asset from ksqlDB and initializes the internal ``ofa_methods`` dictionary. """
        # test_mode
        if getattr(self, "_test_mode", False):
            return

        # in production query ksqlDB
        query = f"SELECT ID, VALUE, TYPE FROM {self.KSQL_ASSET_TABLE} WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}' AND TYPE='Method';"
        result = self.ksql.query(query)

        for row in result:
            self.ofa_methods[row["ID"]] = self._parse_method_value(row["VALUE"])

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
        the method defaults to ``UNAVAILABLE``.

        Returns:
            Literal['Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE']:
                The asset type as stored in the ``assets_type`` table, or ``UNAVAILABLE`` if not found.
        """
        query = f"SELECT TYPE FROM assets_type WHERE ASSET_UUID='{self.asset_uuid}';"
        result = self.ksql.query(query)

        if not result:  # empty list
            return 'UNAVAILABLE'

        return result[0]['TYPE']

    def attributes(self) -> List[str]:
        """
        Returns the IDs of all attributes currently associated with this asset.

        Returns:
            List[str]: A list of attribute IDs.
        """
        return [attr.id for attr in self.ofa_attributes.values()]

    def _get_attributes_by_type(self, attr_type: str) -> List[Dict[str, Any]]:
        """
        Returns all asset attributes of the specified type.

        Args:
            attr_type (str): The type of the asset attribute ('Samples', 'Events', 'Condition').

        Returns:
            List[Dict]: A list of dictionaries containing 'ID', 'VALUE', and cleaned 'TAG'.
        """
        return [
            {
                "ID": attr.id,
                "VALUE": attr.value,
                "TAG": re.sub(r'\{.*?\}', '', attr.tag).strip()
            }
            for attr in self.ofa_attributes.values() if attr.type == attr_type
        ]

    def samples(self) -> List[Dict[str, Any]]:
        """
        Returns all sample-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - ``ID`` (str): The attribute ID.
                - ``VALUE`` (float): The value of the sample.
                - ``TAG`` (str): The cleaned tag name with placeholders removed.
        """
        return self._get_attributes_by_type('Samples')

    def events(self) -> List[Dict[str, Any]]:
        """
        Returns all event-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - ``ID`` (str): The attribute ID.
                - ``VALUE`` (Any): The value of the event.
                - ``TAG`` (str): The cleaned tag name with placeholders removed.
        """
        return self._get_attributes_by_type('Events')

    def conditions(self) -> List[Dict[str, Any]]:
        """
        Returns all condition-type attributes for this asset.

        Returns:
            List[Dict]: A list of dictionaries, each containing:
                - ``ID`` (str): The attribute ID.
                - ``VALUE`` (Any): The value of the condition.
                - ``TAG`` (str): The condition tag ('Normal', 'Warning', 'Fault')
        """
        return self._get_attributes_by_type('Condition')

    def methods(self) -> Dict[str, dict | None]:
        """
        Returns all methods associated with the asset.

        Returns:
            Dict[str, dict | None]:
                Dictionary mapping method IDs to their parsed method contract (description + arguments).
                Returns None if no value is stored.

        .. admonition:: Returned Dictionnary Example

           .. code-block:: json

             {
                "GenerateCode": {
                    "description": "GenerateCode",
                    "arguments": [
                      {
                        "name": "Code",
                        "description": "Barcode to generate (empty for random)"
                      }
                    ]
                },
                "SetAutomaticMode": {
                    "description": "SetAutomaticMode",
                    "arguments": []
                },
                "SetManualMode": {
                    "description": "SetManualMode",
                    "arguments": []
                }
             }
        """
        return self.ofa_methods

    def method(self, method: str, sender_uuid: str, args: list[tuple[str, str]] | None) -> str:
        """
        Requests the execution of a method for the asset.

        This function further sets the corresponding callable attribute with the name of the method
        (e.g. ``GenerateCode``) to trigger the command execution.

        Methods execution can be requested in two ways:

        1. Using the :meth:`method()` interface:

           .. code-block:: python

              asset.method('GenerateCode', sender_uuid='SENDER-ID', args=[('Code', '123')])

        2. Or directly via the generated callable attribute:

           .. code-block:: python

              asset.GenerateCode(sender_uuid='SENDER-ID', Code='123')

        In both cases, ``sender_uuid`` must be provided in addition to the command's named arguments.

        Note:
          Named arguments are case sensitive and can be discovered by calling :meth:`methods()`.

        Args:
            method (str): Name of the method to be executed.
            sender_uuid (str): Asset UUID of the asset sending the request.
            args (list[tuple[str, str]] | None): List of (argument_name, value) pairs.

                All values must be strings. Defaults to empty list if not provided.

        Returns:
            str: The correlation_id of the command, which can be used to track the response.
        """
        cmd_args = {name: value for name, value in (args or [])}

        correlation_id = uuid4()
        envelope = CommandEnvelope(
            header=CommandHeader(
                correlation_id=correlation_id,
                sender_uuid=sender_uuid,
                signature=None,
            ),
            arguments=cmd_args
        )

        # Set the attribute to trigger the command
        cmd_id = f"{method}_CMD"
        if cmd_id not in self.ofa_attributes:
            self.add_attribute(
                asset_attribute=AssetAttribute(
                    id=cmd_id,
                    value=envelope.model_dump_json(),
                    type='OpenFactory',
                    tag='Method.Command'
                )
            )
        else:
            self.__setattr__(cmd_id, envelope.model_dump_json())

        return str(correlation_id)

    def __getattr__(self, attribute_id: str) -> AssetAttribute | Callable[..., str]:
        """
        Allows access to samples, events, conditions, and methods as attributes.

        Returns an attribute or method from the internal asset state.
        If the attribute is a method, it returns a callable function to execute that method.

        Args:
            attribute_id (str): The ID of the attribute being accessed.

        Returns:
            AssetAttribute/Callable:
                - If the attribute is a sample, event, or condition, returns an AssetAttribute.
                - If the attribute is a method, returns a callable method caller function.
        """
        if attribute_id in self.ofa_methods:

            def method_caller(**kwargs: Any) -> str:
                """
                Executes the asset method with named string arguments.

                Special keyword:
                    sender_uuid (str): The UUID of the asset requesting the command.
                All other keyword arguments are treated as command arguments.

                Returns:
                    str: correlation_id of the command.
                """
                sender_uuid = kwargs.pop("sender_uuid", None)
                if not sender_uuid:
                    raise ValueError("sender_uuid must be provided for method execution")

                args_list = list(kwargs.items())
                return self.method(attribute_id, sender_uuid, args_list)

            return method_caller

        if attribute_id not in self.ofa_attributes:
            return AssetAttribute(
                id=attribute_id,
                value='UNAVAILABLE',
                type='UNAVAILABLE',
                tag='UNAVAILABLE',
                timestamp='UNAVAILABLE'
            )

        return self.ofa_attributes[attribute_id]

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Updates the local asset state and publishes attribute changes to Kafka.

        Asset attributes are immediately reflected in the local asset state before being
        published to the OpenFactory event stream.

        Overrides the default attribute setting behavior. If the attribute name corresponds
        to an existing asset attribute, its value is updated and published to the OpenFactory
        event stream.

        If the attribute is **not** a defined Asset attribute:
        - It is treated as a regular class attribute and set normally.
        - If the value is an instance of `AssetAttribute`, an exception is raised to prevent
        accidentally setting asset-specific attributes outside the defined schema.

        If the attribute **is** a defined Asset attribute:
        - If the value is an `AssetAttribute`, it is sent directly.
        - If the value is a raw value (e.g., int, str, etc.), it wraps the value in an
        `AssetAttribute` using the current attribute’s metadata (tag, type) and sends it.

        Note:
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
        if name in self.ofa_methods:
            raise OFAException(f"{name} is an Asset method. It can not be assigned.")

        # if not an Asset attributes, handle it as a class attribute
        if name not in self.ofa_attributes:
            if isinstance(value, AssetAttribute):
                raise OFAException(f"The attribute {name} is not defined in the asset {self.ASSET_ID}. Use the `add_attribute` method to define a new asset attribute.")
            super().__setattr__(name, value)
            return

        # check if value is of type AssetAttribute
        if isinstance(value, AssetAttribute):
            if value.id != name:
                raise OFAException(f"The AssetAttribute.id {value.id} does not match the attribute {name}")
            self.ofa_attributes[name] = value
            if not self._test_mode:
                self.producer.send_asset_attribute(self.asset_uuid, value)
            return

        # get the current AssetAttribute
        attr = self.ofa_attributes[name]

        new_attr = AssetAttribute(
            id=name,
            value=value,
            tag=attr.tag,
            type=attr.type
        )
        self.ofa_attributes[name] = new_attr

        # send kafka message
        if not self._test_mode:
            self.producer.send_asset_attribute(self.asset_uuid, new_attr)

    def add_attribute(self, asset_attribute: AssetAttribute, wait_to_become_available: bool = True) -> None:
        """
        Adds a new attribute to the local asset state and publishes it to the OpenFactory event stream.

        The attribute is immediately added to the local asset state before being published
        to the OpenFactory event stream.

        Args:
            asset_attribute (AssetAttribute): The attribute to be added.
            wait_to_become_available (bool): If True, waits until the newly added attribute has been materialized in ksqlDB before returning.
        """
        self.ofa_attributes[asset_attribute.id] = asset_attribute

        if self._test_mode:
            return

        self.producer.send_asset_attribute(self.asset_uuid, asset_attribute)
        if wait_to_become_available:
            self.wait_until(attribute_id=asset_attribute.id, value=asset_attribute.value, use_ksqlDB=True)

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

    def get_references_above_uuid(self) -> List[str]:
        """
        Retrieves a list of asset-references of assets above the current asset.

        Returns:
            List[str]: A list of asset-references (as strings) that are above the current asset.
        """
        return self._get_reference_list(direction="above", as_assets=False)

    def get_references_above(self) -> List[Self]:
        """
        Retrieves a list of assets above the current asset.

        Returns:
            List[Self]: A list of asset objects that are above the current asset.
        """
        return self._get_reference_list(direction="above", as_assets=True)

    def get_references_below_uuid(self) -> List[str]:
        """
        Retrieves a list of asset-references below the current asset.

        Returns:
            List[str]: A list of asset-references (as strings) that are below the current asset.
        """
        return self._get_reference_list(direction="below", as_assets=False)

    def get_references_below(self) -> List[Self]:
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

    def wait_until(self, attribute_id: str, value: Any, comparison: Callable[[Any, Any], bool] = eq,
                   *, timeout: int = 30, use_ksqlDB: bool = False) -> bool:
        """
        Waits until an asset attribute satisfies a comparison or times out.

        Waits until the specified asset attribute satisfies the given comparison
        against ``value``. By default, the comparison is performed against the
        asset's locally cached state, which is continuously updated from the
        OpenFactory event stream. If ``use_ksqlDB=True``, the comparison is instead
        performed against the distributed state materialized in ksqlDB, ensuring
        that the complete stream-processing pipeline has propagated the update.

        .. admonition:: Example usage:

            .. code-block:: python

                from operator import gt, ge, lt, le

                asset.wait_until("Execution", "ACTIVE")
                asset.wait_until("Temperature", 42)
                asset.wait_until("Temperature", 42, gt)
                asset.wait_until("Temperature", 42, ge)
                asset.wait_until("Temperature", 100, lt)
                asset.wait_until("Temperature", 100, le)

        Args:
            attribute_id (str): The attribute ID of the asset to monitor.
            value (Any): The reference value used for the comparison.
            comparison (Callable[[Any, Any], bool]):
                Function used to compare the current attribute value with
                ``value``. Defaults to :func:`operator.eq`.
            timeout (int): The maximum time to wait, in seconds. Default is 30 seconds.
            use_ksqlDB (bool):
                If ``False`` (default), waits for the local cached state to satisfy
                the comparison. If ``True``, waits until the distributed state
                materialized in ksqlDB satisfies the comparison.

        Returns:
            bool: `True` if the comparison is satisfied before the timeout expires, otherwise ``False``.
        """
        if not callable(comparison):
            raise TypeError(
                f"comparison must be a callable (e.g. operator.eq or operator.gt), "
                f"got {type(comparison).__name__}. "
                f"Did you mean to specify the timeout? "
                f"Use wait_until(..., timeout={comparison!r})."
            )

        # If not an attribute  raise
        if attribute_id not in self.ofa_attributes:
            raise OFAException(f"'{attribute_id}' is not an Attribute of Asset '{self.asset_uuid}'")

        # First, check the current attribute value
        if comparison(self.ofa_attributes[attribute_id].value, value):
            return True

        # Checks that the full stream-processing pipeline has completed
        if use_ksqlDB:
            start_time = time.time()

            while True:
                if (time.time() - start_time) > timeout:
                    return False

                query = (f"SELECT VALUE, TYPE, TIMESTAMP FROM {self.KSQL_ASSET_TABLE} WHERE key='{self.ASSET_ID}|{attribute_id}';")
                result = self.ksql.query(query)

                if result:
                    row = result[0]
                    current = row["VALUE"]
                    if row["TYPE"] == "Samples":
                        try:
                            current = float(current)
                        except (TypeError, ValueError):
                            pass
                    if comparison(current, value):
                        return True

                time.sleep(0.1)

        with self._condition:
            return self._condition.wait_for(
                lambda: comparison(
                    self.ofa_attributes[attribute_id].value,
                    value
                ),
                timeout=timeout,
            )

    def __start_nats_consumer(self):
        """ Starts the asset's NATS subscriber. """
        asset_uuid = self.asset_uuid
        self._subscriber = NATSSubscriber(
            self.loop_thread,
            get_nats_cluster_url(asset_uuid, self.asset_router_url),
            f"{asset_uuid.upper()}.*",
            self._on_message,
        )
        self._subscriber.start()

    def _restart_nats_subscription(self):
        """
        Restarts the asset's NATS subscriber.

        This method is used when the asset UUID changes after construction
        (for example, during initialization of derived classes).
        The subscriber is recreated using the new subject so that the internal state
        continues to receive updates for the correct asset.
        """

        # Nothing to do in test mode
        if self._test_mode:
            return

        # Stop current subscriber
        try:
            self._subscriber.stop()
        except Exception as e:
            print(f"Warning: failed to close NATS subscriber: {e}")

        # Cancel any remaining pending tasks in the loop
        loop = self.loop_thread.loop
        pending = asyncio.all_tasks(loop=loop)
        for task in pending:
            task.cancel()

        if pending:
            try:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            except Exception:
                pass

        # Recreate subscriber with the current asset UUID
        asset_uuid = self.asset_uuid
        self._subscriber = NATSSubscriber(
            self.loop_thread,
            get_nats_cluster_url(asset_uuid, self.asset_router_url),
            f"{asset_uuid.upper()}.*",
            self._on_message,
        )
        self._subscriber.start()

    def subscribe_to_attribute(self, attribute_id: str, on_message: AssetNATSCallback) -> None:
        """
        Registers a callback invoked whenever the specified asset attribute changes.

        Args:
            attribute_id (str): The attribute ID to monitor.
            on_message (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict) and handles messages.
        """
        if attribute_id in self._attribute_callbacks:
            raise OFAException(
                f"A callback is already registered for attribute '{attribute_id}'. "
                f"Call stop_attribute_subscription('{attribute_id}') before registering a new callback."
            )
        self._attribute_callbacks[attribute_id] = on_message

    def subscribe_to_messages(self, on_message: AssetNATSCallback) -> None:
        """
        Registers a callback invoked for every incoming asset message.

        Args:
            on_message (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict) and handles messages.
        """
        if self._messages_callback is not None:
            raise OFAException(
                "A callback is already registered for asset messages. "
                "Call stop_messages_subscription() before registering a new callback."
            )

        self._messages_callback = on_message

    def stop_attribute_subscription(self, attribute_id: str) -> None:
        """
        Unregisters the callback associated with the specified attribute.

        Args:
            attribute_id (str): The attribute ID to for which to stop the subscription.
        """
        self._attribute_callbacks.pop(attribute_id, None)

    def stop_messages_subscription(self) -> None:
        """ Unregisters the callback for asset messages. """
        self._messages_callback = None

    def subscribe_to_samples(self, on_sample: AssetNATSCallback) -> None:
        """
        Registers a callback invoked for incoming sample messages.

        Args:
            on_sample (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        if self._samples_callback is not None:
            raise OFAException(
                "A callback is already registered for asset samples. "
                "Call stop_samples_subscription() before registering a new callback."
            )

        self._samples_callback = on_sample

    def stop_samples_subscription(self) -> None:
        """ Unregisters the callback for sample messages. """
        self._samples_callback = None

    def subscribe_to_events(self, on_event: AssetNATSCallback) -> None:
        """
        Registers a callback invoked for incoming event messages.

        Args:
            on_event (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        if self._events_callback is not None:
            raise OFAException(
                "A callback is already registered for asset events. "
                "Call stop_events_subscription() before registering a new callback."
            )

        self._events_callback = on_event

    def stop_events_subscription(self) -> None:
        """ Unregisters the callback for event messages. """
        self._events_callback = None

    def subscribe_to_conditions(self, on_condition: AssetNATSCallback) -> None:
        """
        Registers a callback invoked for incoming condition messages.

        Args:
            on_condition (AssetNATSCallback): Callable that takes (msg_subject: str, msg_value: dict).
        """
        if self._conditions_callback is not None:
            raise OFAException(
                "A callback is already registered for asset conditions. "
                "Call stop_conditions_subscription() before registering a new callback."
            )

        self._conditions_callback = on_condition

    def stop_conditions_subscription(self) -> None:
        """ Unregisters the callback for condition messages. """
        self._conditions_callback = None
