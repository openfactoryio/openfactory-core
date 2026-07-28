""" OpenFactory AssetUNS class. """

from __future__ import annotations
import threading
from openfactory.assets.asset_base import BaseAsset
from openfactory.kafka import KafkaAssetUNSConsumer, KSQLDBClient
from openfactory.exceptions import OFAException


class AssetUNS(BaseAsset):
    """
    Represents an OpenFactory Asset using the UNS identifier.

    This class represents an OpenFactory Asset identified by its UNS identifier. It maintains
    a locally cached view of the Asset state synchronized with the OpenFactory platform while
    providing methods to publish attribute updates and invoke Asset methods.

    It uses the OpenFactory data model to retrieve the initial Asset state from ksqlDB
    while keeping the local cache synchronized through the OpenFactory event stream.

    Note:
        All write operations to the asset take place in the ``assets`` stream.

    Attributes:
        KSQL_ASSET_TABLE (str): Name of ksqlDB table of Asset states (``assets_uns``)
        KSQL_ASSET_ID (str): ksqlDB ID used to identify the Asset (``uns_id``) in the ``KSQL_ASSET_TABLE``
        ASSET_ID (str): value of the identifer of the Asset (``uns_id``) used in the ``KSQL_ASSET_TABLE``
        ksql (KSQLDBClient): Client for interacting with ksqlDB.
        bootstrap_servers (str): Kafka bootstrap server address.
        asset_router_url (str): Asset Router URL from the OpenFactory Fan-Out-Layer.
        ASSET_CONSUMER_CLASS (KafkaAssetUNSConsumer): Kafka consumer class for reading messages from Asset strean.
        producer (AssetProducer): Kafka producer instance for sending Asset messages.

    .. admonition:: Example usage:

        .. code-block:: python

            import time
            from openfactory.assets import AssetUNS
            from openfactory.kafka import KSQLDBClient

            ksql = KSQLDBClient('http://localhost:8088')
            cnc = AssetUNS('cnc-003', ksqlClient=ksql, bootstrap_servers='localhost:9092')

            # list samples
            print(cnc.samples())
            print(cnc.Zact.value)
            print(cnc.Zact.type)
            print(cnc.Zact.timestamp)

            # redefine an attribute value
            cnc.Zact = 10.0
            print(cnc.Zact.value)

            # callbacks for subscriptions
            def on_messages(msg_key, msg_value):
                print(f"[Message] [{msg_key}] {msg_value}")

            def on_sample(msg_key, msg_value):
                print(f"[Sample] [{msg_key}] {msg_value}")

            def on_event(msg_key, msg_value):
                print(f"[Event] [{msg_key}] {msg_value}")

            def on_condition(msg_key, msg_value):
                print(f"[Condition] [{msg_key}] {msg_value}")

            cnc.subscribe_to_messages(on_messages)
            cnc.subscribe_to_samples(on_sample)
            cnc.subscribe_to_events(on_event)
            cnc.subscribe_to_conditions(on_condition)

            # run a main loop while subscriptions remain active
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("Stopping subscriptions ...")
                cnc.stop_messages_subscription()
                cnc.stop_samples_subscription()
                cnc.stop_events_subscription()
                cnc.stop_conditions_subscription()
                print("Subscriptions stopped")
            finally:
                cnc.close()
                ksql.close()
    """

    KSQL_ASSET_TABLE = 'assets_uns'
    KSQL_ASSET_ID = 'uns_id'
    ASSET_CONSUMER_CLASS = KafkaAssetUNSConsumer

    _MAPPING_WATCH_INTERVAL = 2.0

    def __init__(self, uns_id: str,
                 ksqlClient: KSQLDBClient,
                 bootstrap_servers: str | None = None,
                 asset_router_url: str | None = None,
                 test_mode: bool = False,
                 start_mapping_watcher: bool = True) -> None:

        """
        Initializes the Asset, its local state cache, and the communication infrastructure.

        Besides initializing the local asset cache, this constructor optionally starts a
        background thread that monitors changes to the UNS mapping and automatically
        recreates the NATS subscription whenever the mapped Asset UUID changes.

        Args:
            uns_id (str): UNS identifier of the asset.
            ksqlClient (KSQLDBClient): Client for interacting with ksqlDB.
            bootstrap_servers (str | None): Kafka bootstrap server address.
            asset_router_url (str | None): Asset Router URL from the OpenFactory Fan-Out-Layer.
            test_mode (bool): If True, disables live Kafka/ksql interaction (useful for unit tests).
            start_mapping_watcher (bool): If ``True`` (default), starts the background thread that monitors changes in the UNS-to-Asset mapping.

        Raises:
            OFAException: If ``bootstrap_servers`` is not provided and the
                ``KAFKA_BROKER`` environment variable is not set.
            OFAException: If ``asset_router_url`` is not provided and the
                ``ASSET_ROUTER_URL`` environment variable is not set.

        Note:
          - If ``bootstrap_servers`` is not explicitly provided, the constructor will attempt to read it from the ``KAFKA_BROKER`` environment variable.
          - If ``asset_router_url`` is not explicitly provided, the constructor will attempt to read it from the ``ASSET_ROUTER_URL`` environment variable.
          - When used in an :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` deployed on the OpenFactory cluster, the environment variables ``KAFKA_BROKER`` and ``ASSET_ROUTER_URL`` will be set.

        .. tip::
           The environment variables ``KSQLDB_URL``, ``KAFKA_BROKER`` and ``ASSET_ROUTER_URL`` will be set when deployed on the OpenFactory Cluster.
        """
        object.__setattr__(self, 'ASSET_ID', uns_id)
        super().__init__(ksqlClient=ksqlClient,
                         bootstrap_servers=bootstrap_servers,
                         asset_router_url=asset_router_url,
                         test_mode=test_mode)

        self._subscribed_asset_uuid: str | None = None

        if not test_mode:

            # UUID currently subscribed by the NATS subscriber
            self._subscribed_asset_uuid = self.asset_uuid

            # Create stop event
            self._stop_mapping_watcher = threading.Event()

            self._mapping_watcher: threading.Thread | None = None

            if start_mapping_watcher:
                self._mapping_watcher = threading.Thread(
                    target=self._watch_asset_mapping,
                    daemon=True,
                )
                self._mapping_watcher.start()

    @property
    def asset_uuid(self) -> str:
        """
        Returns the asset UUID based on runtime state.

        Returns:
            str: The asset's UUID.
        """
        query = f"SELECT asset_uuid FROM asset_to_uns_map WHERE {self.KSQL_ASSET_ID}='{self.ASSET_ID}';"
        result = self.ksql.query(query)
        if not result:
            raise OFAException(f"No Asset UUID mapping found for UNS asset '{self.ASSET_ID}'.")
        return result[0]['ASSET_UUID']

    def _get_reference_list(self, direction: str, as_assets: bool = False) -> list[str | AssetUNS]:
        """
        Retrieves a list of asset references (UUIDs or AssetUNS objects) in the given direction.

        Args:
            direction (str): Either 'above' or 'below', indicating reference direction.
            as_assets (bool): If True, returns AssetUNS instances instead of UUID strings.

        Returns:
            List: List of asset UUIDs or AssetUNS objects.
        """
        key = f"{self.ASSET_ID}|references_{direction}"
        query = f"SELECT VALUE FROM {self.KSQL_ASSET_TABLE} WHERE key='{key}';"
        results = self.ksql.query(query)

        if not results or not results[0].get('VALUE', '').strip():
            return []

        uns_ids = [uns_id.strip() for uns_id in results[0]['VALUE'].split(",")]
        if as_assets:
            return [AssetUNS(uns_id, ksqlClient=self.ksql) for uns_id in uns_ids]
        return uns_ids

    def _check_asset_mapping(self):
        """
        Checks whether the Asset UUID currently associated with this UNS identifier has changed.

        If the mapping changed, the existing NATS subscription is recreated
        so the asset continues receiving updates for the newly mapped Asset.
        """
        current_uuid = self.asset_uuid

        if current_uuid != self._subscribed_asset_uuid:
            self._restart_nats_subscription()
            self._subscribed_asset_uuid = current_uuid

    def _watch_asset_mapping(self):
        """
        Background worker executed by a dedicated thread.

        Periodically checks whether the UNS identifier resolves to a different
        Asset UUID and updates the NATS subscription if required.
        """
        while not self._stop_mapping_watcher.wait(self._MAPPING_WATCH_INTERVAL):
            self._check_asset_mapping()

    def close(self):
        """
        Stops the background mapping watcher and then delegates resource cleanup
        to :meth:`BaseAsset.close`.

        This ensures the monitoring thread exits before the NATS subscriber and
        other shared resources are released.
        """
        self._stop_mapping_watcher.set()

        if self._mapping_watcher is not None and self._mapping_watcher.is_alive():
            self._mapping_watcher.join(timeout=5)

        super().close()
