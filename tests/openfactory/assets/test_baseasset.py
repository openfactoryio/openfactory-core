import json
import threading
from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from operator import gt
from openfactory.exceptions import OFAException
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.assets.asset_base import BaseAsset, KafkaAssetConsumer


class ValidAsset(BaseAsset):
    """ A valid subclass of BaseAsset """
    KSQL_ASSET_TABLE = "assets"
    KSQL_ASSET_ID = "asset_uuid"
    ASSET_CONSUMER_CLASS = KafkaAssetConsumer

    def __init__(self, asset_id, ksqlClient, bootstrap_servers='MockedBroker', asset_router_url='Mocked_Asset_URL'):
        object.__setattr__(self, 'ASSET_ID', asset_id)
        super().__init__(ksqlClient, bootstrap_servers, asset_router_url)

    @property
    def asset_uuid(self):
        return self.ASSET_ID


@patch("openfactory.assets.asset_base.AssetProducer")
class TestBaseAsset(TestCase):
    """
    Test class BaseAsset
    """

    def setUp(self):
        self.ksql_mock = Mock(spec=KSQLDBClient)

        # Reset singleton before each test
        BaseAsset._shared_producer = None

        # Freeze datetime for deterministic AssetAttribute.timestamp
        self.fixed_ts = datetime(2023, 1, 1, 12, 0, 0)
        datetime_patcher = patch("openfactory.assets.utils.time_methods.datetime")
        self.mock_datetime = datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # Make datetime.now() return fixed timestamp
        self.mock_datetime.now.return_value = self.fixed_ts
        # Allow datetime(...) constructor to still work
        self.mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

        # Patch NATSSubsriber for all tests
        nats_patcher = patch("openfactory.assets.asset_base.NATSSubscriber")
        self.MockNATSSubscriber = nats_patcher.start()
        self.addCleanup(nats_patcher.stop)

        url_patcher = patch(
            "openfactory.assets.asset_base.get_nats_cluster_url",
            return_value="nats://mocked_cluster"
        )
        self.mock_get_nats_cluster_url = url_patcher.start()
        self.addCleanup(url_patcher.stop)

    def test_valid_subclass(self, MockAssetProducer):
        """ Test valid subclass """
        self.ksql_mock.query.return_value = []
        asset = ValidAsset('some_id', self.ksql_mock)
        self.assertEqual(asset.ksql, self.ksql_mock)
        self.assertEqual(asset.bootstrap_servers, 'MockedBroker')

        # Confirm mock constructor was called
        MockAssetProducer.assert_called_once_with(
            ksqlClient=self.ksql_mock, bootstrap_servers='MockedBroker'
        )
        # Confirm the asset is using the mock instance
        self.assertEqual(asset.producer, MockAssetProducer.return_value)

    def test_asset_router_url_explicit(self, MockAssetProducer):
        """ Test explicite definition of asset_router_url """
        self.ksql_mock.query.return_value = []
        asset = ValidAsset(
            "some_id",
            self.ksql_mock,
            asset_router_url="http://explicit-router"
        )

        self.assertEqual(asset.asset_router_url, "http://explicit-router")

    @patch.dict("os.environ", {"ASSET_ROUTER_URL": "http://env-router"})
    def test_asset_router_url_from_env(self, MockAssetProducer):
        """ Test environment variable fallback for asset_router_url """
        self.ksql_mock.query.return_value = []
        asset = ValidAsset("some_id", self.ksql_mock, asset_router_url=None)

        self.assertEqual(asset.asset_router_url, "http://env-router")

    @patch.dict("os.environ", {}, clear=True)
    def test_asset_router_url_missing_raises(self, MockAssetProducer):
        """ Test missing asset_router_url and missing ASSET_ROUTER_URL env var raises """
        with self.assertRaises(OFAException) as ctx:
            ValidAsset("some_id", self.ksql_mock, asset_router_url=None)

        self.assertIn("ASSET_ROUTER_URL", str(ctx.exception))

    def test_bootstrap_servers_explicit(self, MockAssetProducer):
        """ Test explicite definition of bootstrap_servers """
        self.ksql_mock.query.return_value = []
        asset = ValidAsset(
            "some_id",
            self.ksql_mock,
            bootstrap_servers="mocked_kafka_broker"
        )

        self.assertEqual(asset.bootstrap_servers, "mocked_kafka_broker")

    @patch.dict("os.environ", {"KAFKA_BROKER": "mocked-broker"})
    def test_bootstrap_servers_from_env(self, MockAssetProducer):
        """ Test environment variable fallback for bootstrap_servers """
        self.ksql_mock.query.return_value = []
        asset = ValidAsset("some_id", self.ksql_mock, bootstrap_servers=None)

        self.assertEqual(asset.bootstrap_servers, "mocked-broker")

    @patch.dict("os.environ", {}, clear=True)
    def test_bootstrap_servers_missing_raises(self, MockAssetProducer):
        """ Test missing bootstrap_servers and missing KAFKA_BROKER env var raises """
        with self.assertRaises(OFAException) as ctx:
            ValidAsset("some_id", self.ksql_mock, bootstrap_servers=None)

        self.assertIn("KAFKA_BROKER", str(ctx.exception))

    def test_missing_ksql_asset_table(self, MockAssetProducer):
        """ Test missing KSQL_ASSET_TABLE raise error """
        class MissingTable(ValidAsset):
            KSQL_ASSET_TABLE = None

        with self.assertRaises(ValueError):
            MissingTable('some_id', self.ksql_mock)

    def test_missing_ksql_asset_id(self, MockAssetProducer):
        """ Test missing KSQL_ASSET_ID raise error """
        class MissingKSQL_AssetID(ValidAsset):
            KSQL_ASSET_ID = None

        with self.assertRaises(ValueError):
            MissingKSQL_AssetID('some_id', self.ksql_mock)

    def test_missing_asset_id(self, MockAssetProducer):
        """ Test missing ASSET_ID raise error """
        class MissingAssetID(BaseAsset):
            KSQL_ASSET_TABLE = "assets"
            KSQL_ASSET_ID = "asset_uuid"
            ASSET_CONSUMER_CLASS = KafkaAssetConsumer

            def __init__(self, asset_id, ksqlClient, bootstrap_servers='MockedBroker'):
                super().__init__(ksqlClient, bootstrap_servers)

            @property
            def asset_uuid(self):
                return self.ASSET_ID

        with self.assertRaises(ValueError):
            MissingAssetID('some_id', self.ksql_mock)

    def test_missing_asset_consumer_class(self, MockAssetProducer):
        """ Test missing ASSET_CONSUMER_CLASS raise error """
        class MissingConsumerClass(ValidAsset):
            ASSET_CONSUMER_CLASS = None

        with self.assertRaises(ValueError):
            MissingConsumerClass('some_id', self.ksql_mock)

    def test_invalid_consumer_class(self, MockAssetProducer):
        """ Test invalid ASSET_CONSUMER_CLASS raise error """
        class InvalidConsumer(ValidAsset):
            ASSET_CONSUMER_CLASS = str

        with self.assertRaises(TypeError):
            InvalidConsumer('some_id', self.ksql_mock)

    def test_resync_state_refreshes_attributes_and_methods(self, MockAssetProducer):
        """ Test resync_state() reloads attributes and methods from ksqlDB. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset._fetch_attributes = MagicMock()
        asset._fetch_methods = MagicMock()

        asset.resync_state()

        asset._fetch_attributes.assert_called_once_with()
        asset._fetch_methods.assert_called_once_with()

    def test_close_stops_subscriber(self, MockAssetProducer):
        """ Test close() stops the asset's NATS subscriber. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset._subscriber = MagicMock()
        asset.loop_thread.stop = MagicMock()

        asset.close()

        asset._subscriber.stop.assert_called_once()

    @patch("asyncio.all_tasks")
    @patch("asyncio.gather")
    def test_close_cancels_pending_tasks(self, mock_gather, mock_all_tasks, MockAssetProducer):
        """ Test close cancles pending tasks """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        # Mock one pending task
        mock_task = MagicMock()
        mock_all_tasks.return_value = [mock_task]

        asset.loop_thread.stop = MagicMock()

        asset.close()

        mock_task.cancel.assert_called_once()
        mock_gather.assert_called_once()

    def test_close_stops_loop_thread(self, MockAssetProducer):
        """ Test close stops loop_thread """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset.loop_thread.stop = MagicMock()

        asset.close()

        asset.loop_thread.stop.assert_called_once()

    def test_parse_method_value_none(self, MockAssetProducer):
        """ Test _parse_method_value returns None unchanged. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        self.assertIsNone(asset._parse_method_value(None))

    def test_parse_method_value_dict(self, MockAssetProducer):
        """ Test _parse_method_value returns dictionaries unchanged. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        payload = {
            "description": "GenerateCode",
            "arguments": [],
        }

        self.assertIs(asset._parse_method_value(payload), payload)

    def test_parse_method_value_json(self, MockAssetProducer):
        """ Test _parse_method_value parses valid JSON strings. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        payload = {
            "description": "GenerateCode",
            "arguments": [],
        }

        self.assertEqual(asset._parse_method_value(json.dumps(payload)), payload)

    def test_parse_method_value_invalid_json(self, MockAssetProducer):
        """ Test _parse_method_value returns invalid JSON strings unchanged. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        self.assertEqual(asset._parse_method_value("not json"), "not json")

    def test_parse_method_value_other_type(self, MockAssetProducer):
        """ Test _parse_method_value returns unsupported types unchanged. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        self.assertEqual(asset._parse_method_value(42), 42)

    def test_on_message_updates_attribute_cache(self, MockAssetProducer):
        """ Test _on_message updates the cached AssetAttribute. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset._on_message(
            f"{asset.asset_uuid.upper()}.temperature",
            {
                "TYPE": "Samples",
                "VALUE": 42,
                "TAG": "Temperature",
                "attributes": {"timestamp": "MockedTimeStamp"},
            },
        )

        self.assertEqual(asset.temperature.value, 42)
        self.assertEqual(asset.temperature.tag, "Temperature")
        self.assertEqual(asset.temperature.type, "Samples")

    def test_on_message_updates_method_definition(self, MockAssetProducer):
        """ Test _on_message updates cached methods. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        payload = {
            "description": "Generate code",
            "arguments": []
        }

        asset._on_message(
            f"{asset.asset_uuid.upper()}.GenerateCode",
            {
                "TYPE": "Method",
                "VALUE": json.dumps(payload),
            },
        )

        self.assertEqual(asset.methods()["GenerateCode"], payload)

    def test_on_message_updates_method_definition_from_string(self, MockAssetProducer):
        """ Test _on_message keeps invalid JSON method definitions as strings. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset._on_message(
            f"{asset.asset_uuid.upper()}.GenerateCode",
            {
                "TYPE": "Method",
                "VALUE": "not json",
            },
        )

        self.assertEqual(asset.methods()["GenerateCode"], "not json")

    def test_on_message_updates_method_definition_from_dict(self, MockAssetProducer):
        """ Test _on_message accepts already parsed method definitions. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        payload = {
            "description": "GenerateCode",
            "arguments": []
        }

        asset._on_message(
            f"{asset.asset_uuid.upper()}.GenerateCode",
            {
                "TYPE": "Method",
                "VALUE": payload,
            },
        )

        self.assertIs(asset.methods()["GenerateCode"], payload)

    def test_on_message_replaces_existing_method_definition(self, MockAssetProducer):
        """ Test _on_message replaces an existing cached method definition. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset.ofa_methods["GenerateCode"] = {
            "description": "Old description",
            "arguments": ["old"],
        }

        payload = {
            "description": "New description",
            "arguments": [],
        }

        asset._on_message(
            f"{asset.asset_uuid.upper()}.GenerateCode",
            {
                "TYPE": "Method",
                "VALUE": json.dumps(payload),
            },
        )

        self.assertEqual(asset.methods()["GenerateCode"], payload)

    def test_on_message_ignores_unknown_message_type(self, MockAssetProducer):
        """ Test _on_message ignores unsupported message types. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.ofa_attributes["temperature"] = attr

        asset.ofa_methods["GenerateCode"] = {
            "description": "GenerateCode",
            "arguments": [],
        }

        asset._on_message(
            f"{asset.asset_uuid.upper()}.temperature",
            {
                "TYPE": "SomethingUnexpected",
                "VALUE": "new value",
                "TAG": "Temperature",
                "attributes": {"timestamp": "MockedTimeStamp"},
            },
        )

        self.assertIs(asset.ofa_attributes["temperature"], attr)

        self.assertEqual(
            asset.ofa_methods["GenerateCode"],
            {
                "description": "GenerateCode",
                "arguments": [],
            },
        )

    def test_fetch_attributes_converts_samples_to_float(self, MockAssetProducer):
        """ Test _fetch_attributes converts sample values to float. """

        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return [{
                    "ID": "temperature",
                    "VALUE": "42.5",
                    "TYPE": "Samples",
                    "TAG": "Temperature",
                    "TIMESTAMP": "MockedTimeStamp",
                }]

            if "TYPE='Method'" in query:
                return []

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)

        self.assertEqual(asset.temperature.value, 42.5)
        self.assertIsInstance(asset.temperature.value, float)

    def test_fetch_methods_parses_json(self, MockAssetProducer):
        """ Test _fetch_methods parses JSON method definitions. """

        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return [{
                    "ID": "GenerateCode",
                    "VALUE": json.dumps({
                        "description": "GenerateCode",
                        "arguments": []
                    }),
                    "TYPE": "Method",
                }]

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)

        self.assertEqual(
            asset.methods()["GenerateCode"],
            {
                "description": "GenerateCode",
                "arguments": []
            }
        )

    def test_fetch_methods_accepts_dict(self, MockAssetProducer):
        """ Test _fetch_methods accepts already parsed method definitions. """

        payload = {
            "description": "GenerateCode",
            "arguments": []
        }

        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return [{
                    "ID": "GenerateCode",
                    "VALUE": payload,
                    "TYPE": "Method",
                }]

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)

        self.assertIs(asset.methods()["GenerateCode"], payload)

    def test_fetch_methods_accepts_none(self, MockAssetProducer):
        """ Test _fetch_methods stores None method definitions. """

        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return [{
                    "ID": "GenerateCode",
                    "VALUE": None,
                    "TYPE": "Method",
                }]

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)

        self.assertIsNone(asset.methods()["GenerateCode"])

    def test_type_returns_unavailable_when_empty(self, MockAssetProducer):
        """ Test if asset.type returns 'UNAVAILABLE' when the ksql query yields no results """

        # Simulate an empty result from ksqlDB
        self.ksql_mock.query.return_value = []

        asset = ValidAsset('some_id', self.ksql_mock)

        # Expect 'UNAVAILABLE' when no data is returned
        self.assertEqual(asset.type, 'UNAVAILABLE')

        # Check if the correct query was executed
        expected_query = "SELECT TYPE FROM assets_type WHERE ASSET_UUID='some_id';"
        self.ksql_mock.query.assert_any_call(expected_query)

    def test_type_returns_value_when_present(self, MockAssetProducer):
        """ Test if asset.type returns the correct value when the ksql query returns data """

        # Simulate a valid result from ksqlDB with type 'Condition'
        ksql_mock = Mock(spec=KSQLDBClient)

        def query_side_effect(query):
            if query.startswith("SELECT TYPE FROM assets_type"):
                return [{"TYPE": "Condition"}]
            return []

        ksql_mock.query.side_effect = query_side_effect

        asset = ValidAsset('some_id', ksql_mock)

        # Expect the actual type returned from the query
        self.assertEqual(asset.type, 'Condition')

        # Check if the correct query was executed
        expected_query = "SELECT TYPE FROM assets_type WHERE ASSET_UUID='some_id';"
        ksql_mock.query.assert_any_call(expected_query)

    def test_attributes_success(self, MockAssetProducer):
        """ Test attributes() returns correct attribute IDs """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": 101,
                "VALUE": 1,
                "TYPE": "Samples",
                "TAG": "Temperature",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": 102,
                "VALUE": 2,
                "TYPE": "Events",
                "TAG": "Execution",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": 103,
                "VALUE": 3,
                "TYPE": "Condition",
                "TAG": "Fault",
                "TIMESTAMP": "MockedTimeStamp",
            },
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attributes = asset.attributes()

        self.assertEqual(attributes, [101, 102, 103])  # Expected list of IDs

    def test_attributes_empty(self, MockAssetProducer):
        """ Test attributes() returns an empty list when no attributes exist """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = []

        asset = ValidAsset("uuid-456", ksqlClient=ksqlMock)
        attributes = asset.attributes()

        self.assertEqual(attributes, [])

    def test_get_attributes_by_type(self, MockAssetProducer):
        """ Test _get_attributes_by_type() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id1",
                "VALUE": "val1",
                "TYPE": "Samples",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": "id2",
                "VALUE": "val2",
                "TYPE": "Events",
                "TAG": "Execution",
                "TIMESTAMP": "MockedTimeStamp",
            },
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        samples = asset._get_attributes_by_type('Samples')

        self.assertEqual(samples, [{'ID': 'id1', 'VALUE': 'val1', 'TAG': 'MockedTag'}])

    def test_samples(self, MockAssetProducer):
        """ Test samples() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id1",
                "VALUE": "val1",
                "TYPE": "Samples",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": "event1",
                "VALUE": "ACTIVE",
                "TYPE": "Events",
                "TAG": "Execution",
                "TIMESTAMP": "MockedTimeStamp",
            },
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        samples = asset.samples()

        self.assertEqual(samples, [{'ID': 'id1', 'VALUE': 'val1', 'TAG': 'MockedTag'}])

    def test_events(self, MockAssetProducer):
        """ Test events() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id1",
                "VALUE": "val1",
                "TYPE": "Samples",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": "id2",
                "VALUE": "val2",
                "TYPE": "Events",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag",
                "TIMESTAMP": "MockedTimeStamp",
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        events = asset.events()

        self.assertEqual(events, [{'ID': 'id2', 'VALUE': 'val2', 'TAG': 'MockedTag'}])

    def test_conditions(self, MockAssetProducer):
        """ Test conditions() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id3",
                "VALUE": "val3",
                "TYPE": "Condition",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}Fault",
                "TIMESTAMP": "MockedTimeStamp",
            },
            {
                "ID": "id4",
                "VALUE": "val4",
                "TYPE": "Samples",
                "TAG": "Temperature",
                "TIMESTAMP": "MockedTimeStamp",
            },
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        conditions = asset.conditions()

        expected_conditions = [{
            "ID": "id3",
            "VALUE": "val3",
            "TAG": "Fault"  # The namespace is removed
        }]
        self.assertEqual(conditions, expected_conditions)

    def test_methods(self, MockAssetProducer):
        """ Test methods() """
        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []
            elif "TYPE='Method'" in query:
                return [{"ID": "id4", "VALUE": "val4"}]
            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        methods = asset.methods()

        self.assertEqual(methods, {'id4': 'val4'})

    def test_method_builds_correct_envelope(self, MockAssetProducer):
        """ Test that method() builds and sends a valid CommandEnvelope """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        # Spy on send_asset_attribute
        asset.producer.send_asset_attribute = MagicMock()

        correlation_id = asset.method(
                    method="start",
                    sender_uuid="SENDER-1",
                    args=[("param1", "value1"), ("param2", "value2")]
                )

        asset.producer.send_asset_attribute.assert_called_once()

        # Get arguments of send_asset_attribute
        asset_uuid, asset_attribute = asset.producer.send_asset_attribute.call_args.args
        cmd_headder = json.loads(asset_attribute.value)["header"]
        cmd_args = json.loads(asset_attribute.value)["arguments"]

        # Ensure send_asset_attribute was called correctly
        self.assertEqual(asset_uuid, "uuid-123")
        self.assertEqual(asset_attribute.id, "start_CMD")
        self.assertEqual(cmd_headder["correlation_id"], correlation_id)
        self.assertEqual(cmd_headder["sender_uuid"], "SENDER-1")
        self.assertEqual(cmd_args, {"param1": "value1", "param2": "value2"})

    def test_setattr_non_asset_attribute(self, MockAssetProducer):
        """ Test setting a non-asset attribute (not in attributes list) """
        # Mock asset with a single 'temperature' attribute
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.attributes = MagicMock(return_value=["temperature"])

        asset.new_attr = "something"
        self.assertEqual(asset.new_attr, "something")

    def test_setattr_method_raises_exception(self, MockAssetProducer):
        """ Test assigning to a method raises OFAException. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset.ofa_methods["GenerateCode"] = {
            "description": "GenerateCode",
            "arguments": []
        }

        with self.assertRaises(OFAException):
            asset.GenerateCode = 42

    def test_setattr_raises_exception_on_invalid_asset_attribute(self, MockAssetProducer):
        """ Test setting an AssetAttribute on undefined asset attribute raises exception """
        # Mock asset with a single 'temperature' attribute
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.attributes = MagicMock(return_value=["temperature"])

        with self.assertRaises(OFAException):
            asset.invalid_attr = AssetAttribute(
                id='mocked_id',
                value=100,
                type='Samples',
                tag='SomeTag')

    def test_setattr_valid_asset_attribute_with_asset_attribute(self, MockAssetProducer):
        """ Test setting a defined asset attribute with AssetAttribute instance """
        mock_producer = MockAssetProducer.return_value
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.ofa_attributes["temperature"] = AssetAttribute(
            id="temperature",
            value=None,
            tag="Temperature",
            type="Samples",
        )

        attr = AssetAttribute(id='temperature', value=25, tag="Temperature", type="Samples")
        asset.temperature = attr

        mock_producer.send_asset_attribute.assert_called_once_with("uuid-123", attr)
        self.assertIs(asset.ofa_attributes["temperature"], attr)

    def test_setattr_with_wrong_asset_attribute_id(self, MockAssetProducer):
        """ Test setting a defined asset attribute with AssetAttribute instance having wrong id raises exception """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.attributes = MagicMock(return_value=["temperature"])

        attr = AssetAttribute(id='mocked_id', value=25, tag="Temperature", type="Samples")

        with self.assertRaises(OFAException):
            asset.temperature = attr

    def test_getattr_returns_unavailable_when_no_result(self, MockAssetProducer):
        """ Test __getattr__ returns an UNAVAILABLE AssetAttribute when query yields no results """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = []  # Simulate no data

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attribute = asset.some_missing_attribute

        expected = AssetAttribute(
            id="some_missing_attribute",
            value="UNAVAILABLE",
            type="UNAVAILABLE",
            tag="UNAVAILABLE",
            timestamp="UNAVAILABLE"
        )
        self.assertEqual(attribute, expected)

    def test_setattr_valid_asset_attribute_with_raw_value(self, MockAssetProducer):
        """ Test setting a defined asset attribute with a raw value (not an AssetAttribute) """
        mock_producer = MockAssetProducer.return_value
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")

        # Simulate current attribute
        current_attr = AssetAttribute(id="temperature", value=10, tag="Temperature", type="Samples")
        asset.ofa_attributes["temperature"] = current_attr

        asset.temperature = 30

        mock_producer.send_asset_attribute.assert_called_once()
        expected = AssetAttribute(id="temperature", value=30, tag="Temperature", type="Samples")
        mock_producer.send_asset_attribute.assert_called_once_with("uuid-123", expected)

        updated = asset.ofa_attributes["temperature"]
        self.assertEqual(updated.id, "temperature")
        self.assertEqual(updated.value, 30)
        self.assertEqual(updated.tag, current_attr.tag)
        self.assertEqual(updated.type, current_attr.type)

    def test_getattr_samples(self, MockAssetProducer):
        """ Test __getattr__ returns float for 'Samples' type """
        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return [
                    {
                        "ID": "id1",
                        "VALUE": "42.5",
                        "TYPE": "Samples",
                        "TAG": "MockedTag",
                        "TIMESTAMP": "MockedTimeStamp",
                    }
                ]

            if "TYPE='Method'" in query:
                return []

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attribute = asset.id1

        expected = AssetAttribute(id='id1', value=42.5, type='Samples', tag='MockedTag', timestamp='MockedTimeStamp')
        self.assertEqual(attribute, expected)

    def test_getattr_string_value(self, MockAssetProducer):
        """ Test __getattr__ returns raw VALUE for non-'Samples' and non-'Method' types """
        ksqlMock = MagicMock()

        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return [
                    {
                        "ID": "id2",
                        "VALUE": "val2",
                        "TYPE": "Events",
                        "TAG": "MockedTag",
                        "TIMESTAMP": "MockedTimeStamp"
                    }
                ]

            if "TYPE='Method'" in query:
                return []

            return []

        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attribute = asset.id2

        expected = AssetAttribute(id='id2', value='val2', type='Events', tag='MockedTag', timestamp='MockedTimeStamp')
        self.assertEqual(attribute, expected)

    @patch("openfactory.assets.asset_base.BaseAsset.method")
    def test_getattr_method(self, mock_method, MockAssetProducer):
        """ Test __getattr__ returns a callable for 'Method' type attributes """
        # Arrange: mock method returns fixed correlation_id
        mock_method.return_value = "mocked-correlation-id"

        # Mock KSQL query to return a Method type row
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "a_method",
                "VALUE": "val4",
                "TYPE": "Method",
                "TAG": "MockedTag",
                "TIMESTAMP": "MockedTimeStamp"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)

        # Act: call the dynamically returned method with kwargs
        correlation_id = asset.a_method(sender_uuid="TEST-ASSET", arg1="value1", arg2="value2")

        # Assert: the return value comes from mock_method
        self.assertEqual(correlation_id, "mocked-correlation-id")

        # Ensure BaseAsset.method() was called with correct arguments
        expected_args_list = [("arg1", "value1"), ("arg2", "value2")]
        mock_method.assert_called_once_with("a_method", "TEST-ASSET", expected_args_list)

    def test_add_attribute_sends_asset_attribute(self, MockAssetProducer):
        """ Test add_attribute sends the new AssetAttribute to the producer """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        asset.wait_until = MagicMock(return_value=True)
        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.add_attribute(attr)

        asset.producer.send_asset_attribute.assert_called_once_with("uuid-123", attr)

    def test_add_attribute_updates_cache_immediately(self, MockAssetProducer):
        """ Test add_attribute immediately updates the local cache. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        asset.wait_until = MagicMock(return_value=True)

        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.add_attribute(attr)

        self.assertIs(asset.temperature, attr)

    def test_add_attribute_in_test_mode_adds_attribute(self, MockAssetProducer):
        """ Test add_attribute stores the attribute locally in test mode. """
        asset = ValidAsset.__new__(ValidAsset)
        BaseAsset.__init__(asset, ksqlClient=MagicMock(), test_mode=True)

        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.add_attribute(attr)

        self.assertEqual(asset.temperature.value, 42)
        self.assertEqual(asset.temperature.type, "Samples")
        self.assertEqual(asset.temperature.tag, "Temperature")

    def test_add_attribute_waits_until_available_by_default(self, MockAssetProducer):
        """ Test add_attribute waits until the new attribute is available by default """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        asset.wait_until = MagicMock(return_value=True)
        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.add_attribute(attr)

        asset.producer.send_asset_attribute.assert_called_once_with("uuid-123", attr)
        asset.wait_until.assert_called_once_with(
            attribute_id="temperature",
            value=42,
            use_ksqlDB=True,
        )

    def test_add_attribute_does_not_wait_when_disabled(self, MockAssetProducer):
        """ Test add_attribute does not wait when wait_to_become_available=False """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        asset.wait_until = MagicMock()
        attr = AssetAttribute(
            id="temperature",
            value=42,
            type="Samples",
            tag="Temperature",
        )

        asset.add_attribute(attr, wait_to_become_available=False)

        asset.producer.send_asset_attribute.assert_called_once_with("uuid-123", attr)
        asset.wait_until.assert_not_called()

    def test_get_reference_list_not_implemented(self, MockAssetProducer):
        """ Test if _get_reference_list raises NotImplementedError when not implemented in subclass """
        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return []

            if "SELECT VALUE FROM assets WHERE key=" in query:
                return [{"VALUE": "existing_ref"}]

            return []

        ksqlMock = MagicMock()
        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset('some_id', ksqlClient=ksqlMock)

        with self.assertRaises(NotImplementedError):
            asset._get_reference_list('above')

    def test_get_references_above_uuid_calls_get_reference_list(self, MockAssetProducer):
        """ Test get_references_above_uuid() calls _get_reference_list with direction='above' and as_assets=False """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.get_references_above_uuid()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="above", as_assets=False)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_get_references_above_calls_get_reference_list(self, MockAssetProducer):
        """ Test get_references_above() calls _get_reference_list with direction='above' and as_assets=True """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.get_references_above()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="above", as_assets=True)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_get_references_below_uuid_calls_get_reference_list(self, MockAssetProducer):
        """ Testget_ references_below_uuid() calls _get_reference_list with direction='below' and as_assets=False """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.get_references_below_uuid()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="below", as_assets=False)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_get_references_below_calls_get_reference_list(self, MockAssetProducer):
        """ Test get_references_below() calls _get_reference_list with direction='below' and as_assets=True """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.get_references_below()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="below", as_assets=True)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_add_reference_above_no_existing_reference(self, MockAssetProducer):
        """ Test add_reference_above when no existing references are present """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = []
        asset = ValidAsset("asset-001", ksqlClient=ksqlMock)
        asset.producer = MagicMock()

        # Call the method
        asset.add_reference_above("new-ref")

        # Ensure the correct query was executed
        expected_query = "SELECT VALUE FROM assets WHERE key='asset-001|references_above';"
        ksqlMock.query.assert_any_call(expected_query)

        # Assert producer called with the expected AssetAttribute
        expected_attr = AssetAttribute(
            id="references_above",
            value="new-ref",
            type="OpenFactory",
            tag="AssetsReferences"
        )
        asset.producer.send_asset_attribute.assert_called_once_with("asset-001", expected_attr)

    def test_add_reference_above_with_existing_reference(self, MockAssetProducer):
        """ Test add_reference_above when existing references are present """
        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return []

            if "SELECT VALUE FROM assets WHERE key='asset-001|references_above';" in query:
                return [{"VALUE": "existing-ref1, existing-ref2"}]

            return []

        ksqlMock = MagicMock()
        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("asset-001", ksqlClient=ksqlMock)
        asset.producer = MagicMock()

        # Call the method
        asset.add_reference_above("new-ref")

        # Ensure the correct query was executed
        expected_query = "SELECT VALUE FROM assets WHERE key='asset-001|references_above';"
        ksqlMock.query.assert_any_call(expected_query)

        # Assert producer called with the expected AssetAttribute
        expected_attr = AssetAttribute(
            id="references_above",
            value="new-ref, existing-ref1, existing-ref2",
            type="OpenFactory",
            tag="AssetsReferences"
        )
        asset.producer.send_asset_attribute.assert_called_once_with("asset-001", expected_attr)

    def test_add_reference_below_no_existing_reference(self, MockAssetProducer):
        """ Test add_reference_below when no existing references are present """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = []
        asset = ValidAsset("asset-001", ksqlClient=ksqlMock)
        asset.producer = MagicMock()

        # Call the method
        asset.add_reference_below("new-ref")

        # Ensure the correct query was executed
        expected_query = "SELECT VALUE FROM assets WHERE key='asset-001|references_below';"
        ksqlMock.query.assert_any_call(expected_query)

        # Assert producer called with the expected AssetAttribute
        expected_attr = AssetAttribute(
            id="references_below",
            value="new-ref",
            type="OpenFactory",
            tag="AssetsReferences"
        )
        asset.producer.send_asset_attribute.assert_called_once_with("asset-001", expected_attr)

    def test_add_reference_below_with_existing_reference(self, MockAssetProducer):
        """ Test add_reference_below when existing references are present """
        def query_side_effect(query):
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return []

            if "references_above" in query:
                return [{"VALUE": "existing-ref1, existing-ref2"}]

            if "references_below" in query:
                return [{"VALUE": "existing-ref1, existing-ref2"}]

            return []

        ksqlMock = MagicMock()
        ksqlMock.query.side_effect = query_side_effect

        asset = ValidAsset("asset-001", ksqlClient=ksqlMock)
        asset.producer = MagicMock()

        # Call the method
        asset.add_reference_below("new-ref")

        # Ensure the correct query was executed
        expected_query = "SELECT VALUE FROM assets WHERE key='asset-001|references_below';"
        ksqlMock.query.assert_any_call(expected_query)

        # Assert producer called with the expected AssetAttribute
        expected_attr = AssetAttribute(
            id="references_below",
            value="new-ref, existing-ref1, existing-ref2",
            type="OpenFactory",
            tag="AssetsReferences"
        )
        asset.producer.send_asset_attribute.assert_called_once_with("asset-001", expected_attr)

    def test_wait_until_method_raises(self, MockAssetProducer):
        """ wait_until cannot be used on methods. """
        asset = ValidAsset("test_uuid", ksqlClient=MagicMock())

        asset.ofa_methods["test_method"] = {}

        with self.assertRaises(OFAException):
            asset.wait_until(attribute_id="test_method", value=None)

    def test_wait_until_attribute_matches_initially(self, MockAssetProducer):
        """ Test wait_until returns True when the attribute matches initially """
        asset = ValidAsset("test_uuid", ksqlClient=MagicMock())

        attribute = AssetAttribute(
            id="test_attribute",
            value="expected_value",
            type="Events",
            tag="TestAttribute",
            timestamp="MockedTimeStamp",
        )
        asset.ofa_attributes["test_attribute"] = attribute

        result = asset.wait_until(attribute_id="test_attribute", value="expected_value")

        self.assertTrue(result)

    def test_wait_until_initial_comparison(self, MockAssetProducer):
        """ Test wait_until uses the comparison function for the initial value. """

        asset = ValidAsset("test_uuid", ksqlClient=MagicMock())

        asset.ofa_attributes["temperature"] = AssetAttribute(
            id="temperature",
            value=50,
            type="Samples",
            tag="Temperature",
        )

        self.assertTrue(asset.wait_until(attribute_id="temperature", value=42, comparison=gt))

    def test_wait_until_matches_nats_message(self, MockAssetProducer):
        """ Test wait_until returns when a matching NATS message updates the cache. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        attribute = AssetAttribute(
            id="test_attribute",
            value="not_expected_value",
            type="Samples",
            tag="TestAttribute",
        )
        asset.ofa_attributes["test_attribute"] = attribute

        result = None

        def waiter():
            nonlocal result
            result = asset.wait_until(attribute_id="test_attribute", value=42, timeout=1)

        thread = threading.Thread(target=waiter)
        thread.start()

        # simulate arrival of a NATS message
        asset._on_message(
            f"{asset.asset_uuid.upper()}.test_attribute",
            {
                "TYPE": "Samples",
                "VALUE": 42,
                "TAG": "TestAttribute",
                "attributes": {
                    "timestamp": "MockedTimeStamp"
                }
            }
        )

        thread.join()

        self.assertTrue(result)
        self.assertEqual(asset.ofa_attributes["test_attribute"].value, 42)

    def test_wait_until_matches_comparison_after_nats_update(self, MockAssetProducer):
        """ Test wait_until wakes up when the comparison becomes true. """

        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        asset.ofa_attributes["temperature"] = AssetAttribute(
            id="temperature",
            value=20,
            type="Samples",
            tag="Temperature",
        )

        result = None

        def waiter():
            nonlocal result
            result = asset.wait_until(
                attribute_id="temperature",
                value=42,
                comparison=gt,
                timeout=1,
            )

        thread = threading.Thread(target=waiter)
        thread.start()

        asset._on_message(
            f"{asset.asset_uuid.upper()}.temperature",
            {
                "TYPE": "Samples",
                "VALUE": 50,
                "TAG": "Temperature",
                "attributes": {"timestamp": "MockedTimeStamp"},
            },
        )

        thread.join()

        self.assertTrue(result)

    def test_wait_until_times_out(self, MockAssetProducer):
        """ Test wait_until returns False when the attribute never reaches the expected value. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        attribute = AssetAttribute(
            id="temperature",
            value=10.0,
            type="Samples",
            tag="Temperature",
        )

        asset.ofa_attributes["temperature"] = attribute

        result = asset.wait_until(attribute_id="temperature", value=42.0, timeout=0.5)

        self.assertFalse(result)

        # Value should remain unchanged
        self.assertEqual(asset.ofa_attributes["temperature"].value, 10.0)

    def test_wait_until_ksqldb_matches(self, MockAssetProducer):
        """ Test wait_until with use_ksqlDB=True returns True when ksqlDB eventually matches. """

        ksql = MagicMock()

        def query_side_effect(query):
            # Constructor queries
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return []

            # wait_until polling
            query_side_effect.calls += 1

            if query_side_effect.calls == 1:
                return [{
                    "VALUE": "initial",
                    "TYPE": "Events",
                    "TIMESTAMP": "MockedTimeStamp",
                }]

            return [{
                "VALUE": "target",
                "TYPE": "Events",
                "TIMESTAMP": "MockedTimeStamp",
            }]

        query_side_effect.calls = 0
        ksql.query.side_effect = query_side_effect

        asset = ValidAsset("test_uuid", ksqlClient=ksql)

        asset.ofa_attributes["test_attribute"] = AssetAttribute(
            id="test_attribute",
            value="initial",
            type="Events",
            tag="TestAttribute",
            timestamp="MockedTimeStamp",
        )

        result = asset.wait_until(
            attribute_id="test_attribute",
            value="target",
            timeout=10,
            use_ksqlDB=True,
        )

        self.assertTrue(result)
        self.assertEqual(query_side_effect.calls, 2)

    def test_wait_until_ksqldb_timeout(self, MockAssetProducer):
        """ Test wait_until with use_ksqlDB=True returns False after timeout when no match is found. """

        ksql = MagicMock()

        def query_side_effect(query):
            # Constructor queries
            if "TYPE != 'Method'" in query:
                return []

            if "TYPE='Method'" in query:
                return []

            # wait_until polling always returns the same value
            query_side_effect.calls += 1
            return [{
                "VALUE": "initial",
                "TYPE": "Events",
                "TIMESTAMP": "MockedTimeStamp",
            }]

        query_side_effect.calls = 0
        ksql.query.side_effect = query_side_effect

        asset = ValidAsset("test_uuid", ksqlClient=ksql)

        asset.ofa_attributes["test_attribute"] = AssetAttribute(
            id="test_attribute",
            value="initial",
            type="Events",
            tag="TestAttribute",
            timestamp="MockedTimeStamp",
        )

        result = asset.wait_until(
            attribute_id="test_attribute",
            value="target",
            timeout=1,
            use_ksqlDB=True,
        )

        self.assertFalse(result)
        self.assertGreater(query_side_effect.calls, 1)

    def test___start_nats_consumer(self, MockAssetProducer):
        """ Test that __start_nats_consumer creates and starts the asset subscriber. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())

        mock_loop = MagicMock()
        object.__setattr__(asset, "loop_thread", mock_loop)

        with patch("openfactory.assets.asset_base.NATSSubscriber") as MockSubscriber, \
            patch("openfactory.assets.asset_base.get_nats_cluster_url",
                  return_value="nats://mocked_cluster"):

            mock_subscriber = MockSubscriber.return_value

            asset._BaseAsset__start_nats_consumer()

            MockSubscriber.assert_called_once_with(
                mock_loop,
                "nats://mocked_cluster",
                "UUID-123.*",
                asset._on_message,
            )

            mock_subscriber.start.assert_called_once()

            self.assertIs(asset._subscriber, mock_subscriber)

    def test_subscribe_to_attribute_registers_callback(self, MockAssetProducer):
        """ Test subscribe_to_attribute registers the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_attribute("mock_id", callback)

        self.assertIs(asset._attribute_callbacks["mock_id"], callback)

    def test_subscribe_to_attribute_callback_invoked(self, MockAssetProducer):
        """ Test registered callback is invoked when a matching message arrives. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_attribute("temperature", callback)

        asset._on_message(
            f"{asset.asset_uuid.upper()}.temperature",
            {
                "TYPE": "Samples",
                "VALUE": 42,
                "TAG": "Temperature",
                "attributes": {"timestamp": "MockedTimeStamp"},
            },
        )

        callback.assert_called_once()

    def test_stop_attribute_subscription_removes_callback(self, MockAssetProducer):
        """ Test stop_attribute_subscription unregisters the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_attribute("mock_id", callback)

        self.assertIn("mock_id", asset._attribute_callbacks)

        asset.stop_attribute_subscription("mock_id")

        self.assertNotIn("mock_id", asset._attribute_callbacks)

    def test_stop_attribute_subscription_unknown_id(self, MockAssetProducer):
        """ Test stopping an unknown attribute subscription does nothing. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        asset.stop_attribute_subscription("does_not_exist")
        self.assertEqual(asset._attribute_callbacks, {})

    def test_subscribe_to_messages_registers_callback(self, MockAssetProducer):
        """ Test subscribe_to_messages registers the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_messages(callback)

        self.assertIs(asset._messages_callback, callback)

    def test_subscribe_to_messages_callback_invoked(self, MockAssetProducer):
        """ Test registered callback is invoked when a matching message arrives. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_messages(callback)

        asset._on_message(
            f"{asset.asset_uuid.upper()}.temperature",
            {
                "TYPE": "Samples",
                "VALUE": 42,
                "TAG": "Temperature",
                "attributes": {"timestamp": "MockedTimeStamp"},
            },
        )

        callback.assert_called_once()

    def test_stop_messages_subscription_removes_callback(self, MockAssetProducer):
        """ Test stop_messages_subscription unregisters the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_messages(callback)
        self.assertIs(asset._messages_callback, callback)

        asset.stop_messages_subscription()

        self.assertIsNone(asset._messages_callback)

    def test_subscribe_to_samples_registers_callback(self, MockAssetProducer):
        """ Test subscribe_to_samples registers the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_samples(callback)

        self.assertIs(asset._samples_callback, callback)

    def test_subscribe_to_samples_callback_invoked(self, MockAssetProducer):
        """ Test registered callback is invoked only for sample messages. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_samples(callback)

        # Sample message -> callback should be invoked
        sample_msg = {
            "TYPE": "Samples",
            "VALUE": 42,
            "TAG": "Temperature",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.temperature", sample_msg)

        callback.assert_called_once_with(f"{asset.asset_uuid.upper()}.temperature", sample_msg)

        callback.reset_mock()

        # Event message -> callback should NOT be invoked
        event_msg = {
            "TYPE": "Events",
            "VALUE": 123,
            "TAG": "Execution",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.execution", event_msg)

        callback.assert_not_called()

    def test_stop_samples_subscription_removes_callback(self, MockAssetProducer):
        """ Test stop_samples_subscription unregisters the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_samples(callback)
        self.assertIs(asset._samples_callback, callback)

        asset.stop_samples_subscription()

        self.assertIsNone(asset._samples_callback)

    def test_subscribe_to_events_registers_callback(self, MockAssetProducer):
        """ Test subscribe_to_events registers the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_events(callback)

        self.assertIs(asset._events_callback, callback)

    def test_subscribe_to_events_callback_invoked(self, MockAssetProducer):
        """ Test registered callback is invoked only for event messages. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_events(callback)

        # Event message -> callback should be invoked
        event_msg = {
            "TYPE": "Events",
            "VALUE": 123,
            "TAG": "Execution",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.execution", event_msg)

        callback.assert_called_once_with(f"{asset.asset_uuid.upper()}.execution", event_msg)

        callback.reset_mock()

        # Sample message -> callback should NOT be invoked
        sample_msg = {
            "TYPE": "Samples",
            "VALUE": 42,
            "TAG": "Temperature",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.temperature", sample_msg)

        callback.assert_not_called()

    def test_stop_events_subscription_removes_callback(self, MockAssetProducer):
        """ Test stop_events_subscription unregisters the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_events(callback)
        self.assertIs(asset._events_callback, callback)

        asset.stop_events_subscription()

        self.assertIsNone(asset._events_callback)

    def test_subscribe_to_conditions_registers_callback(self, MockAssetProducer):
        """ Test subscribe_to_conditions registers the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_conditions(callback)

        self.assertIs(asset._conditions_callback, callback)

    def test_subscribe_to_conditions_callback_invoked(self, MockAssetProducer):
        """ Test registered callback is invoked only for condition messages. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_conditions(callback)

        # Condition message -> callback should be invoked
        condition_msg = {
            "TYPE": "Condition",
            "VALUE": 123,
            "TAG": "Execution",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.execution", condition_msg)

        callback.assert_called_once_with(f"{asset.asset_uuid.upper()}.execution", condition_msg)

        callback.reset_mock()

        # Sample message -> callback should NOT be invoked
        sample_msg = {
            "TYPE": "Samples",
            "VALUE": 42,
            "TAG": "Temperature",
            "attributes": {"timestamp": "MockedTimeStamp"},
        }

        asset._on_message(f"{asset.asset_uuid.upper()}.temperature", sample_msg)

        callback.assert_not_called()

    def test_stop_conditions_subscription_removes_callback(self, MockAssetProducer):
        """ Test stop_conditions_subscription unregisters the callback. """
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock())
        callback = MagicMock()

        asset.subscribe_to_conditions(callback)
        self.assertIs(asset._conditions_callback, callback)

        asset.stop_conditions_subscription()

        self.assertIsNone(asset._conditions_callback)
