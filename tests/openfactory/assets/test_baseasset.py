import json
from itertools import chain, repeat
from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from openfactory.exceptions import OFAException
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.assets.asset_base import BaseAsset, KafkaAssetConsumer


class ValidAsset(BaseAsset):
    """ A valid subclass of BaseAsset """
    KSQL_ASSET_TABLE = "assets"
    KSQL_ASSET_ID = "asset_uuid"
    ASSET_CONSUMER_CLASS = KafkaAssetConsumer

    def __init__(self, asset_id, ksqlClient, bootstrap_servers='MockedBroker'):
        object.__setattr__(self, 'ASSET_ID', asset_id)
        super().__init__(ksqlClient, bootstrap_servers)

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
        datetime_patcher = patch("openfactory.assets.utils.datetime")
        self.mock_datetime = datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # Make datetime.now() return fixed timestamp
        self.mock_datetime.now.return_value = self.fixed_ts
        # Allow datetime(...) constructor to still work
        self.mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

    def test_valid_subclass(self, MockAssetProducer):
        """ Test valid subclass """
        asset = ValidAsset('some_id', self.ksql_mock)
        self.assertEqual(asset.ksql, self.ksql_mock)
        self.assertEqual(asset.bootstrap_servers, 'MockedBroker')

        # Confirm mock constructor was called
        MockAssetProducer.assert_called_once_with(
            ksqlClient=self.ksql_mock, bootstrap_servers='MockedBroker'
        )
        # Confirm the asset is using the mock instance
        self.assertEqual(asset.producer, MockAssetProducer.return_value)

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

    def test_type_returns_unavailable_when_empty(self, MockAssetProducer):
        """ Test if asset.type returns 'UNAVAILABLE' when the ksql query yields no results """

        # Simulate an empty result from ksqlDB
        self.ksql_mock.query.return_value = []

        asset = ValidAsset('some_id', self.ksql_mock)

        # Expect 'UNAVAILABLE' when no data is returned
        self.assertEqual(asset.type, 'UNAVAILABLE')

        # Check if the correct query was executed
        expected_query = "SELECT TYPE FROM assets_type WHERE ASSET_UUID='some_id';"
        self.ksql_mock.query.assert_called_once_with(expected_query)

    def test_type_returns_value_when_present(self, MockAssetProducer):
        """ Test if asset.type returns the correct value when the ksql query returns data """

        # Simulate a valid result from ksqlDB with type 'Condition'
        ksql_mock = Mock(spec=KSQLDBClient)
        ksql_mock.query.return_value = [{'TYPE': 'Condition'}]

        asset = ValidAsset('some_id', ksql_mock)

        # Expect the actual type returned from the query
        self.assertEqual(asset.type, 'Condition')

        # Check if the correct query was executed
        expected_query = "SELECT TYPE FROM assets_type WHERE ASSET_UUID='some_id';"
        ksql_mock.query.assert_called_once_with(expected_query)

    def test_attributes_success(self, MockAssetProducer):
        """ Test attributes() returns correct attribute IDs """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {"ID": 101},
            {"ID": 102},
            {"ID": 103}
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
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        samples = asset._get_attributes_by_type('Samples')

        self.assertEqual(samples, [{'ID': 'id1', 'VALUE': 'val1', 'TAG': 'MockedTag'}])

        # Ensure correct query was executed
        expected_query = f"SELECT ID, VALUE, TAG, TYPE FROM {asset.KSQL_ASSET_TABLE} WHERE {asset.KSQL_ASSET_ID}='{asset.ASSET_ID}' AND TYPE='Samples';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_samples(self, MockAssetProducer):
        """ Test samples() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id1",
                "VALUE": "val1",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        samples = asset.samples()

        self.assertEqual(samples, [{'ID': 'id1', 'VALUE': 'val1', 'TAG': 'MockedTag'}])

        # Ensure correct query was executed
        expected_query = f"SELECT ID, VALUE, TAG, TYPE FROM {asset.KSQL_ASSET_TABLE} WHERE {asset.KSQL_ASSET_ID}='{asset.ASSET_ID}' AND TYPE='Samples';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_events(self, MockAssetProducer):
        """ Test events() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id2",
                "VALUE": "val2",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}MockedTag"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        events = asset.events()

        self.assertEqual(events, [{'ID': 'id2', 'VALUE': 'val2', 'TAG': 'MockedTag'}])

        # Ensure correct query was executed
        expected_query = f"SELECT ID, VALUE, TAG, TYPE FROM {asset.KSQL_ASSET_TABLE} WHERE {asset.KSQL_ASSET_ID}='{asset.ASSET_ID}' AND TYPE='Events';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_conditions(self, MockAssetProducer):
        """ Test conditions() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id3",
                "VALUE": "val3",
                "TAG": "{urn:mtconnect.org:MTConnectStreams:2.2}Fault"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        conditions = asset.conditions()

        expected_conditions = [{
            "ID": "id3",
            "VALUE": "val3",
            "TAG": "Fault"  # The namespace is removed
        }]
        self.assertEqual(conditions, expected_conditions)

        # Ensure correct query was exectued
        expected_query = f"SELECT ID, VALUE, TAG, TYPE FROM {asset.KSQL_ASSET_TABLE} WHERE {asset.KSQL_ASSET_ID}='{asset.ASSET_ID}' AND TYPE='Condition';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_methods(self, MockAssetProducer):
        """ Test methods() """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {"ID": "id4", "VALUE": "val4"}
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        methods = asset.methods()

        self.assertEqual(methods, {'id4': 'val4'})

        # Ensure correct query was exectued
        expected_query = f"SELECT ID, VALUE, TYPE FROM {asset.KSQL_ASSET_TABLE} WHERE {asset.KSQL_ASSET_ID}='{asset.ASSET_ID}' AND TYPE='Method';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_method_execution(self, MockAssetProducer):
        """ Test method() sends the correct Kafka message """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [{"ID": "ID1"}]

        # Mock the Kafka topic resolution
        ksqlMock.get_kafka_topic.return_value = "test_topic"

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        asset.producer = MagicMock()
        ksqlMock.get_kafka_topic.reset_mock()

        # Call the method
        asset.method("start", "param1 param2")

        # Check Kafka topic resolution
        ksqlMock.get_kafka_topic.assert_called_once_with("CMDS_STREAM")

        # Expected message
        expected_msg = {
            "CMD": "start",
            "ARGS": "param1 param2"
        }

        # Ensure produce() was called with correct values
        asset.producer.produce.assert_called_once_with(
            topic="test_topic",
            key="uuid-123",
            value=json.dumps(expected_msg)
        )

        # Ensure flush() was called
        asset.producer.flush.assert_called_once()

    def test_setattr_non_asset_attribute(self, MockAssetProducer):
        """ Test setting a non-asset attribute (not in attributes list) """
        # Mock asset with a single 'temperature' attribute
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.attributes = MagicMock(return_value=["temperature"])

        asset.new_attr = "something"
        self.assertEqual(asset.new_attr, "something")

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
        asset.attributes = MagicMock(return_value=["temperature"])

        attr = AssetAttribute(id='temperature', value=25, tag="Temperature", type="Samples")
        asset.temperature = attr

        mock_producer.send_asset_attribute.assert_called_once_with("uuid-123", attr)

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

        # Ensure correct query was executed
        expected_query = (
            "SELECT VALUE, TYPE, TAG, TIMESTAMP "
            "FROM assets WHERE key='uuid-123|some_missing_attribute';"
        )
        ksqlMock.query.assert_any_call(expected_query)

    def test_setattr_valid_asset_attribute_with_raw_value(self, MockAssetProducer):
        """ Test setting a defined asset attribute with a raw value (not an AssetAttribute) """
        mock_producer = MockAssetProducer.return_value
        asset = ValidAsset("uuid-123", ksqlClient=MagicMock(), bootstrap_servers="mock_broker")
        asset.attributes = MagicMock(return_value=["temperature"])

        # Simulate current attribute with metadata
        current_attr = AssetAttribute(id="temperature", value=10, tag="Temperature", type="Samples")
        asset.__getattr__ = MagicMock(return_value=current_attr)

        asset.temperature = 30

        mock_producer.send_asset_attribute.assert_called_once()
        expected = AssetAttribute(id="temperature", value=30, tag="Temperature", type="Samples")
        mock_producer.send_asset_attribute.assert_called_once_with("uuid-123", expected)

    def test_getattr_samples(self, MockAssetProducer):
        """ Test __getattr__ returns float for 'Samples' type """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id1",
                "VALUE": "42.5",
                "TYPE": "Samples",
                "TAG": "MockedTag",
                "TIMESTAMP": "MockedTimeStamp"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attribute = asset.id1

        expected = AssetAttribute(id='id1', value=42.5, type='Samples', tag='MockedTag', timestamp='MockedTimeStamp')
        self.assertEqual(attribute, expected)

        # Ensure correct query was exectued
        expected_query = "SELECT VALUE, TYPE, TAG, TIMESTAMP FROM assets WHERE key='uuid-123|id1';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_getattr_string_value(self, MockAssetProducer):
        """ Test __getattr__ returns raw VALUE for non-'Samples' and non-'Method' types """
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [
            {
                "ID": "id2",
                "VALUE": "val2",
                "TYPE": "Events",
                "TAG": "MockedTag",
                "TIMESTAMP": "MockedTimeStamp"
            }
        ]

        asset = ValidAsset("uuid-123", ksqlClient=ksqlMock)
        attribute = asset.id2

        expected = AssetAttribute(id='id2', value='val2', type='Events', tag='MockedTag', timestamp='MockedTimeStamp')
        self.assertEqual(attribute, expected)

        # Ensure correct query was exectued
        expected_query = "SELECT VALUE, TYPE, TAG, TIMESTAMP FROM assets WHERE key='uuid-123|id2';"
        ksqlMock.query.assert_any_call(expected_query)

    @patch("openfactory.assets.asset_base.BaseAsset.method")
    def test_getattr_method(self, mock_method, MockAssetProducer):
        """ Test __getattr__ returns a callable for 'Method' type """
        mock_method.return_value = "Mocked method called successfully"

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
        ret = asset.a_method('arg1', 'arg2')

        self.assertEqual(ret, "Mocked method called successfully")
        mock_method.assert_called_once_with("a_method", "arg1 arg2")

        # Ensure correct query was exectued
        expected_query = "SELECT VALUE, TYPE, TAG, TIMESTAMP FROM assets WHERE key='uuid-123|a_method';"
        ksqlMock.query.assert_any_call(expected_query)

    def test_get_reference_list_not_implemented(self, MockAssetProducer):
        """ Test if _get_reference_list raises NotImplementedError when not implemented in subclass """

        asset = ValidAsset('some_id', self.ksql_mock)

        with self.assertRaises(NotImplementedError):
            asset._get_reference_list('above')

    def test_references_above_uuid_calls_get_reference_list(self, MockAssetProducer):
        """ Test references_above_uuid() calls _get_reference_list with direction='above' and as_assets=False """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.references_above_uuid()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="above", as_assets=False)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_references_above_calls_get_reference_list(self, MockAssetProducer):
        """ Test references_above() calls _get_reference_list with direction='above' and as_assets=True """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.references_above()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="above", as_assets=True)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_references_below_uuid_calls_get_reference_list(self, MockAssetProducer):
        """ Test references_below_uuid() calls _get_reference_list with direction='below' and as_assets=False """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.references_below_uuid()

        # Assert the method was called with correct parameters
        asset._get_reference_list.assert_called_once_with(direction="below", as_assets=False)

        # Assert the return value is passed through
        self.assertEqual(result, ["mocked-asset"])

    def test_references_below_calls_get_reference_list(self, MockAssetProducer):
        """ Test references_below() calls _get_reference_list with direction='below' and as_assets=True """

        asset = ValidAsset("uuid-123", MagicMock())

        # Replace _get_reference_list with a MagicMock
        asset._get_reference_list = MagicMock(return_value=["mocked-asset"])

        result = asset.references_below()

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
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [{"VALUE": "existing-ref1, existing-ref2", "ID": "ID1"}]
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
        ksqlMock = MagicMock()
        ksqlMock.query.return_value = [{"VALUE": "existing-ref1, existing-ref2", "ID": "ID1"}]
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

    @patch('openfactory.assets.asset_base.uuid.uuid4')
    @patch('openfactory.assets.asset_base.time.time')
    def test_wait_until_attribute_matches_initially(self, mock_time, mock_uuid, MockAssetProducer):
        """ Test wait_until when the attribute matches initially """
        # Mock the Asset object
        mock_ksql = MagicMock()
        asset = ValidAsset("test_uuid", ksqlClient=mock_ksql)

        # Mock the attribute value to match
        mock_attribute = MagicMock()
        mock_attribute.value = "expected_value"
        asset.__getattr__ = MagicMock(return_value=mock_attribute)

        # Call the method
        result = asset.wait_until(attribute="test_attribute", value="expected_value")

        # Assert the result is True
        self.assertTrue(result)
        asset.__getattr__.assert_called_once_with("test_attribute")

    @patch("openfactory.assets.asset_base.delete_consumer_group")
    def test_wait_until_matches_kafka_message(self, mock_delete_group, MockAssetProducer):
        """ Test wait_until returns True when a Kafka message matches (In case of none-Samples DataItems) """

        # Simulate a Kafka message that matches the desired attribute and value
        mock_msg = Mock()
        mock_msg.key.return_value = b"test_uuid"
        mock_msg.value.return_value = json.dumps({
            "id": "test_attribute",
            "type": "Events",
            "value": "expected_value"
        }).encode("utf-8")
        mock_msg.error.return_value = None

        # Create mock Kafka consumer that yields that message
        mock_consumer_instance = Mock()
        mock_consumer_instance.poll.side_effect = [mock_msg]

        # Create dummy consumer class
        class DummyKafkaConsumer(KafkaAssetConsumer):
            def __init__(self, *args, **kwargs):
                self.consumer = mock_consumer_instance

        # Subclass BaseAsset with correct consumer
        class AssetWithConsumer(BaseAsset):
            KSQL_ASSET_TABLE = "assets"
            KSQL_ASSET_ID = "asset_uuid"
            ASSET_CONSUMER_CLASS = DummyKafkaConsumer

            def __init__(self, asset_id, ksqlClient, bootstrap_servers="mock_broker"):
                object.__setattr__(self, "ASSET_ID", asset_id)
                super().__init__(ksqlClient, bootstrap_servers)

            @property
            def asset_uuid(self):
                return self.ASSET_ID

            def __getattr__(self, attr):
                return Mock(value="not_expected_value")  # simulate initial mismatch

        asset = AssetWithConsumer("test_uuid", ksqlClient=Mock())
        result = asset.wait_until(attribute="test_attribute", value='expected_value', timeout=1)

        assert result is True
        mock_consumer_instance.close.assert_called_once()
        mock_delete_group.assert_called_once()

    @patch("openfactory.assets.asset_base.delete_consumer_group")
    def test_wait_until_matches_kafka_samples_message(self, mock_delete_group, MockAssetProducer):
        """ Test wait_until returns True when a Kafka message matches (In case of Samples DataItems) """

        # Simulate a Kafka message that matches the desired attribute and value
        mock_msg = Mock()
        mock_msg.key.return_value = b"test_uuid"
        mock_msg.value.return_value = json.dumps({
            "id": "temperature",
            "type": "Samples",
            "value": "42.0"
        }).encode("utf-8")
        mock_msg.error.return_value = None

        # Create mock Kafka consumer that yields that message
        mock_consumer_instance = Mock()
        mock_consumer_instance.poll.side_effect = [mock_msg]

        # Create dummy consumer class
        class DummyKafkaConsumer(KafkaAssetConsumer):
            def __init__(self, *args, **kwargs):
                self.consumer = mock_consumer_instance

        # Subclass BaseAsset with correct consumer
        class AssetWithConsumer(BaseAsset):
            KSQL_ASSET_TABLE = "assets"
            KSQL_ASSET_ID = "asset_uuid"
            ASSET_CONSUMER_CLASS = DummyKafkaConsumer

            def __init__(self, asset_id, ksqlClient, bootstrap_servers="mock_broker"):
                object.__setattr__(self, "ASSET_ID", asset_id)
                super().__init__(ksqlClient, bootstrap_servers)

            @property
            def asset_uuid(self):
                return self.ASSET_ID

            def __getattr__(self, attr):
                return Mock(value="not_42")  # simulate initial mismatch

        asset = AssetWithConsumer("test_uuid", ksqlClient=Mock())
        result = asset.wait_until(attribute="temperature", value=42.0, timeout=1)

        assert result is True
        mock_consumer_instance.close.assert_called_once()
        mock_delete_group.assert_called_once()

    @patch("openfactory.assets.asset_base.delete_consumer_group")
    def test_wait_until_times_out(self, mock_delete_group, MockAssetProducer):
        """ Test wait_until returns False on timeout with no matching Kafka message """

        # Simulated Kafka consumer that always returns None (no messages)
        mock_consumer_instance = Mock()
        mock_consumer_instance.poll.return_value = None

        # Dummy Kafka consumer class that injects the mock consumer
        class DummyKafkaConsumer(KafkaAssetConsumer):
            def __init__(self, *args, **kwargs):
                self.consumer = mock_consumer_instance

        # Subclass of BaseAsset using the dummy consumer
        class AssetWithTimeout(BaseAsset):
            KSQL_ASSET_TABLE = "assets"
            KSQL_ASSET_ID = "asset_uuid"
            ASSET_CONSUMER_CLASS = DummyKafkaConsumer

            def __init__(self, asset_id, ksqlClient, bootstrap_servers="mock_broker"):
                object.__setattr__(self, "ASSET_ID", asset_id)
                super().__init__(ksqlClient, bootstrap_servers)

            @property
            def asset_uuid(self):
                return self.ASSET_ID

            def __getattr__(self, attr):
                return Mock(value="not_42")  # initial mismatch

        # Instantiate asset and invoke wait_until with short timeout
        asset = AssetWithTimeout("uuid-timeout", ksqlClient=Mock())
        result = asset.wait_until(attribute="temperature", value=42.0, timeout=1)

        assert result is False
        mock_consumer_instance.close.assert_called_once()
        mock_delete_group.assert_called_once()

    def test_wait_until_ksqldb_matches(self, MockAssetProducer):
        """ Test wait_until with use_ksqlDB=True returns True when ksqlDB eventually matches """
        asset = ValidAsset("test_uuid", ksqlClient=MagicMock())
        asset.__getattr__ = MagicMock(side_effect=[MagicMock(value="initial"), MagicMock(value="target")])

        # Test when use_ksqlDB is True
        result = asset.wait_until(attribute="test_attribute", value="target", timeout=10, use_ksqlDB=True)
        self.assertTrue(result)

    def test_wait_until_ksqldb_timeout(self, MockAssetProducer):
        """ Test wait_until with use_ksqlDB=True returns False after timeout when no match is found """
        asset = ValidAsset("test_uuid", ksqlClient=MagicMock())
        asset.__getattr__ = MagicMock(return_value=MagicMock(value="initial"))

        # Test timeout when use_ksqlDB is True
        result = asset.wait_until(attribute="test_attribute", value="target", timeout=1, use_ksqlDB=True)
        self.assertFalse(result)

    @patch("openfactory.assets.asset_base.delete_consumer_group")
    def test_wait_until_handles_invalid_json_message(self, mock_delete_group, MockAssetProducer):
        """ Test wait_until gracefully skips invalid JSON Kafka messages """

        class FakeKafkaMessage:
            def key(self):
                return b"test_uuid"

            def value(self):
                return b"not-a-json"

            def error(self):
                return None

        # Create a mock consumer instance with poll side_effect returning one bad message then Nones
        mock_consumer_instance = Mock()
        mock_consumer_instance.poll.side_effect = chain([FakeKafkaMessage()], repeat(None))

        # DummyKafkaConsumer overriding consumer with the mock
        class DummyKafkaConsumer(KafkaAssetConsumer):
            def __init__(self, *args, **kwargs):
                self.consumer = mock_consumer_instance

        class AssetWithConsumer(ValidAsset):
            ASSET_CONSUMER_CLASS = DummyKafkaConsumer

            def __getattr__(self, attr):
                return Mock(value="not_42")

        asset = AssetWithConsumer("test_uuid", ksqlClient=Mock())

        result = asset.wait_until(attribute="temperature", value=42.0, timeout=0.5)

        assert result is False
        mock_consumer_instance.close.assert_called_once()
        mock_delete_group.assert_called_once()
