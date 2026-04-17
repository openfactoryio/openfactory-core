from unittest import TestCase
from unittest.mock import Mock
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.assets.asset_base import BaseAsset, KafkaAssetConsumer


class ValidAsset(BaseAsset):
    """ A valid Asset in test_mode """

    KSQL_ASSET_TABLE = "assets"
    KSQL_ASSET_ID = "asset_uuid"
    ASSET_CONSUMER_CLASS = KafkaAssetConsumer

    def __init__(self, asset_id, ksqlClient, test_mode):
        object.__setattr__(self, 'ASSET_ID', asset_id)
        super().__init__(ksqlClient, test_mode=test_mode)

    @property
    def asset_uuid(self):
        return self.ASSET_ID


class TestBaseAssetTestMode(TestCase):
    """
    Test class BaseAsset in test_mode
    """

    def setUp(self):
        self.ksql_mock = Mock(spec=KSQLDBClient)

    def test_regular_attributes(self):
        """ Test regular python attributes """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Set arbitrary attributes
        asset.foo = 123
        asset.bar = "hello"

        self.assertEqual(asset.foo, 123)
        self.assertEqual(asset.bar, "hello")

    def test_add_attribute(self):
        """ Test that add_attributes populates internal list in test_mode """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='test_attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='test_attribute2', value='attr1', type='Events', tag='test')
        asset.add_attribute(attribute1)
        asset.add_attribute(attribute2)

        self.assertEqual(asset._mocked_attributes, [attribute1, attribute2])

    def test_attributes(self):
        """ Test attributes() """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        asset.add_attribute(attribute1)
        asset.add_attribute(attribute2)

        self.assertEqual(asset.attributes(), ['attribute1', 'attribute2'])

    def test_get_attributes_by_type(self):
        """ Test _get_attributes_by_type """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        asset.add_attribute(attribute1)
        asset.add_attribute(attribute2)

        self.assertEqual(asset._get_attributes_by_type('Events'), [{'ID': 'attribute1', 'VALUE': 'attr1', 'TAG': 'test'}])
        self.assertEqual(asset._get_attributes_by_type('Samples'), [{'ID': 'attribute2', 'VALUE': 5, 'TAG': 'test'}])

    def test_samples(self):
        """ Test samples() """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value=5, type='Samples', tag='test')
        asset.add_attribute(attr)

        self.assertEqual(asset.samples(), [{'ID': 'attribute', 'VALUE': 5, 'TAG': 'test'}])

    def test_events(self):
        """ Test events() """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value='some_value', type='Events', tag='test')
        asset.add_attribute(attr)

        self.assertEqual(asset.events(), [{'ID': 'attribute', 'VALUE': 'some_value', 'TAG': 'test'}])

    def test_get_mocked_attribute_by_id(self):
        """ Test __get_mocked_attribute_by_id """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add an AssetAttribute
        attr = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        asset.add_attribute(attr)
        self.assertEqual(asset._get_mocked_attribute_by_id('attribute1'), attr)

        # A none existing AssetAttribute should return None
        self.assertEqual(asset._get_mocked_attribute_by_id('does_not_exist'), None)

    def test_setattr_in_test_mode_bypasses_producer(self):
        """ Test AssetAttribute attributes """
        asset = ValidAsset("uuid-test", self.ksql_mock, test_mode=True)

        # Add an AssetAttribute
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        asset.add_attribute(attribute1)
        self.assertEqual(asset.attribute1.value, "attr1")

        # Set new value
        asset.attribute1 = "hello"
        self.assertEqual(asset.attribute1.value, "hello")
