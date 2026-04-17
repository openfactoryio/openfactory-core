from unittest import TestCase
from unittest.mock import Mock
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.assets.asset_class import Asset


class TestAssetTestMode(TestCase):
    """
    Test class Asset in test_mode
    """

    def setUp(self):
        self.ksql_mock = Mock(spec=KSQLDBClient)

        self.asset = Asset(
            'test_001',
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mocked_broker',
            asset_router_url='mocked_asset_url',
            test_mode=True
        )

    def test_regular_attributes(self):
        """ Test regular python attributes """
        # Set arbitrary attributes
        self.asset.foo = 123
        self.asset.bar = "hello"

        self.assertEqual(self.asset.foo, 123)
        self.assertEqual(self.asset.bar, "hello")

    def test_add_attribute(self):
        """ Test that add_attributes populates internal list in test_mode """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='test_attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='test_attribute2', value='attr1', type='Events', tag='test')
        self.asset.add_attribute(attribute1)
        self.asset.add_attribute(attribute2)

        self.assertEqual(self.asset._mocked_attributes, [attribute1, attribute2])

    def test_attributes(self):
        """ Test attributes() """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        self.asset.add_attribute(attribute1)
        self.asset.add_attribute(attribute2)

        self.assertEqual(self.asset.attributes(), ['attribute1', 'attribute2'])

    def test_get_attributes_by_type(self):
        """ Test _get_attributes_by_type """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        self.asset.add_attribute(attribute1)
        self.asset.add_attribute(attribute2)

        self.assertEqual(self.asset._get_attributes_by_type('Events'), [{'ID': 'attribute1', 'VALUE': 'attr1', 'TAG': 'test'}])
        self.assertEqual(self.asset._get_attributes_by_type('Samples'), [{'ID': 'attribute2', 'VALUE': 5, 'TAG': 'test'}])

    def test_samples(self):
        """ Test samples() """
        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value=5, type='Samples', tag='test')
        self.asset.add_attribute(attr)

        self.assertEqual(self.asset.samples(), [{'ID': 'attribute', 'VALUE': 5, 'TAG': 'test'}])

    def test_events(self):
        """ Test events() """
        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value='some_value', type='Events', tag='test')
        self.asset.add_attribute(attr)

        self.assertEqual(self.asset.events(), [{'ID': 'attribute', 'VALUE': 'some_value', 'TAG': 'test'}])

    def test_get_mocked_attribute_by_id(self):
        """ Test __get_mocked_attribute_by_id """
        # Add an AssetAttribute
        attr = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        self.asset.add_attribute(attr)
        self.assertEqual(self.asset._get_mocked_attribute_by_id('attribute1'), attr)

        # A none existing AssetAttribute should return None
        self.assertEqual(self.asset._get_mocked_attribute_by_id('does_not_exist'), None)

    def test_setattr_in_test_mode_bypasses_producer(self):
        """ Test AssetAttribute attributes """
        # Add an AssetAttribute
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        self.asset.add_attribute(attribute1)
        self.assertEqual(self.asset.attribute1.value, "attr1")

        # Set new value
        self.asset.attribute1 = "hello"
        self.assertEqual(self.asset.attribute1.value, "hello")
