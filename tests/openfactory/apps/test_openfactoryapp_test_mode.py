from unittest import TestCase
from unittest.mock import Mock
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute
from openfactory.apps import OpenFactoryApp, EventAttribute, SampleAttribute, ofa_method


class TestApp(OpenFactoryApp):

    status = EventAttribute(value="idle", tag="App.Status")
    sample_rate = SampleAttribute(value=42, tag="Sample.Rate")

    @ofa_method()
    def stop_axis(self):
        """ Stops all motion immediately. """
        self.logger.info("Stopping all axis")


class TestOpenFactoryAppTestMode(TestCase):
    """
    Test class OpenFactoryApp in test_mode
    """

    def setUp(self):
        self.ksql_mock = Mock(spec=KSQLDBClient)

        self.app = TestApp(
            asset_router_url='mocked_asset_url',
            bootstrap_servers='mocked_broker',
            ksqlClient=self.ksql_mock,
            test_mode=True
        )

    def test_regular_attributes(self):
        """ Test regular python attributes """
        # Set arbitrary attributes
        self.app.foo = 123
        self.app.bar = "hello"

        self.assertEqual(self.app.foo, 123)
        self.assertEqual(self.app.bar, "hello")

    def test_declarative_attributes(self):
        """ Test declarative attributes """
        self.assertEqual(self.app.sample_rate.value, 42)
        self.assertEqual(self.app.sample_rate.type, 'Samples')
        self.assertEqual(self.app.sample_rate.tag, 'Sample.Rate')
        self.assertEqual(self.app.status.value, "idle")
        self.assertEqual(self.app.status.type, 'Events')
        self.assertEqual(self.app.status.tag, 'App.Status')

    def test_ofa_methods_attributes(self):
        """ Test ofa_methods command attributes """
        self.assertEqual(self.app.stop_axis_CMD.value, '')
        self.assertEqual(self.app.stop_axis_CMD.type, 'OpenFactory')
        self.assertEqual(self.app.stop_axis_CMD.tag, 'Method.Command')

    def test_add_attribute(self):
        """ Test that add_attributes populates internal list in test_mode """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='test_attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='test_attribute2', value='attr1', type='Events', tag='test')
        self.app.add_attribute(attribute1)
        self.app.add_attribute(attribute2)

        self.assertIn(attribute1, self.app._mocked_attributes)
        self.assertIn(attribute2, self.app._mocked_attributes)

    def test_attributes(self):
        """ Test attributes() """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        self.app.add_attribute(attribute1)
        self.app.add_attribute(attribute2)

        self.assertIn('attribute1', self.app.attributes())
        self.assertIn('attribute2', self.app.attributes())

    def test_get_attributes_by_type(self):
        """ Test _get_attributes_by_type """
        # Add two AssetAttributes
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        attribute2 = AssetAttribute(id='attribute2', value=5, type='Samples', tag='test')
        self.app.add_attribute(attribute1)
        self.app.add_attribute(attribute2)

        self.assertIn({'ID': 'attribute1', 'VALUE': 'attr1', 'TAG': 'test'}, self.app._get_attributes_by_type('Events'))
        self.assertIn({'ID': 'attribute2', 'VALUE': 5, 'TAG': 'test'}, self.app._get_attributes_by_type('Samples'))

    def test_samples(self):
        """ Test samples() """
        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value=5, type='Samples', tag='test')
        self.app.add_attribute(attr)

        self.assertIn({'ID': 'attribute', 'VALUE': 5, 'TAG': 'test'}, self.app.samples())

    def test_events(self):
        """ Test events() """
        # Add two AssetAttributes
        attr = AssetAttribute(id='attribute', value='some_value', type='Events', tag='test')
        self.app.add_attribute(attr)

        self.assertIn({'ID': 'attribute', 'VALUE': 'some_value', 'TAG': 'test'}, self.app.events())

    def test_get_mocked_attribute_by_id(self):
        """ Test __get_mocked_attribute_by_id """
        # Add an AssetAttribute
        attr = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        self.app.add_attribute(attr)
        self.assertEqual(self.app._get_mocked_attribute_by_id('attribute1'), attr)

        # A none existing AssetAttribute should return None
        self.assertEqual(self.app._get_mocked_attribute_by_id('does_not_exist'), None)

    def test_setattr_in_test_mode_bypasses_producer(self):
        """ Test AssetAttribute attributes """
        # Add an AssetAttribute
        attribute1 = AssetAttribute(id='attribute1', value='attr1', type='Events', tag='test')
        self.app.add_attribute(attribute1)
        self.assertEqual(self.app.attribute1.value, "attr1")

        # Set new value
        self.app.attribute1 = "hello"
        self.assertEqual(self.app.attribute1.value, "hello")
