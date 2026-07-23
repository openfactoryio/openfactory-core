import unittest
from unittest.mock import MagicMock, patch
from openfactory.apps.attributefield import EventAttribute, SampleAttribute
from openfactory.apps import OpenFactoryApp


class TestApp(OpenFactoryApp):
    """ Test app with declarative attributes. """

    app_version = EventAttribute(value="1.2.3", tag="App.Version")
    license_type = EventAttribute(value="MIT", tag="App.License")
    sample_rate = SampleAttribute(value=42, tag="Sample.Rate")
    temp = SampleAttribute(tag="Temperature")

    def main_loop(self):
        pass


class TestDeclarativeAttributes(unittest.TestCase):

    def setUp(self):
        """ Instantiate the test app before each test. """
        self.ksql_mock = MagicMock()

        mock_data = {
            "app_version": {"VALUE": "1.2.3", "TYPE": "Events", "TAG": "App.Version", "TIMESTAMP": "2026-03-30T00:00:00"},
            "license_type": {"VALUE": "MIT", "TYPE": "Events", "TAG": "App.License", "TIMESTAMP": "2026-03-30T00:00:00"},
            "sample_rate": {"VALUE": 42, "TYPE": "Samples", "TAG": "Sample.Rate", "TIMESTAMP": "2026-03-30T00:00:00"},
            "temp": {"VALUE": 23, "TYPE": "Samples", "TAG": "Sample.Rate", "TIMESTAMP": "2026-03-30T00:00:00"},
        }

        def query_side_effect(q):
            for key, val in mock_data.items():
                if key in q:
                    return [val]
            return []

        self.ksql_mock.query.side_effect = query_side_effect

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        # Patch wait_until
        self.wait_until_patcher = patch("openfactory.assets.asset_base.BaseAsset.wait_until", return_value=True)
        self.wait_until_patcher.start()
        self.addCleanup(self.wait_until_patcher.stop)

        self.app = TestApp(ksqlClient=self.ksql_mock, bootstrap_servers="mocked_broker", asset_router_url="mocked_url")

    def test_declared_attributes_collection(self):
        """ Check that declarative attributes are collected in _declared_attributes. """
        declared = self.app._declared_attributes
        self.assertIn("app_version", declared)
        self.assertIsInstance(declared["app_version"], EventAttribute)

        self.assertIn("license_type", declared)
        self.assertIsInstance(declared["license_type"], EventAttribute)

        self.assertIn("sample_rate", declared)
        self.assertIsInstance(declared["sample_rate"], SampleAttribute)

        self.assertIn("temp", declared)
        self.assertIsInstance(declared["temp"], SampleAttribute)

    def test_add_attribute_called_for_declaratives(self):
        """ Check that add_attribute is called with correct AssetAttribute for each declarative attribute. """
        with patch.object(TestApp, "add_attribute", autospec=True) as mock_add_attr:
            TestApp(ksqlClient=self.ksql_mock, bootstrap_servers="mocked_broker", asset_router_url="mocked_url")

            # Grab all calls to add_attribute like this: add_attribute(asset_attribute=...)
            calls = [call.kwargs["asset_attribute"] for call in mock_add_attr.mock_calls if "asset_attribute" in call.kwargs]

            # Convert to dict by id for easy access
            asset_dict = {attr.id: attr for attr in calls}

            # Check declarative attributes
            self.assertIn("app_version", asset_dict)
            self.assertEqual(asset_dict["app_version"].value, "1.2.3")
            self.assertEqual(asset_dict["app_version"].tag, "App.Version")
            self.assertEqual(asset_dict["app_version"].type, "Events")

            self.assertIn("license_type", asset_dict)
            self.assertEqual(asset_dict["license_type"].value, "MIT")
            self.assertEqual(asset_dict["license_type"].tag, "App.License")
            self.assertEqual(asset_dict["license_type"].type, "Events")

            self.assertIn("sample_rate", asset_dict)
            self.assertEqual(asset_dict["sample_rate"].value, 42)
            self.assertEqual(asset_dict["sample_rate"].tag, "Sample.Rate")
            self.assertEqual(asset_dict["sample_rate"].type, "Samples")

            self.assertIn("temp", asset_dict)
            self.assertEqual(asset_dict["temp"].value, "UNAVAILABLE")
            self.assertEqual(asset_dict["temp"].tag, "Temperature")
            self.assertEqual(asset_dict["temp"].type, "Samples")

    def test_attribute_access_from_class_returns_descriptor(self):
        """ Check that class access returns the declared attribute descriptor itself. """
        self.assertIs(TestApp.app_version, TestApp._declared_attributes["app_version"])
        self.assertIs(TestApp.license_type, TestApp._declared_attributes["license_type"])
        self.assertIs(TestApp.sample_rate, TestApp._declared_attributes["sample_rate"])
        self.assertIs(TestApp.temp, TestApp._declared_attributes["temp"])

        self.assertIsInstance(TestApp.app_version, EventAttribute)
        self.assertIsInstance(TestApp.license_type, EventAttribute)
        self.assertIsInstance(TestApp.sample_rate, SampleAttribute)
        self.assertIsInstance(TestApp.temp, SampleAttribute)

    def test_attribute_access_from_instance_delegates_to_getattr(self):
        """ Check that accessing declarative attributes delegates to __getattr__. """
        with patch.object(TestApp, "__getattr__", autospec=True) as mock_getattr:
            mock_getattr.side_effect = lambda instance, name: f"value-for-{name}"

            self.assertEqual(self.app.app_version, "value-for-app_version")
            self.assertEqual(self.app.license_type, "value-for-license_type")
            self.assertEqual(self.app.sample_rate, "value-for-sample_rate")
            self.assertEqual(self.app.temp, "value-for-temp")

            self.assertEqual(
                [call.args[1] for call in mock_getattr.call_args_list],
                ["app_version", "license_type", "sample_rate", "temp"],
            )

    def test_descriptor_names_are_set_by_metaclass(self):
        """ Check that the metaclass assigns the declared attribute name to each descriptor. """
        self.assertEqual(TestApp.app_version.name, "app_version")
        self.assertEqual(TestApp.license_type.name, "license_type")
        self.assertEqual(TestApp.sample_rate.name, "sample_rate")
        self.assertEqual(TestApp.temp.name, "temp")

    def test_declarative_attribute_instance_access_delegates_to_getattr(self):
        """ Access through an instance delegates to __getattr__. """
        with patch.object(TestApp, "__getattr__", return_value="resolved") as mock_getattr:
            result = self.app.temp

            mock_getattr.assert_called_once_with("temp")
            self.assertEqual(result, "resolved")
