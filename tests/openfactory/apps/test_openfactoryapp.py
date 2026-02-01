import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import signal
import os
import json
from datetime import datetime
from openfactory.apps import OpenFactoryApp
from openfactory.assets import AssetAttribute
from openfactory.filelayer.backend import FileBackend


class TestOpenFactoryApp(unittest.TestCase):
    """
    Tests for class OpenFactoryApp
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        # Patch deregister_asset
        self.deregister_patcher = patch('openfactory.apps.ofaapp.deregister_asset')
        self.mock_deregister = self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

        # Freeze datetime for deterministic AssetAttribute.timestamp
        self.fixed_ts = datetime(2023, 1, 1, 12, 0, 0)
        datetime_patcher = patch("openfactory.assets.utils.time_methods.datetime")
        self.mock_datetime = datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # Make datetime.now() return fixed timestamp
        self.mock_datetime.now.return_value = self.fixed_ts
        # Allow datetime(...) constructor to still work
        self.mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

    def test_inheritance(self):
        """ Test if OpenFactoryApp derives from Asset """
        from openfactory.assets import Asset
        self.assertTrue(issubclass(OpenFactoryApp, Asset), "OpenFactoryApp should derive from Asset")

    def test_initialization_with_env_vars(self):
        """ Test initialization with external environment variables set """
        with patch.dict(os.environ, {
            'APPLICATION_VERSION': '1.0.0',
            'APPLICATION_MANUFACTURER': 'TestFactory',
            'APPLICATION_LICENSE': 'MIT',
            'APP_UUID': 'env-uuid'
        }, clear=True):
            import importlib
            import openfactory.apps.ofaapp as openfactoryapp
            importlib.reload(openfactoryapp)
            OpenFactoryApp = openfactoryapp.OpenFactoryApp

            with patch('openfactory.utils.assets.deregister_asset'):

                app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock)

                self.assertEqual(app.APPLICATION_VERSION, '1.0.0')
                self.assertEqual(app.APPLICATION_MANUFACTURER, 'TestFactory')
                self.assertEqual(app.APPLICATION_LICENSE, 'MIT')
                self.assertEqual(app.asset_uuid, 'env-uuid')

    def test_initialization_with_defaults(self):
        """ Test initialization with no external environment variables set """
        app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')

        self.assertEqual(app.APPLICATION_VERSION, 'latest')
        self.assertEqual(app.APPLICATION_MANUFACTURER, 'OpenFactory')
        self.assertEqual(app.APPLICATION_LICENSE, 'BSD-3-Clause license')
        self.assertEqual(app.asset_uuid, 'init-uuid')

    @patch("openfactory.apps.ofaapp.StorageBackendSchema")
    def test_storage_initialization(self, MockStorageSchema):
        """ Test that storage JSON is parsed and runtime backend instance is created """

        # Mock the StorageBackendSchema instance
        mock_schema_instance = MockStorageSchema.return_value
        mock_backend_instance = MagicMock(spec=FileBackend)
        mock_backend_instance.config = MagicMock()
        mock_backend_instance.config.type = "nfs"
        mock_schema_instance.storage.create_backend_instance.return_value = mock_backend_instance

        # Make get_mount_spec return a dict so json.dumps() works in code under test
        mock_backend_instance.get_mount_spec.return_value = {}

        # Example storage JSON
        storage_json = json.dumps({
            "type": "nfs",
            "server": "10.0.5.2",
            "remote_path": "/nfs/data",
            "mount_point": "/mnt",
            "mount_options": ["rw", "noatime"]
        })

        with patch.dict("os.environ", {"STORAGE": storage_json}):
            app = OpenFactoryApp(app_uuid="test-app", ksqlClient=self.ksql_mock)

        # Assert StorageBackendSchema was called with parsed JSON
        MockStorageSchema.assert_called_once_with(storage=json.loads(storage_json))
        # Assert create_backend_instance() was called
        mock_schema_instance.storage.create_backend_instance.assert_called_once()
        # Assert self.storage is set to the backend instance
        self.assertIs(app.storage, mock_backend_instance)

    def test_attributes_added(self):
        """ Test if attributes are added correctly to the app """
        # Instead of patching globally, we wrap add_attribute to spy calls while calling real method
        original_add_attribute = OpenFactoryApp.add_attribute

        call_args_list = []

        def spy_add_attribute(self_obj, *args, **kwargs):
            call_args_list.append((args, kwargs))
            return original_add_attribute(self_obj, *args, **kwargs)

        with patch.object(OpenFactoryApp, 'add_attribute', new=spy_add_attribute):
            OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')

        # Check that the expected attribute_ids were added
        expected_ids = ['application_version', 'application_manufacturer', 'application_license']
        actual_ids = [kwargs['asset_attribute'].id for args, kwargs in call_args_list]
        for attr_id in expected_ids:
            self.assertIn(attr_id, actual_ids, f"{attr_id} not added via add_attribute")

        # Check that each attribute was created with correct values, type, and tag
        for args, kwargs in call_args_list:
            attr: AssetAttribute = kwargs['asset_attribute']
            if attr.id == 'application_version':
                self.assertEqual(attr.value, 'latest', "Value mismatch for application_version")
                self.assertEqual(attr.type, 'Events', "Type mismatch for application_version")
                self.assertEqual(attr.tag, 'Application.Version', "Tag mismatch for application_version")
            elif attr.id == 'application_manufacturer':
                self.assertEqual(attr.value, 'OpenFactory', "Value mismatch for application_manufacturer")
                self.assertEqual(attr.type, 'Events', "Type mismatch for application_manufacturer")
                self.assertEqual(attr.tag, 'Application.Manufacturer', "Tag mismatch for application_manufacturer")
            elif attr.id == 'application_license':
                self.assertEqual(attr.value, 'BSD-3-Clause license', "Value mismatch for application_license")
                self.assertEqual(attr.type, 'Events', "Type mismatch for application_license")
                self.assertEqual(attr.tag, 'Application.License', "Tag mismatch for application_license")

    @patch('openfactory.apps.ofaapp.configure_prefixed_logger')
    def test_logger_is_configured_correctly(self, mock_configure_logger):
        """ Test that logger is configured with correct prefix and level. """
        mock_logger = MagicMock()
        mock_configure_logger.return_value = mock_logger

        app = OpenFactoryApp(
            app_uuid='test-uuid',
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock-bootstrap',
            loglevel='DEBUG'
        )

        mock_configure_logger.assert_called_once_with(
            'test-uuid',
            prefix='TEST-UUID',
            level='DEBUG'
        )
        self.assertIs(app.logger, mock_logger)
        mock_logger.debug.assert_called_with("Setup OpenFactory App test-uuid")

    def test_signal_sigint(self):
        """ Test signal SIGINT """
        app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        app.app_event_loop_stopped = MagicMock()

        with patch('openfactory.apps.ofaapp.signal.Signals') as mock_signals:
            mock_signals.return_value.name = 'SIGINT'
            # Assert that SystemExit is raised
            with self.assertRaises(SystemExit):
                app.signal_handler(signal.SIGINT, None)

        # Check that deregister_asset was called with the expected arguments
        self.mock_deregister.assert_called_once_with(
            'init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap'
        )
        # Check app_event_loop_stopped was called
        app.app_event_loop_stopped.assert_called_once()

    def test_signal_sigterm(self):
        """ Test signal SIGTERM """
        app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        app.app_event_loop_stopped = MagicMock()

        with patch('openfactory.apps.ofaapp.signal.Signals') as mock_signals:
            mock_signals.return_value.name = 'SIGTERM'
            # Assert that SystemExit is raised
            with self.assertRaises(SystemExit):
                app.signal_handler(signal.SIGINT, None)

        # Check that deregister_asset was called with the expected arguments
        self.mock_deregister.assert_called_once_with(
            'init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap'
        )
        # Check app_event_loop_stopped was called
        app.app_event_loop_stopped.assert_called_once()

    def test_main_loop_not_implemented(self):
        """ Test call to main_loop raise NotImplementedError """
        app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        with self.assertRaises(NotImplementedError):
            app.main_loop()

    def test_run_invokes_main_loop(self):
        """ Test run method invokes main_loop """
        app = OpenFactoryApp(app_uuid='init-uuid', ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        app.main_loop = MagicMock()
        app.run()
        app.main_loop.assert_called_once()

    def test_run_handles_main_loop_exception(self):
        """ Test handling of exception in main_loop """
        app = OpenFactoryApp(
            app_uuid='init-uuid',
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock_bootstrap'
        )
        app.main_loop = MagicMock(side_effect=Exception("Boom"))

        with patch.object(app.logger, 'exception') as mock_logger_exception:
            app.run()
            mock_logger_exception.assert_called_once()
            args, kwargs = mock_logger_exception.call_args
            assert "An error occurred in the main_loop of the app." in args[0]
            self.mock_deregister.assert_called_once()


class TestOpenFactoryAppAsync(unittest.IsolatedAsyncioTestCase):
    """
    Test async methods of OpenFactoryApp
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        # Patch add_attribute
        self.add_attribute_patcher = patch.object(OpenFactoryApp, 'add_attribute')
        self.mock_add_attribute = self.add_attribute_patcher.start()
        self.addCleanup(self.add_attribute_patcher.stop)

        # Patch deregister_asset
        self.deregister_patcher = patch('openfactory.apps.ofaapp.deregister_asset')
        self.mock_deregister = self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

        # Freeze datetime for deterministic AssetAttribute.timestamp
        self.fixed_ts = datetime(2023, 1, 1, 12, 0, 0)
        datetime_patcher = patch("openfactory.assets.utils.time_methods.datetime")
        self.mock_datetime = datetime_patcher.start()
        self.addCleanup(datetime_patcher.stop)

        # Make datetime.now() return fixed timestamp
        self.mock_datetime.now.return_value = self.fixed_ts
        # Allow datetime(...) constructor to still work
        self.mock_datetime.side_effect = lambda *a, **kw: datetime(*a, **kw)

    async def test_async_main_loop_not_implemented(self):
        """ Verify async_main_loop raises NotImplementedError by default. """
        app = OpenFactoryApp(app_uuid="test-uuid", ksqlClient=self.ksql_mock)
        with self.assertRaises(NotImplementedError):
            await app.async_main_loop()

    async def test_async_run_calls_welcome_and_adds_avail(self):
        """ Ensure async_run calls welcome_banner, adds 'avail' attribute, and runs async_main_loop. """
        app = OpenFactoryApp(app_uuid="test-uuid", ksqlClient=self.ksql_mock)

        # Mock async_main_loop
        app.async_main_loop = AsyncMock()

        # Patch welcome_banner
        with patch.object(app, "welcome_banner", return_value=None) as mock_banner:
            await app.async_run()

        # Check welcome banner called
        mock_banner.assert_called_once()

        # Check that add_attribute was called with an AssetAttribute
        self.mock_add_attribute.assert_any_call(
            AssetAttribute(
                id="avail",
                value="AVAILABLE",
                tag="Availability",
                type="Events"
            )
        )

        # Check async_main_loop executed
        app.async_main_loop.assert_awaited_once()

    async def test_async_run_handles_exception(self):
        """ Verify async_run handles exceptions from async_main_loop. """
        app = OpenFactoryApp(app_uuid="test-uuid", ksqlClient=self.ksql_mock)

        # Patch async_main_loop to raise an exception
        app.async_main_loop = AsyncMock(side_effect=Exception("Boom!"))

        # Patch welcome_banner to avoid side effects
        app.welcome_banner = lambda: None

        # Patch add_attribute
        app.add_attribute = lambda *a, **k: None

        # Patch logger to avoid actual logging
        app.logger = unittest.mock.Mock()

        # Patch deregister_asset and app_event_loop_stopped
        app.app_event_loop_stopped = unittest.mock.Mock()

        await app.async_run()

        # Check app_event_loop_stopped called
        app.app_event_loop_stopped.assert_called_once()

        # Check deregister_asset called with asset_uuid
        self.mock_deregister.assert_called_once_with(
            app.asset_uuid,
            ksqlClient=app.ksql,
            bootstrap_servers=app.bootstrap_servers
        )

        # Check logger.exception called
        app.logger.exception.assert_called()
