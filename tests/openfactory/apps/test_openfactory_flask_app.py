import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from flask import Flask
from openfactory.apps.ofa_flask_app import QuietWSGIRequestHandler

from openfactory.apps import OpenFactoryFlaskApp


class _TestApp(OpenFactoryFlaskApp):

    async def async_main_loop(self):
        await asyncio.sleep(0.01)


class TestQuietWSGIRequestHandler(unittest.TestCase):
    """
    Tests for class QuietWSGIRequestHandler
    """

    @patch("werkzeug.serving.WSGIRequestHandler.log_request")
    def test_log_request_is_suppressed(self, mock_log_request):
        """ QuietWSGIRequestHandler should suppress HTTP request logging. """

        handler = QuietWSGIRequestHandler.__new__(QuietWSGIRequestHandler)

        handler.log_request()

        mock_log_request.assert_not_called()


class TestOpenFactoryFlaskApp(unittest.TestCase):
    """
    Tests for class OpenFactoryFlaskApp
    """

    def setUp(self):

        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.wait_until_patcher = patch("openfactory.assets.asset_base.BaseAsset.wait_until", return_value=True)
        self.wait_until_patcher.start()
        self.addCleanup(self.wait_until_patcher.stop)

        self.deregister_patcher = patch("openfactory.apps.ofaapp.deregister_asset")
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

    def test_initialization_creates_flask(self):
        """ Test initialization creates Flask app """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        self.assertIsInstance(app.app, Flask)

    def test_application_root_not_set_when_env_missing(self):
        """ Should not inject APPLICATION_ROOT if env var is absent """

        with patch.dict("os.environ", {}, clear=True):

            app = OpenFactoryFlaskApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock"
            )

            self.assertEqual(app.app.config["APPLICATION_ROOT"], "/")

    def test_application_root_loaded_from_env(self):
        """ Should configure APPLICATION_ROOT from env var """

        with patch.dict(
            "os.environ",
            {"OPENFACTORY_ROOT_PATH": "/my-app"},
            clear=True
        ):

            app = OpenFactoryFlaskApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock"
            )

            self.assertEqual(
                app.app.config["APPLICATION_ROOT"],
                "/my-app"
            )

    def test_application_root_user_override_preserved(self):
        """ User-defined APPLICATION_ROOT should not be overwritten """

        class App(OpenFactoryFlaskApp):

            def create_flask_app(self):
                app = Flask(__name__)
                app.config["APPLICATION_ROOT"] = "/user-defined"
                return app

        with patch.dict(
            "os.environ",
            {"OPENFACTORY_ROOT_PATH": "/framework-defined"},
            clear=True
        ):

            app = App(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock"
            )

            self.assertEqual(app.app.config["APPLICATION_ROOT"], "/user-defined")

    def test_create_flask_app_called(self):
        """ Test initialization calls create_flask_app """

        class App(OpenFactoryFlaskApp):

            def create_flask_app(self):
                self.called = True
                return Flask(__name__)

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        self.assertTrue(hasattr(app, "called"))

    def test_proxyfix_enabled(self):
        """ Flask app should enable ProxyFix middleware """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        from werkzeug.middleware.proxy_fix import ProxyFix
        self.assertIsInstance(app.app.wsgi_app, ProxyFix)

    def test_proxyfix_configuration(self):
        """ Flask app should configure ProxyFix correctly """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        from werkzeug.middleware.proxy_fix import ProxyFix
        self.assertIsInstance(app.app.wsgi_app, ProxyFix)
        self.assertEqual(app.app.wsgi_app.x_prefix, 1)
        self.assertEqual(app.app.wsgi_app.x_host, 1)
        self.assertEqual(app.app.wsgi_app.x_proto, 1)

    def test_configure_routes_called(self):
        """ Test initialization calls configure_routes """

        class App(OpenFactoryFlaskApp):

            def configure_routes(self):
                self.called = True

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        self.assertTrue(hasattr(app, "called"))

    def test_flask_app_exposes_ofa_app(self):
        """ Flask app should expose OpenFactory app """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        self.assertIs(app.app.ofa_app, app)

    def test_create_flask_app_default(self):
        """ Default implementation should create a Flask app using the module name. """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        flask_app = app.create_flask_app()

        self.assertIsInstance(flask_app, Flask)
        self.assertEqual(
            flask_app.import_name,
            OpenFactoryFlaskApp.__module__,
        )

    def test_run_invokes_async_run(self):
        """ Test run invokes async_run """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        app.async_run = AsyncMock()

        with patch("asyncio.run") as mock_run:

            app.run()
            mock_run.assert_called_once()
            coro = mock_run.call_args[0][0]

            self.assertTrue(asyncio.iscoroutine(coro))


class TestOpenFactoryFlaskAppAsync(unittest.IsolatedAsyncioTestCase):
    """
    Async tests for class OpenFactoryFlaskApp
    """

    def setUp(self):

        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.wait_until_patcher = patch("openfactory.assets.asset_base.BaseAsset.wait_until", return_value=True)
        self.wait_until_patcher.start()
        self.addCleanup(self.wait_until_patcher.stop)

        self.deregister_patcher = patch("openfactory.apps.ofaapp.deregister_asset")
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

    async def test_run_openfactory_calls_async_main_loop(self):
        """ Should call async_main_loop when user overrides it """

        class App(OpenFactoryFlaskApp):

            async def async_main_loop(self):
                self.called = True

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        await app._run_openfactory()

        self.assertTrue(hasattr(app, "called"))

    async def test_run_openfactory_waits_when_no_async_main_loop(self):
        """ Should block when no async_main_loop defined """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        task = asyncio.create_task(app._run_openfactory())

        await asyncio.sleep(0.01)

        self.assertFalse(task.done())
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_run_openfactory_rejects_sync_main_loop(self):
        """ Should raise if main_loop is defined """

        class App(OpenFactoryFlaskApp):

            def main_loop(self):
                pass

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        with self.assertRaises(RuntimeError):
            await app._run_openfactory()

    async def test_async_run_invokes_run_flask(self):
        """ Test async_run invokes _run_flask """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            test_mode=True
        )

        app._run_flask = MagicMock()

        with patch(
            "threading.Thread.start",
            side_effect=lambda: app._run_flask()
        ):
            await app.async_run()

        app._run_flask.assert_called_once()

    async def test_async_run_failure_triggers_shutdown(self):
        """ Verify async_run invokes shutdown when Flask thread crashes. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            test_mode=True
        )

        def failing_flask():
            raise RuntimeError("boom")

        app._run_flask = failing_flask
        app.shutdown = MagicMock()

        await app.async_run()

        app.shutdown.assert_called_once()

    async def test_default_async_main_loop_cancel(self):
        """ Ensure default async_main_loop is cancellable """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        task = asyncio.create_task(app.async_main_loop())
        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    @patch("openfactory.apps.ofa_flask_app.make_server")
    async def test_run_flask_default_port(self, mock_make_server):
        """ Should use default port 4000 if PORT not set """

        mock_server = MagicMock()
        mock_make_server.return_value = mock_server

        with patch.dict("os.environ", {}, clear=True):

            app = OpenFactoryFlaskApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock"
            )

            await asyncio.to_thread(app._run_flask)
            _, kwargs = mock_make_server.call_args
            self.assertEqual(kwargs["port"], 4000)
            self.assertIsNone(kwargs["request_handler"])

    @patch("openfactory.apps.ofa_flask_app.make_server")
    async def test_run_flask_env_port(self, mock_make_server):
        """ Should use PORT environment variable """

        mock_server = MagicMock()
        mock_make_server.return_value = mock_server

        with patch.dict("os.environ", {"PORT": "5555"}):

            app = OpenFactoryFlaskApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock"
            )

            await asyncio.to_thread(app._run_flask)
            _, kwargs = mock_make_server.call_args
            self.assertEqual(kwargs["port"], 5555)
            self.assertIsNone(kwargs["request_handler"])

    async def test_thread_wrapper_captures_exception(self):
        """ Thread wrapper should propagate exceptions """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        def failing():
            raise RuntimeError("boom")

        with patch.object(app.logger, "exception"):
            app._thread_wrapper(failing)

        self.assertIsInstance(app._thread_exception, RuntimeError)

    async def test_async_run_shutdown_is_only_executed_once(self):
        """ Verify async_run does not execute cleanup twice. """
        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            test_mode=True
        )

        app.shutdown = MagicMock()

        def failing_flask():
            raise RuntimeError("boom")

        app._run_flask = failing_flask

        await app.async_run()

        app.shutdown.assert_called_once()

    @patch("openfactory.apps.ofa_flask_app.make_server")
    async def test_run_flask_logs_http_requests_by_default(self, mock_make_server):
        """ Should enable HTTP request logging by default. """

        mock_make_server.return_value = MagicMock()

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
        )

        await asyncio.to_thread(app._run_flask)

        _, kwargs = mock_make_server.call_args
        self.assertIsNone(kwargs["request_handler"])

    @patch("openfactory.apps.ofa_flask_app.make_server")
    async def test_run_flask_disables_http_request_logging(self, mock_make_server):
        """ Should disable HTTP request logging when requested. """

        from openfactory.apps.ofa_flask_app import QuietWSGIRequestHandler

        mock_make_server.return_value = MagicMock()

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            log_http_requests=False,
        )

        await asyncio.to_thread(app._run_flask)

        _, kwargs = mock_make_server.call_args
        self.assertIs(kwargs["request_handler"], QuietWSGIRequestHandler)

    def test_log_http_requests_configuration(self):
        """ Should store HTTP request logging configuration. """

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
        )
        self.assertTrue(app.log_http_requests)

        app = OpenFactoryFlaskApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            log_http_requests=False,
        )
        self.assertFalse(app.log_http_requests)
