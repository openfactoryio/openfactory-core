import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from flask import Flask

from openfactory.apps import OpenFactoryFlaskApp


class _TestApp(OpenFactoryFlaskApp):

    async def async_main_loop(self):
        await asyncio.sleep(0.01)


class TestOpenFactoryFlaskApp(unittest.TestCase):
    """
    Tests for class OpenFactoryFlaskApp
    """

    def setUp(self):

        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch(
            "openfactory.assets.asset_base.AssetProducer"
        )
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch(
            "openfactory.apps.ofaapp.deregister_asset"
        )
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

        self.asset_producer_patcher = patch(
            "openfactory.assets.asset_base.AssetProducer"
        )
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch(
            "openfactory.apps.ofaapp.deregister_asset"
        )
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

    async def test_async_run_failure_triggers_cleanup(self):
        """ Test async_run failure triggers cleanup """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            test_mode=True
        )

        def failing_flask():
            raise RuntimeError("boom")

        app._run_flask = failing_flask
        app.app_event_loop_stopped = MagicMock()
        await app.async_run()

        app.app_event_loop_stopped.assert_called_once()

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
