
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
import signal
from pathlib import Path
from tempfile import TemporaryDirectory

import streamlit as st

from openfactory.apps import OpenFactoryStreamlitApp


class _TestApp(OpenFactoryStreamlitApp):

    async def async_main_loop(self):
        await asyncio.sleep(0.01)


class TestOpenFactoryStreamlitApp(unittest.TestCase):
    """
    Tests for class OpenFactoryStreamlitApp
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

        # Patch get_nats_cluster_url
        self.get_nats_cluster_url_patcher = patch(
            "openfactory.assets.asset_base.get_nats_cluster_url",
            return_value="nats://mock"
        )
        self.mock_get_nats_cluster_url = self.get_nats_cluster_url_patcher.start()
        self.addCleanup(self.get_nats_cluster_url_patcher.stop)

        # Patch NATSSubscriber
        self.nats_subscriber_patcher = patch("openfactory.assets.asset_base.NATSSubscriber")
        self.mock_nats_subscriber = self.nats_subscriber_patcher.start()
        self.addCleanup(self.nats_subscriber_patcher.stop)

        # Prevent modification of the test runner's signal handlers
        self.signal_patcher = patch("openfactory.apps.ofaapp.signal.signal")
        self.signal_patcher.start()
        self.addCleanup(self.signal_patcher.stop)

        # Create temporary Streamlit script
        self.temp_dir = TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.script_path = Path(self.temp_dir.name) / "dashboard.py"
        self.script_path.write_text(
            'import streamlit as st\n'
            'st.title("Test OpenFactory Streamlit App")\n',
            encoding="utf-8"
        )

    def test_initialization_creates_streamlit(self):
        """ Test initialization creates Streamlit app. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertIsInstance(app.app, st.App)

    def test_script_path_accepts_string(self):
        """ Should accept a Streamlit script path as a string. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=str(self.script_path)
        )

        self.assertEqual(app.script_path, self.script_path.resolve())
        self.assertIsInstance(app.script_path, Path)

    def test_script_path_accepts_path(self):
        """ Should accept a Streamlit script path as a Path object. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertEqual(app.script_path, self.script_path.resolve())

    def test_script_path_resolved_to_absolute_path(self):
        """ Should resolve the Streamlit script path to an absolute path. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path.parent / ".." / self.script_path.parent.name / "dashboard.py"
        )

        self.assertTrue(app.script_path.is_absolute())
        self.assertEqual(app.script_path, self.script_path.resolve())

    def test_missing_script_raises(self):
        """ Should raise FileNotFoundError if the Streamlit script does not exist. """

        with self.assertRaises(FileNotFoundError):

            OpenFactoryStreamlitApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock",
                script_path=Path(self.temp_dir.name) / "missing.py"
            )

    def test_directory_script_path_raises(self):
        """ Should reject a directory as a Streamlit script path. """

        with self.assertRaises(FileNotFoundError):

            OpenFactoryStreamlitApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock",
                script_path=Path(self.temp_dir.name)
            )

    def test_missing_script_does_not_initialize_openfactory(self):
        """ Should validate the script path before initializing the OpenFactory asset. """

        with patch("openfactory.apps.ofaapp.OpenFactoryApp.__init__") as mock_init:

            with self.assertRaises(FileNotFoundError):

                OpenFactoryStreamlitApp(
                    ksqlClient=self.ksql_mock,
                    bootstrap_servers="mock",
                    asset_router_url="mock",
                    script_path=Path(self.temp_dir.name) / "missing.py"
                )

        mock_init.assert_not_called()

    def test_create_streamlit_app_called(self):
        """ Test initialization calls create_streamlit_app. """

        class App(OpenFactoryStreamlitApp):

            def create_streamlit_app(self):
                self.called = True
                return super().create_streamlit_app()

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertTrue(app.called)

    def test_create_streamlit_app_default(self):
        """ Default implementation should create a Streamlit app using the configured script. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        with patch("openfactory.apps.ofa_streamlit_app.st.App") as mock_app:

            streamlit_app = app.create_streamlit_app()

        mock_app.assert_called_once_with(self.script_path.resolve())
        self.assertIs(streamlit_app, mock_app.return_value)

    def test_create_streamlit_app_user_override(self):
        """ Should preserve a Streamlit application returned by a subclass factory. """

        custom_app = st.App(self.script_path)

        class App(OpenFactoryStreamlitApp):

            def create_streamlit_app(self):
                return custom_app

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertIs(app.app, custom_app)

    def test_log_http_requests_configuration(self):
        """ Should store HTTP request logging configuration. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertTrue(app.log_http_requests)

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False
        )

        self.assertFalse(app.log_http_requests)

    def test_initial_server_state(self):
        """ The Uvicorn server should not be started during initialization. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        self.assertIsNone(app._uvicorn_server)
        self.assertFalse(app._shutdown_requested)

    def test_signal_handler_before_server_initialization(self):
        """ Should record shutdown requests before the server is initialized. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        app.signal_handler(signal.SIGTERM, None)

        self.assertTrue(app._shutdown_requested)
        self.assertIsNone(app._uvicorn_server)

    def test_signal_handler_requests_server_shutdown(self):
        """ Should request graceful shutdown of the Uvicorn server. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        app._uvicorn_server = MagicMock()
        app._uvicorn_server.should_exit = False

        app.signal_handler(signal.SIGTERM, None)

        self.assertTrue(app._shutdown_requested)
        self.assertTrue(app._uvicorn_server.should_exit)

    def test_signal_handler_does_not_execute_cleanup(self):
        """ Signal handling should defer OpenFactory cleanup to async_run. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        app.shutdown = MagicMock()

        app.signal_handler(signal.SIGINT, None)

        app.shutdown.assert_not_called()
        self.assertTrue(app._shutdown_requested)

    def test_run_invokes_async_run(self):
        """ Test run invokes async_run. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        app.async_run = AsyncMock()

        with patch("asyncio.run") as mock_run:

            app.run()

            mock_run.assert_called_once()
            coro = mock_run.call_args[0][0]

            try:
                self.assertTrue(asyncio.iscoroutine(coro))
                app.async_run.assert_called_once_with()
            finally:
                coro.close()


class TestOpenFactoryStreamlitAppAsync(unittest.IsolatedAsyncioTestCase):
    """
    Async tests for class OpenFactoryStreamlitApp
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

        # Patch get_nats_cluster_url
        self.get_nats_cluster_url_patcher = patch(
            "openfactory.assets.asset_base.get_nats_cluster_url",
            return_value="nats://mock"
        )
        self.mock_get_nats_cluster_url = self.get_nats_cluster_url_patcher.start()
        self.addCleanup(self.get_nats_cluster_url_patcher.stop)

        # Patch NATSSubscriber
        self.nats_subscriber_patcher = patch("openfactory.assets.asset_base.NATSSubscriber")
        self.mock_nats_subscriber = self.nats_subscriber_patcher.start()
        self.addCleanup(self.nats_subscriber_patcher.stop)

        # Prevent modification of the test runner's signal handlers
        self.signal_patcher = patch("openfactory.apps.ofaapp.signal.signal")
        self.signal_patcher.start()
        self.addCleanup(self.signal_patcher.stop)

        # Create temporary Streamlit script
        self.temp_dir = TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.script_path = Path(self.temp_dir.name) / "dashboard.py"
        self.script_path.write_text(
            'import streamlit as st\n'
            'st.title("Test OpenFactory Streamlit App")\n',
            encoding="utf-8"
        )

    async def test_run_openfactory_calls_async_main_loop(self):
        """ Should call async_main_loop when user overrides it. """

        class App(OpenFactoryStreamlitApp):

            async def async_main_loop(self):
                self.called = True

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        await app._run_openfactory()

        self.assertTrue(app.called)

    async def test_run_openfactory_waits_when_no_async_main_loop(self):
        """ Should block when no custom async_main_loop is defined. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        task = asyncio.create_task(app._run_openfactory())

        await asyncio.sleep(0.01)

        self.assertFalse(task.done())
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_run_openfactory_rejects_sync_main_loop(self):
        """ Should raise if main_loop is defined. """

        class App(OpenFactoryStreamlitApp):

            def main_loop(self):
                pass

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        with self.assertRaisesRegex(RuntimeError, "does NOT support 'main_loop'"):
            await app._run_openfactory()

    async def test_default_async_main_loop_cancel(self):
        """ Ensure default async_main_loop is cancellable. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        task = asyncio.create_task(app.async_main_loop())

        await asyncio.sleep(0.01)

        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_default_port(self, mock_config, mock_server):
        """ Should use default port 4000 if PORT is not set. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        with patch.dict("os.environ", {}, clear=True):

            app = OpenFactoryStreamlitApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock",
                script_path=self.script_path
            )

            await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertEqual(kwargs["port"], 4000)

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_env_port(self, mock_config, mock_server):
        """ Should use PORT environment variable. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        with patch.dict("os.environ", {"PORT": "5555"}):

            app = OpenFactoryStreamlitApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers="mock",
                asset_router_url="mock",
                script_path=self.script_path
            )

            await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertEqual(kwargs["port"], 5555)

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_listens_on_all_interfaces(self, mock_config, mock_server):
        """ Should listen on all network interfaces. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertEqual(kwargs["host"], "0.0.0.0")

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_enables_lifespan(self, mock_config, mock_server):
        """ Should enable the ASGI lifespan protocol. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertEqual(kwargs["lifespan"], "on")

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_logs_http_requests_by_default(self, mock_config, mock_server):
        """ Should enable HTTP request logging by default. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertTrue(kwargs["access_log"])

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_disables_http_request_logging(self, mock_config, mock_server):
        """ Should disable HTTP request logging when requested. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False
        )

        await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertFalse(kwargs["access_log"])

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_uses_openfactory_log_level(self, mock_config, mock_server):
        """ Should align the Uvicorn log level with the OpenFactory logger. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            loglevel="DEBUG"
        )

        await app._run_streamlit()

        _, kwargs = mock_config.call_args

        self.assertEqual(kwargs["log_level"], "debug")

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_invokes_uvicorn(self, mock_config, mock_server):
        """ Should serve the configured Streamlit application using Uvicorn. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = True

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        await app._run_streamlit()

        self.assertIs(mock_config.call_args[0][0], app.app)

        mock_server.assert_called_once_with(mock_config.return_value)

        self.assertIs(app._uvicorn_server, mock_server.return_value)

        mock_server.return_value.serve.assert_awaited_once_with()

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_skips_execution_when_shutdown_requested(self, mock_config, mock_server):
        """ Should not start Uvicorn when application shutdown has been requested. """

        mock_server.return_value.serve = AsyncMock()

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        app.signal_handler(signal.SIGTERM, None)

        await app._run_streamlit()

        mock_server.return_value.serve.assert_not_awaited()

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_startup_failure(self, mock_config, mock_server):
        """ Should raise RuntimeError if Uvicorn fails to start. """

        mock_server.return_value.serve = AsyncMock()
        mock_server.return_value.started = False

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        with self.assertRaisesRegex(RuntimeError, "Streamlit server failed to start"):
            await app._run_streamlit()

    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_streamlit_app.uvicorn.Config")
    async def test_run_streamlit_converts_system_exit(self, mock_config, mock_server):
        """ Should convert Uvicorn SystemExit into RuntimeError. """

        mock_server.return_value.serve = AsyncMock(
            side_effect=SystemExit(1)
        )

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path
        )

        with self.assertRaisesRegex(RuntimeError, "Streamlit server failed") as context:
            await app._run_streamlit()

        self.assertIsInstance(context.exception.__cause__, SystemExit)

    async def test_async_run_invokes_run_streamlit(self):
        """ Test async_run invokes _run_streamlit. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._run_streamlit = AsyncMock()
        app.shutdown = MagicMock()

        await asyncio.wait_for(app.async_run(), timeout=2)

        app._run_streamlit.assert_awaited_once()

    async def test_async_run_invokes_run_openfactory(self):
        """ Test async_run invokes _run_openfactory. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._run_streamlit = AsyncMock()
        app._run_openfactory = AsyncMock()
        app.shutdown = MagicMock()

        await asyncio.wait_for(app.async_run(), timeout=2)

        app._run_openfactory.assert_awaited_once()

    async def test_async_run_server_failure_triggers_shutdown(self):
        """ Verify async_run invokes shutdown when Streamlit crashes. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._run_streamlit = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        app.shutdown = MagicMock()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await asyncio.wait_for(app.async_run(), timeout=2)

        app.shutdown.assert_called_once()

    async def test_async_run_openfactory_failure_triggers_shutdown(self):
        """ Verify async_run invokes shutdown when OpenFactory logic crashes. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._uvicorn_server = MagicMock()
        app._uvicorn_server.should_exit = False

        async def run_streamlit():
            while not app._uvicorn_server.should_exit:
                await asyncio.sleep(0.01)

        app._run_streamlit = run_streamlit

        app._run_openfactory = AsyncMock(
            side_effect=RuntimeError("boom")
        )

        app.shutdown = MagicMock()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await asyncio.wait_for(app.async_run(), timeout=2)

        self.assertTrue(app._uvicorn_server.should_exit)
        app.shutdown.assert_called_once()

    async def test_async_run_server_completion_cancels_openfactory(self):
        """ Normal Streamlit server termination should cancel OpenFactory logic. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        background_started = asyncio.Event()
        background_cancelled = asyncio.Event()

        async def run_openfactory():

            background_started.set()

            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                background_cancelled.set()
                raise

        async def run_streamlit():
            await background_started.wait()

        app._run_openfactory = run_openfactory
        app._run_streamlit = run_streamlit
        app.shutdown = MagicMock()

        await asyncio.wait_for(app.async_run(), timeout=2)

        self.assertTrue(background_cancelled.is_set())
        app.shutdown.assert_called_once()

    async def test_async_run_openfactory_completion_stops_server(self):
        """ Normal completion of OpenFactory logic should stop the Streamlit server. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._uvicorn_server = MagicMock()
        app._uvicorn_server.should_exit = False

        server_stopped = asyncio.Event()

        async def run_streamlit():

            while not app._uvicorn_server.should_exit:
                await asyncio.sleep(0.01)

            server_stopped.set()

        app._run_streamlit = run_streamlit
        app.shutdown = MagicMock()

        await asyncio.wait_for(app.async_run(), timeout=2)

        self.assertTrue(app._uvicorn_server.should_exit)
        self.assertTrue(server_stopped.is_set())

        app.shutdown.assert_called_once()

    async def test_async_run_shutdown_is_only_executed_once(self):
        """ Verify async_run does not execute cleanup twice. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._run_streamlit = AsyncMock()
        app.shutdown = MagicMock()

        await asyncio.wait_for(app.async_run(), timeout=2)

        app.shutdown.assert_called_once()

    async def test_async_run_shutdown_requested_before_start(self):
        """ Should execute cleanup without starting tasks if shutdown was requested. """

        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._run_streamlit = AsyncMock()
        app._run_openfactory = AsyncMock()
        app.shutdown = MagicMock()

        app.signal_handler(signal.SIGTERM, None)

        await asyncio.wait_for(app.async_run(), timeout=2)

        app._run_streamlit.assert_not_awaited()
        app._run_openfactory.assert_not_awaited()

        app.shutdown.assert_called_once()

    async def test_async_run_cancellation_triggers_shutdown(self):
        """ Should stop the server and execute cleanup when async_run is cancelled. """

        app = OpenFactoryStreamlitApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            test_mode=True
        )

        app._uvicorn_server = MagicMock()
        app._uvicorn_server.should_exit = False

        server_started = asyncio.Event()
        server_stopped = asyncio.Event()

        async def run_streamlit():

            server_started.set()

            while not app._uvicorn_server.should_exit:
                await asyncio.sleep(0.01)

            server_stopped.set()

        app._run_streamlit = run_streamlit
        app.shutdown = MagicMock()

        task = asyncio.create_task(app.async_run())

        try:
            await asyncio.wait_for(server_started.wait(), timeout=2)

            task.cancel()

            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)

        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        self.assertTrue(app._uvicorn_server.should_exit)
        self.assertTrue(server_stopped.is_set())

        app.shutdown.assert_called_once()
