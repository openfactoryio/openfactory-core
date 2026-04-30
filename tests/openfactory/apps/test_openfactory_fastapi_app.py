import unittest
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from fastapi import FastAPI
from openfactory.apps import OpenFactoryFastAPIApp


class _TestApp(OpenFactoryFastAPIApp):
    async def async_main_loop(self):
        await asyncio.sleep(0.01)


class TestOpenFactoryFastAPIApp(unittest.TestCase):
    """
    Tests for class OpenFactoryFastAPIApp
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch('openfactory.apps.ofaapp.deregister_asset')
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

    def test_initialization_creates_fastapi(self):
        """" Test initialization creates FastAPI app """
        app = OpenFactoryFastAPIApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        self.assertIsInstance(app.api, FastAPI)

    def test_configure_routes_called(self):
        """ Test initialization calls configure_routes """
        class App(OpenFactoryFastAPIApp):
            def configure_routes(self):
                self.called = True

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        self.assertTrue(hasattr(app, "called"))


class TestOpenFactoryFastAPIAppAsync(unittest.IsolatedAsyncioTestCase):
    """
    Tests for class OpenFactoryFastAPIApp
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch('openfactory.apps.ofaapp.deregister_asset')
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

    async def test_run_openfactory_calls_async_main_loop(self):
        """ Should call async_main_loop when user overrides it """

        class App(OpenFactoryFastAPIApp):
            async def async_main_loop(self):
                self.called = True

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        await app._run_openfactory()

        self.assertTrue(hasattr(app, "called"))

    async def test_run_openfactory_waits_when_no_async_main_loop(self):
        """ Should block when no async_main_loop defined """

        app = OpenFactoryFastAPIApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        task = asyncio.create_task(app._run_openfactory())

        await asyncio.sleep(0.01)
        self.assertFalse(task.done())

        # cancel instead of relying on event
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_run_openfactory_rejects_sync_main_loop(self):
        """ Should raise if main_loop is defined """

        class App(OpenFactoryFastAPIApp):
            def main_loop(self):
                pass

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        with self.assertRaises(RuntimeError):
            await app._run_openfactory()

    def test_run_invokes_async_run(self):
        """ Test run invokes async_run """
        app = OpenFactoryFastAPIApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        app.async_run = AsyncMock()

        with patch("asyncio.run") as mock_run:
            app.run()

            mock_run.assert_called_once()
            # check that asyncio.run was called with the coroutine from async_run
            coro = mock_run.call_args[0][0]
            self.assertTrue(asyncio.iscoroutine(coro))

    async def test_async_run_invokes_run_fastapi(self):
        """ Test async_run invokes _run_fastapi """
        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock',
            test_mode=True
        )

        app._run_fastapi = AsyncMock()

        await app.async_run()

        app._run_fastapi.assert_awaited_once()

    async def test_async_run_failure_triggers_cleanup(self):
        """ Test async_run failure triggers cleanup """
        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock',
            test_mode=True
        )

        async def failing_fastapi():
            raise RuntimeError("boom")

        app._run_fastapi = failing_fastapi
        app.app_event_loop_stopped = MagicMock()

        await app.async_run()

        app.app_event_loop_stopped.assert_called_once()

    async def test_uvicorn_shutdown_flag(self):
        """ Test uvicorn shutdown flag """
        app = _TestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        class FakeServer:
            def __init__(self):
                self.should_exit = False

        app._uvicorn_server = FakeServer()
        app._run_fastapi = AsyncMock()

        await app.async_run()

        self.assertTrue(app._uvicorn_server.should_exit)

    async def test_default_async_main_loop_cancel(self):
        """ Ensure default async_main_loop is cancellable """
        app = OpenFactoryFastAPIApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock',
            asset_router_url='mock'
        )

        task = asyncio.create_task(app.async_main_loop())

        await asyncio.sleep(0.01)
        task.cancel()

        with self.assertRaises(asyncio.CancelledError):
            await task

    @patch("openfactory.apps.ofa_fastapi_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_fastapi_app.uvicorn.Config")
    async def test_run_fastapi_default_port(self, mock_config, mock_server):
        """ Should use default port 4000 if PORT not set """

        with patch.dict("os.environ", {}, clear=True):
            app = OpenFactoryFastAPIApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers='mock',
                asset_router_url='mock'
            )

            # avoid real serve loop
            mock_server.return_value.serve = AsyncMock()

            await app._run_fastapi()

            _, kwargs = mock_config.call_args
            self.assertEqual(kwargs["port"], 4000)

    @patch("openfactory.apps.ofa_fastapi_app.uvicorn.Server")
    @patch("openfactory.apps.ofa_fastapi_app.uvicorn.Config")
    async def test_run_fastapi_env_port(self, mock_config, mock_server):
        """ Should use PORT environment variable """

        with patch.dict("os.environ", {"PORT": "5555"}):
            app = OpenFactoryFastAPIApp(
                ksqlClient=self.ksql_mock,
                bootstrap_servers='mock',
                asset_router_url='mock'
            )

            mock_server.return_value.serve = AsyncMock()

            await app._run_fastapi()

            _, kwargs = mock_config.call_args
            self.assertEqual(kwargs["port"], 5555)
