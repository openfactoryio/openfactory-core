
import unittest
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import urlopen
from unittest.mock import patch, MagicMock

import streamlit as st
from streamlit.runtime import Runtime
from starlette.testclient import TestClient
from starlette.routing import Route
from starlette.responses import JSONResponse
from openfactory.apps import OpenFactoryStreamlitApp


class _HTTPTestApp(OpenFactoryStreamlitApp):
    """
    Test OpenFactoryStreamlitApp class with additional HTTP endpoints.
    """

    def create_streamlit_app(self):

        async def status(request):

            return JSONResponse({
                "status": "ok"
            })

        async def move(request):

            x = float(request.query_params["x"])
            y = float(request.query_params["y"])

            return JSONResponse({
                "x": x,
                "y": y
            })

        async def asset(request):

            return JSONResponse({
                "asset_uuid": self.asset_uuid
            })

        return st.App(
            script_path=self.script_path,
            routes=[
                Route("/status", status, methods=["GET"]),
                Route("/move", move, methods=["POST"]),
                Route("/asset", asset, methods=["GET"]),
            ]
        )


class TestOpenFactoryStreamlitAppHTTP(unittest.TestCase):
    """
    Test HTTP and WebSocket endpoints of OpenFactoryStreamlitApp.
    """

    def setUp(self):

        # Isolate the Streamlit runtime singleton
        self.runtime_patcher = patch.object(Runtime, "_instance", None)
        self.runtime_patcher.start()
        self.addCleanup(self.runtime_patcher.stop)

        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch("openfactory.apps.ofaapp.deregister_asset")
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

        self.wait_until_patcher = patch("openfactory.assets.asset_base.BaseAsset.wait_until", return_value=True)
        self.wait_until_patcher.start()
        self.addCleanup(self.wait_until_patcher.stop)

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

        self.app = _HTTPTestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False
        )

        # Start and stop the Streamlit ASGI lifespan.
        self.client = self.enterContext(TestClient(self.app.app))

    def test_get_root(self):
        """ Should serve the Streamlit frontend. """

        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn('id="root"', response.text)

    def test_get_health(self):
        """ Should expose the native Streamlit health endpoint. """

        response = self.client.get("/_stcore/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.strip(), "ok")

    def test_get_status(self):
        """ Should serve custom HTTP endpoints. """

        response = self.client.get("/status")

        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.json(),
            {"status": "ok"}
        )

    def test_post_move(self):
        """ Should handle POST requests on custom HTTP endpoints. """

        response = self.client.post(
            "/move?x=1.5&y=2.5"
        )

        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.json(),
            {"x": 1.5, "y": 2.5}
        )

    def test_get_asset(self):
        """ Custom routes should be able to access the OpenFactory application. """

        response = self.client.get("/asset")

        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.json(),
            {"asset_uuid": self.app.asset_uuid}
        )

    def test_invalid_http_method(self):
        """ Should reject unsupported HTTP methods on custom endpoints. """

        response = self.client.put("/move")

        self.assertEqual(response.status_code, 405)

    def test_get_request_falls_back_to_streamlit_frontend(self):
        """ GET requests not handled by custom routes should fall back to the Streamlit frontend. """

        response = self.client.get("/move")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])

    def test_websocket_connection(self):
        """ Should accept a Streamlit WebSocket connection. """

        with self.client.websocket_connect(
            "/_stcore/stream",
            subprotocols=["streamlit"]
        ) as websocket:

            self.assertEqual(
                websocket.accepted_subprotocol,
                "streamlit"
            )

    def test_health_after_websocket_disconnect(self):
        """ Closing a browser session should not stop the Streamlit runtime. """

        with self.client.websocket_connect(
            "/_stcore/stream",
            subprotocols=["streamlit"]
        ):
            pass

        response = self.client.get("/_stcore/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.strip(), "ok")

    def test_multiple_websocket_connections(self):
        """ Should support multiple simultaneous Streamlit connections. """

        with self.client.websocket_connect(
            "/_stcore/stream",
            subprotocols=["streamlit"]
        ) as websocket1:

            with self.client.websocket_connect(
                "/_stcore/stream",
                subprotocols=["streamlit"]
            ) as websocket2:

                self.assertEqual(
                    websocket1.accepted_subprotocol,
                    "streamlit"
                )

                self.assertEqual(
                    websocket2.accepted_subprotocol,
                    "streamlit"
                )

                response = self.client.get("/_stcore/health")

                self.assertEqual(response.status_code, 200)


class TestOpenFactoryStreamlitAppConcurrency(unittest.IsolatedAsyncioTestCase):
    """
    Test concurrent HTTP request handling in OpenFactoryStreamlitApp
    using the actual embedded Uvicorn server.
    """

    def setUp(self):

        # Isolate the Streamlit runtime singleton
        self.runtime_patcher = patch.object(Runtime, "_instance", None)
        self.runtime_patcher.start()
        self.addCleanup(self.runtime_patcher.stop)

        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch("openfactory.apps.ofaapp.deregister_asset")
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

        self.wait_until_patcher = patch("openfactory.assets.asset_base.BaseAsset.wait_until", return_value=True)
        self.wait_until_patcher.start()
        self.addCleanup(self.wait_until_patcher.stop)

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

    async def _wait_for_server(self, app, server_task):
        """ Wait until Uvicorn is ready and return its assigned port. """

        async def wait_for_startup():

            while True:

                if server_task.done():
                    await server_task
                    raise RuntimeError("Streamlit server stopped before startup")

                if (
                    app._uvicorn_server is not None
                    and app._uvicorn_server.started
                ):
                    return app._uvicorn_server.servers[0].sockets[0].getsockname()[1]

                await asyncio.sleep(0.01)

        return await asyncio.wait_for(
            wait_for_startup(),
            timeout=10
        )

    async def _stop_server(self, app, server_task):
        """ Request graceful shutdown of the embedded server. """

        if app._uvicorn_server is not None:
            app._uvicorn_server.should_exit = True
        elif not server_task.done():
            server_task.cancel()

        await asyncio.wait_for(
            server_task,
            timeout=10
        )

    @staticmethod
    def _get(url):
        """ Perform a blocking HTTP GET request. """

        with urlopen(url, timeout=5) as response:
            return response.status, response.read()

    async def test_real_http_server(self):
        """ Should serve HTTP requests through the actual embedded Uvicorn server. """

        app = _HTTPTestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False
        )

        # Port 0 lets the OS assign an available port.
        with patch.dict("os.environ", {"PORT": "0"}):

            server_task = asyncio.create_task(
                app._run_streamlit()
            )

            try:
                port = await self._wait_for_server(app, server_task)

                base_url = f"http://127.0.0.1:{port}"

                status, data = await asyncio.to_thread(
                    self._get,
                    f"{base_url}/_stcore/health"
                )

                self.assertEqual(status, 200)
                self.assertEqual(data.strip(), b"ok")

                status, data = await asyncio.to_thread(
                    self._get,
                    f"{base_url}/status"
                )

                self.assertEqual(status, 200)
                self.assertEqual(json.loads(data), {"status": "ok"})

            finally:
                await self._stop_server(app, server_task)

        self.assertTrue(server_task.done())

    async def test_concurrent_http_requests(self):
        """ A long-running asynchronous request should not block other requests. """

        request_started = asyncio.Event()
        release_request = asyncio.Event()
        request_finished = asyncio.Event()

        class App(OpenFactoryStreamlitApp):

            def create_streamlit_app(self):

                async def blocking(request):

                    request_started.set()

                    await asyncio.wait_for(
                        release_request.wait(),
                        timeout=10
                    )

                    request_finished.set()

                    return JSONResponse({
                        "status": "done"
                    })

                async def status(request):

                    return JSONResponse({
                        "status": "ok"
                    })

                return st.App(
                    script_path=self.script_path,
                    routes=[
                        Route("/blocking", blocking),
                        Route("/status", status),
                    ]
                )

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False
        )

        blocking_task = None

        with patch.dict("os.environ", {"PORT": "0"}):

            server_task = asyncio.create_task(
                app._run_streamlit()
            )

            try:
                port = await self._wait_for_server(app, server_task)

                base_url = f"http://127.0.0.1:{port}"

                # Start a long-running request.
                blocking_task = asyncio.create_task(
                    asyncio.to_thread(
                        self._get,
                        f"{base_url}/blocking"
                    )
                )

                await asyncio.wait_for(
                    request_started.wait(),
                    timeout=3
                )

                # Send another request while the first is still blocked.
                status, data = await asyncio.to_thread(
                    self._get,
                    f"{base_url}/status"
                )

                self.assertEqual(status, 200)
                self.assertEqual(json.loads(data), {"status": "ok"})

                # Verify the first request is still active.
                self.assertFalse(request_finished.is_set())
                self.assertFalse(blocking_task.done())

                # Release the first request.
                release_request.set()

                status, data = await asyncio.wait_for(
                    blocking_task,
                    timeout=5
                )

                self.assertEqual(status, 200)
                self.assertEqual(json.loads(data), {"status": "done"})

                self.assertTrue(request_finished.is_set())

            finally:
                release_request.set()

                try:
                    if blocking_task is not None:
                        await asyncio.gather(
                            blocking_task,
                            return_exceptions=True
                        )
                finally:
                    await self._stop_server(app, server_task)

    async def test_http_server_and_openfactory_background_logic(self):
        """ The OpenFactory background logic should run alongside the HTTP server. """

        background_started = asyncio.Event()
        background_updated = asyncio.Event()
        background_cancelled = asyncio.Event()
        request_received = asyncio.Event()

        class App(OpenFactoryStreamlitApp):

            def create_streamlit_app(self):

                async def status(request):

                    request_received.set()

                    await asyncio.wait_for(
                        background_updated.wait(),
                        timeout=3
                    )

                    return JSONResponse({
                        "counter": self.counter
                    })

                return st.App(
                    script_path=self.script_path,
                    routes=[
                        Route("/status", status),
                    ]
                )

            async def async_main_loop(self):

                self.counter = 0

                background_started.set()

                try:
                    await request_received.wait()

                    self.counter += 1
                    background_updated.set()

                    await asyncio.Event().wait()

                except asyncio.CancelledError:
                    background_cancelled.set()
                    raise

        app = App(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            script_path=self.script_path,
            log_http_requests=False,
            test_mode=True
        )

        app.shutdown = MagicMock()

        with patch.dict("os.environ", {"PORT": "0"}):

            app_task = asyncio.create_task(
                app.async_run()
            )

            try:
                port = await self._wait_for_server(app, app_task)

                await asyncio.wait_for(
                    background_started.wait(),
                    timeout=3
                )

                # Background logic should be running before any HTTP request.
                self.assertEqual(app.counter, 0)
                self.assertFalse(app_task.done())

                status, data = await asyncio.to_thread(
                    self._get,
                    f"http://127.0.0.1:{port}/status"
                )

                self.assertEqual(status, 200)
                self.assertEqual(json.loads(data), {"counter": 1})

                self.assertTrue(background_updated.is_set())
                self.assertFalse(app_task.done())

            finally:
                await self._stop_server(app, app_task)

        self.assertTrue(background_cancelled.is_set())

        app.shutdown.assert_called_once()
