import unittest
import threading
import time
import json

from urllib.request import urlopen, Request
from unittest.mock import patch, MagicMock
from flask import jsonify, request
from openfactory.apps import OpenFactoryFlaskApp
from prometheus_client import CollectorRegistry, Counter, CONTENT_TYPE_LATEST


class _HTTPTestApp(OpenFactoryFlaskApp):
    """
    Test OpenFactoryFlaskApp class with some endpoints
    """

    def configure_routes(self):

        @self.app.route("/")
        def root():

            return jsonify({
                "status": "ok"
            })

        @self.app.route("/move", methods=["POST"])
        def move():

            x = float(request.args["x"])
            y = float(request.args["y"])

            return jsonify({
                "x": x,
                "y": y
            })


class TestOpenFactoryFlaskAppHTTP(unittest.TestCase):
    """
    Test if Flask app in the OpenFactoryFlaskApp indeed talks with endpoints
    """

    def setUp(self):

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

        self.app = _HTTPTestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock"
        )

        self.client = self.app.app.test_client()

    def test_get_root(self):

        response = self.client.get("/")

        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.get_json(),
            {"status": "ok"}
        )

    def test_post_move(self):

        response = self.client.post(
            "/move?x=1.5&y=2.5"
        )

        self.assertEqual(response.status_code, 200)

        self.assertEqual(
            response.get_json(),
            {"x": 1.5, "y": 2.5}
        )

    def test_invalid_route(self):

        response = self.client.get("/unknown")

        self.assertEqual(response.status_code, 404)

    def test_forwarded_prefix_applies_to_redirects(self):
        """ ProxyFix should apply forwarded prefix to redirects. """

        @self.app.app.route("/redirect")
        def redirect_route():
            from flask import redirect, url_for
            return redirect(url_for("root"))

        response = self.client.get(
            "/redirect",
            headers={
                "X-Forwarded-Prefix": "/my-app"
            },
            follow_redirects=False
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/my-app/")

    def test_get_metrics(self):
        """ Should expose Prometheus metrics at the default endpoint. """

        registry = CollectorRegistry()

        counter = Counter(
            "test_requests_total",
            "Number of test requests",
            registry=registry
        )
        counter.inc()

        with patch.object(self.app, "register_prometheus_metrics"):
            self.app.expose_metrics(registry=registry)

        response = self.client.get("/metrics")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, CONTENT_TYPE_LATEST)
        self.assertIn(b"test_requests_total 1.0", response.data)

    def test_get_metrics_custom_endpoint(self):
        """ Should expose metrics at the configured endpoint. """

        registry = CollectorRegistry()

        with patch.dict("os.environ", {"METRICS_ENDPOINT": "/custom-metrics"}):

            with patch.object(self.app, "register_prometheus_metrics"):
                self.app.expose_metrics(registry=registry)

        response = self.client.get("/custom-metrics")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, CONTENT_TYPE_LATEST)

        # Default endpoint should not exist.
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 404)

    def test_metrics_not_exposed_by_default(self):
        """ Metrics endpoint should not exist unless explicitly enabled. """

        response = self.client.get("/metrics")

        self.assertEqual(response.status_code, 404)


class TestOpenFactoryFlaskAppConcurrency(unittest.TestCase):
    """
    Test concurrent HTTP request handling in OpenFactoryFlaskApp.
    """

    def setUp(self):

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

        self.app = _HTTPTestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            threaded=True,
            log_http_requests=False
        )

    def test_concurrent_requests_when_threaded(self):
        """ A long-running HTTP request should not block other requests when threaded=True. """

        request_started = threading.Event()
        release_request = threading.Event()
        request_finished = threading.Event()

        @self.app.app.route("/blocking")
        def blocking():
            request_started.set()
            # Keep the request active until explicitly released.
            release_request.wait(timeout=10)
            request_finished.set()
            return "done"

        # Capture exceptions from background threads.
        server_errors = []
        client_errors = []
        client_status = []

        def run_server():
            try:
                self.app._run_flask()
            except Exception as exc:
                server_errors.append(exc)

        def blocking_request():
            try:
                with urlopen(f"{base_url}/blocking", timeout=12) as response:
                    client_status.append(response.status)
            except Exception as exc:
                client_errors.append(exc)

        # Start the actual OpenFactory Flask server.
        server_thread = threading.Thread(target=run_server, daemon=True)

        client_thread = None

        try:
            # Port 0 lets the OS assign an available port.
            with patch.dict("os.environ", {"PORT": "0"}):

                server_thread.start()

                # Wait until Werkzeug has created the server.
                deadline = time.monotonic() + 5

                while (
                    self.app._server is None
                    and not server_errors
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)

                self.assertFalse(server_errors, f"Flask server failed to start: {server_errors}")
                self.assertIsNotNone(self.app._server, "Flask server did not start")

                port = self.app._server.server_port

            base_url = f"http://127.0.0.1:{port}"

            # Start the long-running request.
            client_thread = threading.Thread(target=blocking_request, daemon=True)
            client_thread.start()

            self.assertTrue(request_started.wait(timeout=3), "Blocking request did not start")

            # Send another request while the first is still blocked.
            req = Request(f"{base_url}/move?x=1.5&y=2.5", method="POST")

            with urlopen(req, timeout=3) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()), {"x": 1.5, "y": 2.5})

            # Verify the first request is still active.
            self.assertFalse(request_finished.is_set(), "Blocking request finished before the second request")

        finally:

            # Release the long-running request.
            release_request.set()

            if client_thread is not None:
                client_thread.join(timeout=5)

            # Shut down the actual OpenFactory Flask server.
            if self.app._server is not None:
                self.app._server.shutdown()
                self.app._server.server_close()

            server_thread.join(timeout=5)

        # Verify the blocking request also completed successfully.
        self.assertFalse(server_errors)
        self.assertFalse(client_errors)
        self.assertEqual(client_status, [200])
        self.assertTrue(request_finished.is_set(), "Blocking request did not finish")
