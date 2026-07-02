import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from openfactory.apps import OpenFactoryFastAPIApp


class _HTTPTestApp(OpenFactoryFastAPIApp):
    """
    Test OpenFactoryFastAPIApp class with some endpoints
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # expose OFA app for router-style access if needed
        self.api.state.ofa_app = self

        # expose prometheus metrics
        self.expose_metrics()

        @self.api.get("/")
        async def root():
            return {"status": "ok"}

        @self.api.post("/move")
        async def move(x: float, y: float):
            return {"x": x, "y": y}


class TestOpenFactoryFastAPIAppHTTP(unittest.TestCase):
    """
    Test if FastAPI app in the OpenFactoryFastAPIApp indeed talks with the endpoints
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        self.deregister_patcher = patch("openfactory.apps.ofaapp.deregister_asset")
        self.deregister_patcher.start()
        self.addCleanup(self.deregister_patcher.stop)

        self.register_metrics_patcher = patch.object(OpenFactoryFastAPIApp, "register_prometheus_metrics")
        self.mock_register_metrics = self.register_metrics_patcher.start()
        self.addCleanup(self.register_metrics_patcher.stop)

        self.app = _HTTPTestApp(
            ksqlClient=self.ksql_mock,
            bootstrap_servers="mock",
            asset_router_url="mock",
            test_mode=True
        )

        self.client = TestClient(self.app.api)

    def test_get_root(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_post_move(self):
        response = self.client.post("/move", params={"x": 1.5, "y": 2.5})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"x": 1.5, "y": 2.5})

    def test_invalid_route(self):
        response = self.client.get("/unknown")
        self.assertEqual(response.status_code, 404)

    def test_metrics_endpoint(self):
        """ Should expose Prometheus metrics. """
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["content-type"])
