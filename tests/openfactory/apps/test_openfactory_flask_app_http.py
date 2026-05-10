import unittest
from unittest.mock import patch, MagicMock
from flask import jsonify, request
from openfactory.apps import OpenFactoryFlaskApp


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
