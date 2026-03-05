import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from uuid import uuid4
from openfactory.apps import OpenFactoryApp, ofa_method
from openfactory.schemas.command_header import CommandEnvelope, CommandHeader

# Mocked CommandHeader
header = CommandHeader(
    correlation_id=uuid4(),
    sender_uuid="TEST-UUID",
    timestamp=datetime.now(timezone.utc),
    signature=None
)


class TestOpenFactoryAppMethods(unittest.TestCase):
    """ Tests for decorated OpenFactory methods and CMD subscription """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch subscribe_to_attribute to spy calls
        self.subscribe_patch = patch.object(OpenFactoryApp, 'subscribe_to_attribute')
        self.mock_subscribe = self.subscribe_patch.start()
        self.addCleanup(self.subscribe_patch.stop)

        # Patch logger
        self.logger_patch = patch('openfactory.apps.ofaapp.configure_prefixed_logger', return_value=MagicMock())
        self.mock_logger = self.logger_patch.start()
        self.addCleanup(self.logger_patch.stop)

        # Patch AssetProducer so its send_asset_attribute is a no-op
        producer_mock = MagicMock()
        producer_mock.topic = 'mock_topic'  # must be str
        producer_mock.send_asset_attribute = MagicMock()
        self.asset_producer_patch = patch('openfactory.assets.asset_base.AssetProducer', return_value=producer_mock)
        self.mock_producer_cls = self.asset_producer_patch.start()
        self.addCleanup(self.asset_producer_patch.stop)

    def test_ofa_method_decorator_metadata(self):
        """ Verify that @ofa_method stores correct metadata """
        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(self, x: float, y: float, speed: int = 100):
                return x, y, speed

        # Inspect the class attribute directly
        method = MyApp.__dict__['move_axis']

        self.assertTrue(hasattr(method, "_ofa_method_metadata"))
        meta = method._ofa_method_metadata
        self.assertEqual(meta["method_name"], "move_axis")
        self.assertIn("x", meta["parameters"])
        self.assertIn("y", meta["parameters"])
        self.assertIn("speed", meta["parameters"])
        self.assertEqual(meta["parameters"]["speed"]["default"], 100)

    def test_subscribe_ofa_methods_called(self):
        """ Verify _subscribe_ofa_methods subscribes CMD attributes """
        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(self, x: float, y: float):
                return x + y

        MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
              asset_router_url='mocked_asset_url', )

        # Check subscribe_to_attribute called with correct CMD
        self.mock_subscribe.assert_any_call('move_axis_CMD', unittest.mock.ANY)

    def test_execute_ofa_method_with_envelope(self):
        """ Verify that _execute_ofa_method calls the method with correct args """
        class MyApp(OpenFactoryApp):
            # spy variable for simpler test logic
            called_args = None

            @ofa_method()
            def move_axis(self, x: float, y: float, speed: int = 100):
                self.called_args = (x, y, speed)
                return x + y + speed

        app = MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
                    asset_router_url='mocked_asset_url')

        # Mock envelope
        envelope = CommandEnvelope(
            header=header,
            arguments={
                "x": "1.5",
                "y": "2.5",
                "speed": "200"
            }
        )
        result = app._execute_ofa_method(app.move_axis, envelope)

        # Method executed with correct types
        self.assertEqual(app.called_args, (1.5, 2.5, 200))
        self.assertEqual(result, 1.5 + 2.5 + 200)

    def test_execute_ofa_method_uses_default_for_optional(self):
        """ Verify default value is used if optional param missing """
        class MyApp(OpenFactoryApp):
            called_args = None

            @ofa_method()
            def move_axis(self, x: float, y: float, speed: int = 50):
                self.called_args = (x, y, speed)
                return speed

        app = MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
                    asset_router_url='mocked_asset_url')

        # Envelope missing optional speed
        envelope = CommandEnvelope(
            header=header,
            arguments={
                "x": "3",
                "y": "4"
            }
        )

        result = app._execute_ofa_method(app.move_axis, envelope)

        self.assertEqual(app.called_args, (3.0, 4.0, 50))
        self.assertEqual(result, 50)

    def test_execute_ofa_method_missing_required_raises(self):
        """ Verify error raised if required parameter missing """
        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(self, x: float, y: float):
                return x + y

        app = MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
                    asset_router_url='mocked_asset_url')

        # Envelope missing 'y'
        envelope = CommandEnvelope(
            header=header,
            arguments={"x": "3"}
        )

        with self.assertRaises(ValueError) as ctx:
            app._execute_ofa_method(app.move_axis, envelope)

        self.assertIn("Missing required argument 'y'", str(ctx.exception))
