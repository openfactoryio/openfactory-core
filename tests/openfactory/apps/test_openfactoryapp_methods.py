import unittest
import json
from unittest.mock import patch, MagicMock
from typing import Annotated
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

        # Patch add_attribute
        self.add_attribute_patch = patch.object(OpenFactoryApp, "add_attribute")
        self.mock_add_attribute = self.add_attribute_patch.start()
        self.addCleanup(self.add_attribute_patch.stop)

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

    def test_execute_ofa_method_bool_conversion(self):
        """ Test that _execute_ofa_method converts boolean arguments correctly """

        class MyApp(OpenFactoryApp):
            called_args = None

            @ofa_method()
            def toggle(self, enabled: bool):
                self.called_args = enabled

        app = MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        test_cases = [
            ("true", True),
            ("false", False),
            ("1", True),
            ("0", False),
            ("on", True),
            ("off", False),
        ]

        for value, expected in test_cases:
            with self.subTest(value=value):

                envelope = CommandEnvelope(
                    header=header,
                    arguments={"enabled": value}
                )

                app._execute_ofa_method(app.toggle, envelope)

                self.assertEqual(app.called_args, expected)

    def test_execute_ofa_method_invalid_bool_raises(self):
        """ Test that _execute_ofa_method raises if bool conversion fails """

        class MyApp(OpenFactoryApp):

            @ofa_method()
            def toggle(self, enabled: bool):
                pass

        app = MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        envelope = CommandEnvelope(
            header=header,
            arguments={"enabled": "not_bool"}
        )

        with self.assertRaises(ValueError):
            app._execute_ofa_method(app.toggle, envelope)

    def test_execute_ofa_method_invalid_argument_type_raises(self):
        """ Test that _execute_ofa_method raises if argument conversion fails """

        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(self, x: int, y: int):
                pass

        app = MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        envelope = CommandEnvelope(
            header=header,
            arguments={
                "x": "not_an_int",
                "y": "2"
            }
        )

        with self.assertRaises(ValueError) as ctx:
            app._execute_ofa_method(app.move_axis, envelope)

        self.assertIn("Failed to convert argument 'x'", str(ctx.exception))

    def test_cmd_attribute_created(self):
        """ Verify CMD attribute is added for decorated methods. """

        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(self, x: float, y: float):
                return x + y

        MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        calls = self.mock_add_attribute.call_args_list

        cmd_found = False
        for c in calls:
            attr = c.kwargs["asset_attribute"]
            if attr.id == "move_axis_CMD":
                cmd_found = True

        self.assertTrue(cmd_found)

    def test_register_ofa_method_creates_contract(self):
        """ Verify that method contract is correctly generated. """

        class MyApp(OpenFactoryApp):

            @ofa_method(param_description={"x": "X coord", "y": "Y coord"})
            def move_axis(self, x: float, y: float):
                """Move axis"""
                return x + y

        MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        # capture add_attribute calls
        calls = self.mock_add_attribute.call_args_list

        # Find the method attribute
        method_attr = None
        for c in calls:
            attr = c.kwargs["asset_attribute"]
            if attr.id == "move_axis":
                method_attr = attr
                break

        self.assertIsNotNone(method_attr)

        contract = json.loads(method_attr.value)
        self.assertEqual(contract["description"], "Move axis")
        args = {a["name"]: a for a in contract["arguments"]}
        self.assertEqual(args["x"]["description"], "X coord")
        self.assertEqual(args["y"]["description"], "Y coord")

    def test_register_ofa_method_raises_if_not_decorated(self):
        """ Test that _register_ofa_method raises if method is not decorated """

        class MyApp(OpenFactoryApp):

            def move_axis(self, x: float, y: float):
                pass

        app = MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        with self.assertRaises(ValueError) as ctx:
            app._register_ofa_method(app.move_axis)

        self.assertIn("Method is not decorated with @ofa_method", str(ctx.exception))

    def test_annotated_param_description_propagates_to_contract(self):
        """ Verify Annotated descriptions appear in method contract. """

        class MyApp(OpenFactoryApp):

            @ofa_method()
            def move_axis(
                self,
                x: Annotated[float, "X coord"],
                y: Annotated[float, "Y coord"]
            ):
                return x + y

        MyApp(
            bootstrap_servers='mock_bootstrap',
            ksqlClient=self.ksql_mock,
            asset_router_url='mocked_asset_url'
        )

        calls = self.mock_add_attribute.call_args_list

        method_attr = None
        for c in calls:
            attr = c.kwargs["asset_attribute"]
            if attr.id == "move_axis":
                method_attr = attr
                break

        contract = json.loads(method_attr.value)

        args = {a["name"]: a for a in contract["arguments"]}

        self.assertEqual(args["x"]["description"], "X coord")
        self.assertEqual(args["y"]["description"], "Y coord")

    def test_on_cmd_logs_error_when_validation_fails_real_callback(self):
        """ Test that on_cmd logs an error if model_validate_json raises """
        class MyApp(OpenFactoryApp):
            @ofa_method()
            def move_axis(self):
                pass

        app = MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
                    asset_router_url='mocked_asset_url')

        args, kwargs = self.mock_subscribe.call_args  # last subscribed method
        callback = args[1]  # this is the real on_cmd callback created by the class

        # Patch model_validate_json to raise
        with patch.object(CommandEnvelope, "model_validate_json", side_effect=ValueError("invalid JSON")):
            # Call the callback with any dummy key and msg_value
            callback("key", {"VALUE": "{}"})

        # Assert logger.error was called
        app.logger.error.assert_called()
        logged_msg = app.logger.error.call_args[0][0]
        assert "Failed to execute move_axis" in logged_msg
        assert "ValueError" in logged_msg

    def test_on_cmd_logs_error_when_execution_fails_real_callback(self):
        """ Test that on_cmd logs an error if _execute_ofa_method raises """
        class MyApp(OpenFactoryApp):
            @ofa_method()
            def move_axis(self):
                pass

        app = MyApp(bootstrap_servers='mock_bootstrap', ksqlClient=self.ksql_mock,
                    asset_router_url='mocked_asset_url')

        args, kwargs = self.mock_subscribe.call_args
        callback = args[1]  # real callback

        # Patch model_validate_json to succeed, _execute_ofa_method to fail
        with patch.object(CommandEnvelope, "model_validate_json", return_value=MagicMock()), \
             patch.object(app, "_execute_ofa_method", side_effect=RuntimeError("execution failed")):

            callback("key", {"VALUE": "{}"})

        app.logger.error.assert_called()
        logged_msg = app.logger.error.call_args[0][0]
        assert "Failed to execute move_axis" in logged_msg
        assert "RuntimeError" in logged_msg
