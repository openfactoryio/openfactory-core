import unittest
from unittest.mock import MagicMock, patch
from openfactory.apps.supervisor import BaseSupervisor


class SupervisorTestImpl(BaseSupervisor):
    """ A testable subclass of BaseSupervisor with mocked abstract methods """

    def available_commands(self):
        return [
            {"command": "start", "description": "Start the device"},
            {"command": "stop", "description": "Stop the device"}
        ]

    def on_command(self, msg_key, msg_value):
        """ Mocked method """
        pass


class BaseSupervisorTestCase(unittest.TestCase):
    """
    Tests for class BaseSupervisor
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        # Patch add_attribute
        self.add_attribute_patcher = patch.object(BaseSupervisor, 'add_attribute')
        self.mock_add_attribute = self.add_attribute_patcher.start()
        self.addCleanup(self.add_attribute_patcher.stop)

    def test_inheritance(self):
        """ Test if OpenFactoryApp derives from Asset """
        from openfactory.apps import OpenFactoryApp
        self.assertTrue(issubclass(BaseSupervisor, OpenFactoryApp),
                        "BaseSupervisor should derive from OpenFactoryApp")

    @patch('openfactory.apps.supervisor.base_supervisor.AssetAttribute')
    def test_constructor_adds_device_attribute(self, MockAssetAttribute):
        """ Test if supervisor attributes are set """
        mock_asset_attr = MagicMock()
        MockAssetAttribute.return_value = mock_asset_attr

        device_uuid = "dev-456"
        SupervisorTestImpl(device_uuid, self.ksql_mock, bootstrap_servers='mock_bootstrap_servers')

        # Verify that add_attribute was called with only asset_attribute
        self.mock_add_attribute.assert_any_call(asset_attribute=mock_asset_attr)

        # Verify that AssetAttribute was constructed with the correct values
        MockAssetAttribute.assert_called_with(
            id='device_added',
            value=device_uuid,
            type='Events',
            tag='DeviceAdded'
        )

    @patch('openfactory.apps.supervisor.base_supervisor.Asset')
    def test_send_available_commands(self, MockAsset):
        """ Test _send_available_commands method """
        # Setup
        mock_asset_instance = MockAsset.return_value
        supervisor = SupervisorTestImpl("dev-456", self.ksql_mock, bootstrap_servers='mock_bootstrap_servers')

        # Execute
        supervisor._send_available_commands()

        # Collect call arguments
        calls = mock_asset_instance.add_attribute.call_args_list

        # Expected commands
        expected_commands = {
            "start": "Start the device",
            "stop": "Stop the device"
        }

        # Verify that the expected commands were added as AssetAttributes
        for call_obj in calls:
            asset_attr = call_obj.kwargs['asset_attribute']

            self.assertIn(asset_attr.id, expected_commands, f"Unexpected command id: {asset_attr.id}")
            expected_desc = expected_commands[asset_attr.id]
            self.assertEqual(asset_attr.value, expected_desc)
            self.assertEqual(asset_attr.type, 'Method')
            self.assertEqual(asset_attr.tag, 'Method')
            self.assertEqual(asset_attr.id, asset_attr.id)

    @patch('openfactory.apps.supervisor.base_supervisor.KafkaCommandsConsumer')
    @patch.object(SupervisorTestImpl, '_send_available_commands')
    def test_main_loop_calls_consume(self, mock_send_commands, MockKafkaConsumer):
        """ Test command consumer in main loop """
        # Setup
        mock_consumer_instance = MockKafkaConsumer.return_value
        supervisor = SupervisorTestImpl("dev-456", self.ksql_mock, bootstrap_servers='mock_bootstrap_servers')

        # Execute
        supervisor.main_loop()

        # Assert
        mock_send_commands.assert_called_once()
        MockKafkaConsumer.assert_called_once_with(
            consumer_group_id='DEV-UUID-SUPERVISOR-GROUP',
            asset_uuid='dev-456',
            on_command=supervisor.on_command,
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock_bootstrap_servers'
        )
        mock_consumer_instance.consume.assert_called_once()

    def test_available_commands_not_implemented(self):
        """ Test call to available_commands raise NotImplementedError """
        sup = BaseSupervisor("dev-uuid", ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        with self.assertRaises(NotImplementedError):
            sup.available_commands()

    def test_on_command_not_implemented(self):
        """ Test call to on_command raise NotImplementedError """
        sup = BaseSupervisor("dev-uuid", ksqlClient=self.ksql_mock, bootstrap_servers='mock_bootstrap')
        with self.assertRaises(NotImplementedError):
            sup.on_command("msg_key", "msg_value")
