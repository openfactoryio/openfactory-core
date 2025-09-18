import unittest
from unittest.mock import MagicMock, patch
from openfactory.apps.supervisor import OPCUASupervisor


class OPCUASupervisorTestCase(unittest.TestCase):
    """
    Tests for OPCUASupervisor class
    """

    def setUp(self):
        self.ksql_mock = MagicMock()

        # Patch AssetProducer
        self.asset_producer_patcher = patch("openfactory.assets.asset_base.AssetProducer")
        self.MockAssetProducer = self.asset_producer_patcher.start()
        self.addCleanup(self.asset_producer_patcher.stop)

        # Patch add_attribute
        patcher = patch.object(OPCUASupervisor, 'add_attribute')
        self.mock_add_attribute = patcher.start()
        self.addCleanup(patcher.stop)

        # Patch out async loop creation so we don’t actually start threads
        patch_loop = patch("openfactory.apps.supervisor.opcuasupervisor.asyncio.new_event_loop", return_value=MagicMock())
        self.mock_loop = patch_loop.start()
        self.addCleanup(patch_loop.stop)

        patch_thread = patch("openfactory.apps.supervisor.opcuasupervisor.threading.Thread")
        self.mock_thread = patch_thread.start()
        self.addCleanup(patch_thread.stop)

        patch_run = patch("openfactory.apps.supervisor.opcuasupervisor.asyncio.run_coroutine_threadsafe")
        self.mock_run = patch_run.start()
        self.addCleanup(patch_run.stop)

    def test_constructor_adds_opcua_attributes(self):
        """ Test if OPCUASupervisor sets its OPC UA attributes correctly """

        OPCUASupervisor(
            supervisor_uuid='sup-opc-1',
            device_uuid='dev-opc-1',
            adapter_ip='192.168.0.10',
            adapter_port=4840,
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock_bootstrap'
        )

        calls = self.mock_add_attribute.call_args_list

        expected = {
            'adapter_uri': {
                'value': 'opc.tcp://192.168.0.10:4840',
                'type': 'Events',
                'tag': 'AdapterURI'
            },
            'adapter_connection_status': {
                'value': 'UNAVAILABLE',
                'type': 'Events',
                'tag': 'ConnectionStatus'
            },
            'opcua_namespace_uri': {
                'value': 'demofactory',
                'type': 'Events',
                'tag': 'OPCUANamespaceURI'
            },
            'opcua_browseName': {
                'value': 'PROVER3018',
                'type': 'Events',
                'tag': 'OPCUABrowsName'
            },
        }

        # Collect actual ids from AssetAttribute objects
        actual_ids = [call.kwargs['asset_attribute'].id for call in calls]

        for expected_id in expected:
            self.assertIn(expected_id, actual_ids, f"Missing attribute id: {expected_id}")

        # Verify each attribute’s content
        for call in calls:
            attr = call.kwargs['asset_attribute']
            if attr.id in expected:
                exp = expected[attr.id]
                self.assertEqual(attr.value, exp['value'], f"Incorrect value for {attr.id}")
                self.assertEqual(attr.type, exp['type'], f"Incorrect type for {attr.id}")
                self.assertEqual(attr.tag, exp['tag'], f"Incorrect tag for {attr.id}")

    def test_opcua_client_initialized(self):
        """ Test if OPC UA client is initialized with correct URL """
        supervisor = OPCUASupervisor(
            supervisor_uuid='sup-opc-2',
            device_uuid='dev-opc-2',
            adapter_ip='10.0.0.42',
            adapter_port=4841,
            ksqlClient=self.ksql_mock,
            bootstrap_servers='mock_bootstrap'
        )

        expected_url = "opc.tcp://10.0.0.42:4841"
        actual_url = supervisor.opcua_client.server_url.geturl()

        self.assertEqual(expected_url, actual_url, "OPC UA client URL mismatch")

    def test_reconnect_interval_constant(self):
        """ Ensure the reconnect interval is set as expected """
        self.assertEqual(OPCUASupervisor.RECONNECT_INTERVAL, 10, "Reconnect interval constant mismatch")
