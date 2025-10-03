import unittest
import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch
from openfactory.assets.utils.async_loop import AsyncLoopThread
from openfactory.assets.utils.nats_subscriber import NATSSubscriber


class TestNATSSubscriber(unittest.TestCase):
    """
    Unit tests for NATSSubscriber
    """

    def setUp(self):
        """ Prepare a mocked AsyncLoopThread and callback for tests """
        self.loop_thread = AsyncLoopThread()
        self.mock_callback = Mock()

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_start_calls_connect_and_subscribe(self, mock_nats_connect):
        """ Test that start triggers NATS connection and subscription """
        mock_nc = AsyncMock()
        mock_nats_connect.return_value = mock_nc
        mock_sub = AsyncMock()
        mock_nc.subscribe.return_value = mock_sub

        servers = "nats://localhost:4222"
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()

        # Run the loop until tasks complete
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        mock_nats_connect.assert_awaited_once_with(servers)
        mock_nc.subscribe.assert_awaited_once()
        self.assertEqual(subscriber.sub, mock_sub)
        self.assertEqual(subscriber.nc, mock_nc)

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_message_handler_invokes_callback(self, mock_nats_connect):
        """ Test that incoming NATS messages trigger the callback """
        mock_nc = AsyncMock()
        mock_nats_connect.return_value = mock_nc

        # Create fake message
        class FakeMsg:
            def __init__(self, subject, data):
                self.subject = subject
                self.data = data

        async def fake_subscribe(subject, cb):
            msg = FakeMsg(subject, json.dumps({"foo": "bar"}).encode())
            await cb(msg)
            return AsyncMock()

        mock_nc.subscribe.side_effect = fake_subscribe

        servers = "nats://localhost:4222"
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        self.mock_callback.assert_called_once_with("TEST.*", {"foo": "bar"})

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_stop_calls_unsubscribe_and_close(self, mock_nats_connect):
        """ Test that stop calls unsubscribe and closes the connection """
        mock_nc = AsyncMock()
        mock_sub = AsyncMock()
        mock_nats_connect.return_value = mock_nc
        mock_nc.subscribe.return_value = mock_sub

        servers = "nats://localhost:4222"
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        subscriber.stop()
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        mock_sub.unsubscribe.assert_awaited_once()
        mock_nc.close.assert_awaited_once()

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_connect_with_multiple_servers(self, mock_nats_connect):
        """ Test that NATSSubscriber passes a list of servers correctly to nats.connect """
        mock_nc = AsyncMock()
        mock_nats_connect.return_value = mock_nc
        mock_sub = AsyncMock()
        mock_nc.subscribe.return_value = mock_sub

        servers = ["nats://server1:4222", "nats://server2:4222"]
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        # Assert that the list of servers was passed as-is
        mock_nats_connect.assert_awaited_once_with(servers)
        mock_nc.subscribe.assert_awaited_once()
