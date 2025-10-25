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

        # Run the loop briefly to allow coroutines to schedule
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        # Check that connect was awaited with new callback parameters
        mock_nats_connect.assert_awaited_once()
        connect_kwargs = mock_nats_connect.call_args.kwargs
        self.assertEqual(connect_kwargs["servers"], servers)
        self.assertEqual(connect_kwargs["max_reconnect_attempts"], -1)
        self.assertEqual(connect_kwargs["connect_timeout"], 2)
        self.assertEqual(connect_kwargs["reconnect_time_wait"], 1)

        # Check callbacks are passed
        self.assertIn("error_cb", connect_kwargs)
        self.assertIn("disconnected_cb", connect_kwargs)
        self.assertIn("reconnected_cb", connect_kwargs)

        # Ensure subscription happened
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

        # Stop should run _stop_all coroutine internally
        subscriber.stop()
        self.loop_thread.run_coro(asyncio.sleep(0.01)).result(timeout=1)

        # Make sure unsubscribe and close coroutines were awaited
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

        # Check that connect was awaited
        mock_nats_connect.assert_awaited_once()
        connect_kwargs = mock_nats_connect.call_args.kwargs
        self.assertEqual(connect_kwargs["servers"], servers)
        self.assertEqual(connect_kwargs["max_reconnect_attempts"], -1)
        self.assertEqual(connect_kwargs["connect_timeout"], 2)
        self.assertEqual(connect_kwargs["reconnect_time_wait"], 1)

        # Ensure callbacks are passed
        self.assertIn("error_cb", connect_kwargs)
        self.assertIn("disconnected_cb", connect_kwargs)
        self.assertIn("reconnected_cb", connect_kwargs)

        # Ensure subscription happened
        mock_nc.subscribe.assert_awaited_once()
        self.assertEqual(subscriber.sub, mock_sub)
        self.assertEqual(subscriber.nc, mock_nc)

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_closing_flag_set_on_stop(self, mock_nats_connect):
        """ Test that the _closing flag is True after stop() is called """
        mock_nc = AsyncMock()
        mock_sub = AsyncMock()
        mock_nats_connect.return_value = mock_nc
        mock_nc.subscribe.return_value = mock_sub

        servers = "nats://localhost:4222"
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()

        # Initially, _closing should be False
        self.assertFalse(subscriber._closing)

        # Call stop
        subscriber.stop()

        # After stop, _closing should be True
        self.assertTrue(subscriber._closing)

    @patch("openfactory.assets.utils.nats_subscriber.nats.connect", new_callable=AsyncMock)
    def test_disconnected_cb_skipped_when_closing(self, mock_nats_connect):
        """ Test that disconnected callback does not print reconnect message if _closing is True """
        mock_nc = AsyncMock()
        mock_sub = AsyncMock()
        mock_nats_connect.return_value = mock_nc
        mock_nc.subscribe.return_value = mock_sub

        servers = "nats://localhost:4222"
        subject = "TEST.*"
        subscriber = NATSSubscriber(self.loop_thread, servers, subject, self.mock_callback)
        subscriber.start()

        # Capture the disconnected_cb that was passed to nats.connect
        connect_kwargs = mock_nats_connect.call_args.kwargs
        disconnected_cb = connect_kwargs["disconnected_cb"]

        # Initially, _closing is False, so callback should print the message
        with patch("builtins.print") as mock_print:
            asyncio.run(disconnected_cb())
            mock_print.assert_called_with("NATS disconnected — will attempt reconnect...")

        # Set _closing to True and call the callback again
        subscriber._closing = True
        with patch("builtins.print") as mock_print:
            asyncio.run(disconnected_cb())
            mock_print.assert_not_called()
