import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service import AssetForwarderService


class TestAssetForwarderWatchdog(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for the Kafka watchdog.
    """

    def setUp(self):
        """ Create a service instance configured for watchdog testing. """
        self.service = AssetForwarderService(
            ksqlClient=MagicMock(),
            bootstrap_servers="localhost:9092",
            test_mode=True,
        )

        self.service.logger = MagicMock()
        self.service.consumer = MagicMock()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_watchdog_waits_until_first_assignment(self, mock_sleep):
        """ Test watchdog does nothing until the consumer has been assigned. """

        self.service.consumer_has_been_assigned = False

        mock_sleep.side_effect = asyncio.CancelledError()

        with self.assertRaises(asyncio.CancelledError):
            await self.service.kafka_watchdog()

        self.service.consumer.assignment.assert_not_called()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_watchdog_continues_when_assignment_exists(self, mock_sleep):
        """ Test watchdog continues normally when partitions are assigned. """

        self.service.consumer_has_been_assigned = True
        self.service.consumer.assignment.return_value = [MagicMock()]

        # First iteration runs normally, second iteration stops the infinite loop.
        mock_sleep.side_effect = [
            None,
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.kafka_watchdog()

        self.service.consumer.assignment.assert_called_once()
        self.service.logger.warning.assert_not_called()
        self.service.logger.info.assert_not_called()
        self.service.logger.critical.assert_not_called()

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_watchdog_logs_when_assignment_is_lost(self, mock_sleep):
        """ Test watchdog logs when all partition assignments are lost. """

        self.service.consumer_has_been_assigned = True
        self.service.consumer.assignment.return_value = []

        mock_sleep.side_effect = [
            None,
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.kafka_watchdog()

        self.service.consumer.assignment.assert_called_once()

        self.service.logger.warning.assert_called_once_with(
            "[Kafka Watchdog] Kafka consumer has no partition assignment. "
            "Waiting for rebalance..."
        )

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_watchdog_logs_when_assignment_returns(self, mock_sleep):
        """ Test watchdog logs when a partition assignment is regained. """

        self.service.consumer_has_been_assigned = True
        self.service.consumer.assignment.side_effect = [
            [],
            [MagicMock()],
        ]

        mock_sleep.side_effect = [
            None,
            None,
            asyncio.CancelledError(),
        ]

        with self.assertRaises(asyncio.CancelledError):
            await self.service.kafka_watchdog()

        self.assertEqual(self.service.consumer.assignment.call_count, 2)

        self.service.logger.warning.assert_called_once_with(
            "[Kafka Watchdog] Kafka consumer has no partition assignment. "
            "Waiting for rebalance..."
        )

        self.service.logger.info.assert_called_once_with(
            "[Kafka Watchdog] Kafka consumer has received a partition assignment again."
        )

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.os._exit", side_effect=SystemExit(1))
    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    async def test_watchdog_exits_when_assignment_check_fails(self, mock_sleep, mock_exit):
        """ Test watchdog terminates if consumer.assignment() raises. """

        self.service.consumer_has_been_assigned = True
        self.service.consumer.assignment.side_effect = RuntimeError("boom")

        mock_sleep.side_effect = [None]

        with self.assertRaises(SystemExit):
            await self.service.kafka_watchdog()

        self.service.logger.exception.assert_called_once()
        mock_exit.assert_called_once_with(1)

    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.os._exit", side_effect=SystemExit(1))
    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.asyncio.sleep", new_callable=AsyncMock)
    @patch("openfactory.fanoutlayer.asset_forwarder.src.asset_forwarder_service.os.getenv")
    async def test_watchdog_exits_after_assignment_timeout(self, mock_getenv, mock_sleep, mock_exit):
        """ Test watchdog terminates if no partition assignment returns within the timeout. """

        def getenv_side_effect(key, default=None):
            if key == "KAFKA_WATCHDOG_INTERVAL":
                return "1"
            if key == "KAFKA_WATCHDOG_TIMEOUT":
                return "2"
            return default

        mock_getenv.side_effect = getenv_side_effect

        self.service.consumer_has_been_assigned = True
        self.service.consumer.assignment.return_value = []

        loop = asyncio.get_running_loop()

        original_time = loop.time
        times = iter([100.0, 103.0])

        loop.time = MagicMock(side_effect=lambda: next(times))

        mock_sleep.side_effect = [
            None,
            None,
        ]

        try:
            with self.assertRaises(SystemExit):
                await self.service.kafka_watchdog()
        finally:
            loop.time = original_time

        self.assertEqual(self.service.consumer.assignment.call_count, 2)

        self.service.logger.warning.assert_called_once_with(
            "[Kafka Watchdog] Kafka consumer has no partition assignment. "
            "Waiting for rebalance..."
        )

        self.service.logger.critical.assert_called_once_with(
            "[Kafka Watchdog] Kafka consumer has had no partition assignment for %.0f seconds. "
            "Terminating so the container can be restarted.",
            2.0,
        )

        mock_exit.assert_called_once_with(1)
