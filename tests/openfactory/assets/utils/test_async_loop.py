import unittest
import asyncio
from openfactory.assets.utils import AsyncLoopThread


class TestAsyncLoopThread(unittest.TestCase):
    """
    Unit tests for AsyncLoopThread
    """

    async def async_add(self, a, b):
        await asyncio.sleep(0.01)
        return a + b

    async def async_fail(self):
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    def test_run_simple_coro(self):
        """ Test running a simple coroutine """
        loop_thread = AsyncLoopThread()
        future = loop_thread.run_coro(self.async_add(2, 3))
        self.assertEqual(future.result(timeout=1), 5)
        loop_thread.stop()

    def test_multiple_coroutines(self):
        """ Test running multiple coroutines concurrently """
        loop_thread = AsyncLoopThread()
        futures = [loop_thread.run_coro(self.async_add(i, i)) for i in range(5)]
        results = [f.result(timeout=1) for f in futures]
        self.assertEqual(results, [0, 2, 4, 6, 8])
        loop_thread.stop()

    def test_exception_propagates(self):
        """ Test that exceptions in coroutines propagate to the Future """
        loop_thread = AsyncLoopThread()
        future = loop_thread.run_coro(self.async_fail())
        with self.assertRaises(ValueError):
            future.result(timeout=1)
        loop_thread.stop()

    def test_stop_idempotent(self):
        """ Test stopping the loop multiple times does not raise """
        loop_thread = AsyncLoopThread()
        loop_thread.stop()
        loop_thread.stop()
