import unittest
import asyncio
import os
import psutil
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

    def test_thread_starts(self):
        """ Test background thread is started """
        loop = AsyncLoopThread()
        self.assertTrue(loop.thread.is_alive())
        loop.stop()

    def test_stop_stops_thread(self):
        """ Test stop terminates background thread """
        loop = AsyncLoopThread()
        loop.stop()
        self.assertFalse(loop.thread.is_alive())

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

    @unittest.skipUnless(hasattr(psutil.Process(os.getpid()), "num_fds"), "num_fds only available on Unix platforms")
    def test_stop_releases_file_descriptors(self):
        """ Test stop releases file descriptors created by the event loop """
        proc = psutil.Process(os.getpid())

        before = proc.num_fds()
        loop = AsyncLoopThread()
        loop.stop()
        after_stop = proc.num_fds()

        self.assertEqual(after_stop, before)
