import os
import psutil
import unittest
from openfactory.assets.utils import AsyncLoopThread


class TestAsyncLoopThread(unittest.TestCase):
    """
    Test class AsyncLoopThread
    """

    def test_thread_starts(self):
        """ Test background thread is started """
        loop = AsyncLoopThread()
        self.assertTrue(loop.thread.is_alive())
        loop.stop()

    def test_run_coro(self):
        """ Test run_coro executes coroutine and returns result """

        async def sample():
            return 42

        loop = AsyncLoopThread()

        future = loop.run_coro(sample())

        self.assertEqual(future.result(timeout=5), 42)

        loop.stop()

    def test_run_coro_exception(self):
        """ Test run_coro propagates coroutine exceptions """

        async def sample():
            raise ValueError("boom")

        loop = AsyncLoopThread()

        future = loop.run_coro(sample())

        with self.assertRaises(ValueError):
            future.result(timeout=5)

        loop.stop()

    def test_stop_stops_thread(self):
        """ Test stop terminates background thread """
        loop = AsyncLoopThread()
        loop.stop()
        self.assertFalse(loop.thread.is_alive())

    @unittest.skipUnless(hasattr(psutil.Process(os.getpid()), "num_fds"), "num_fds only available on Unix platforms")
    def test_stop_releases_file_descriptors(self):
        """ Test stop releases file descriptors created by the event loop """
        proc = psutil.Process(os.getpid())

        before = proc.num_fds()
        loop = AsyncLoopThread()
        loop.stop()
        after_stop = proc.num_fds()

        self.assertEqual(after_stop, before)
