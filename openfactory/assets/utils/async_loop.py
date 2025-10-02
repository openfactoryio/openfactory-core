""" AsyncLoopThread """

import asyncio
import threading
from typing import Coroutine, Any


class AsyncLoopThread:
    """
    Runs an asyncio event loop in a dedicated background thread.

    This allows running asynchronous coroutines in a synchronous context,
    without blocking the main thread.

    Example usage:
        loop_thread = AsyncLoopThread()
        future = loop_thread.run_coro(some_async_func())
        result = future.result()  # blocks until coroutine completes
        loop_thread.stop()
    """

    def __init__(self) -> None:
        """ Initializes the asyncio loop and starts the background thread. """
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        """ Sets the event loop in this thread and runs it forever. """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run_coro(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Future:
        """
        Schedules a coroutine to run in the background loop.

        Args:
            coro (Coroutine): The coroutine to run in the loop.

        Returns:
            asyncio.Future: A Future representing the execution of the coroutine.
        """
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self) -> None:
        """
        Stops the background event loop and joins the thread.

        This blocks until the thread has fully terminated.
        """
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()
