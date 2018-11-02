import asyncio
from contextlib import contextmanager
import sys
import threading


class EventLoopThread(object):
    def __init__(self):
        self.loop = None  # type: asyncio.AbstractEventLoop
        self._condition = threading.Condition()

    def _create_loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.new_event_loop()

    def _run(self):
        with self._condition:
            self.loop = self._create_loop()
            asyncio.set_event_loop(self.loop)
            self._condition.notify()
        try:
            self.loop.run_forever()
        finally:
            with self._condition:
                asyncio.set_event_loop(None)
                self.loop.close()
                self.loop = None
                self._condition.notify()

    def __enter__(self):
        with self._condition:
            threading.Thread(target=self._run).start()
            self._condition.wait_for(lambda: self.loop is not None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._condition:
            self.call_soon(self.loop.stop)
            self._condition.wait_for(lambda: self.loop is None)

    def call_soon(self, callback, *args):
        return self.loop.call_soon_threadsafe(callback, *args)

    def run_coroutine(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    @contextmanager
    def context(self, async_context):
        exit = type(async_context).__aexit__
        value = self.run_coroutine(type(async_context).__aenter__(async_context)).result()
        exc = True
        try:
            try:
                yield value
            except:
                exc = False
                if not self.run_coroutine(exit(async_context, *sys.exc_info())).result():
                    raise
        finally:
            if exc:
                self.run_coroutine(exit(async_context, None, None, None)).result()
