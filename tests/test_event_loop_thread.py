import pytest

from contextlib import asynccontextmanager
import threading
from concurrent_utils.event_loop_thread import EventLoopThread


def test_event_loop_thread():
    with EventLoopThread():
        pass


def test_call_soon():
    event = threading.Event()

    def callback():
        event.set()

    with EventLoopThread() as loop:
        loop.call_soon(callback)

    assert event.is_set()


def test_run_coroutine():
    event = threading.Event()

    async def coro_fun():
        event.set()
        return 1

    with EventLoopThread() as loop:
        assert loop.run_coroutine(coro_fun()).result() == 1
        assert event.is_set()


def test_context():
    enter = threading.Event()
    exit = threading.Event()

    @asynccontextmanager
    async def context_manager():
        enter.set()
        try:
            yield 1
        finally:
            exit.set()

    with EventLoopThread() as loop:
        mgr = context_manager()
        assert not enter.is_set()
        with loop.context(mgr) as result:
            assert enter.is_set()
            assert result == 1
            assert not exit.is_set()
        assert exit.is_set()


def test_context_raises():
    @asynccontextmanager
    async def catching():
        with pytest.raises(Exception, message="foo"):
            yield

    @asynccontextmanager
    async def noncatching():
        yield

    with EventLoopThread() as loop:
        with loop.context(catching()):
            raise Exception("foo")

        with pytest.raises(Exception, message="foo"), loop.context(noncatching()):
            raise Exception("foo")
