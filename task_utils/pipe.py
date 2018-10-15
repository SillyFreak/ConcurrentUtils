from abc import abstractmethod
from typing import Any, Tuple
import asyncio
import asyncio.locks


__all__ = ['PipeEnd', 'Pipe', 'pipe', 'ConcurrentPipeEnd']


class PipeEnd:
    _none = object()

    @staticmethod
    def check_send_args(value: Any, *, eof: bool) -> None:
        if value is PipeEnd._none and not eof:
            raise ValueError("Missing value or EOF")
        if value is not PipeEnd._none and eof:
            raise ValueError("value and EOF are mutually exclusive")

    @abstractmethod
    def send_nowait(self, value: Any=_none, *, eof=False) -> None:
        raise NotImplemented  # pragma: nocover

    @abstractmethod
    async def send(self, value: Any=_none, *, eof=False) -> None:
        raise NotImplemented  # pragma: nocover

    @abstractmethod
    async def recv(self) -> Any:
        raise NotImplemented  # pragma: nocover

    async def request_sendnowait(self, value: Any) -> Any:
        self.send_nowait(value)
        return await self.recv()

    async def request(self, value: Any) -> Any:
        await self.send(value)
        return await self.recv()


Pipe = Tuple[PipeEnd, PipeEnd]


def pipe(maxsize=0, *, loop=None) -> Pipe:
    """\
    A bidirectional pipe of Python objects.

    >>> async def example1():
    ...     a, b = pipe()
    ...     a.send_nowait('foo')
    ...     print(await b.recv())
    >>> asyncio.run(example1())
    foo
    >>> async def example2():
    ...     a, b = pipe()
    ...     await b.send(eof=True)
    ...     await a.recv()
    >>> asyncio.run(example2())
    Traceback (most recent call last):
      ...
    EOFError
    """

    class QueueStream:
        def __init__(self, maxsize=0, *, loop=None) -> None:
            self._queue: asyncio.Queue = asyncio.Queue(maxsize, loop=loop)
            self._eof = asyncio.locks.Event(loop=loop)

        def _check_send(self, value: Any, *, eof: bool) -> None:
            if self._eof.is_set():
                raise EOFError("Cannot send after EOF")
            PipeEnd.check_send_args(value, eof=eof)

        def send_nowait(self, value: Any, *, eof: bool) -> None:
            self._check_send(value, eof=eof)

            if eof:
                self._eof.set()
            else:
                self._queue.put_nowait(value)

        async def send(self, value: Any, *, eof: bool) -> None:
            self._check_send(value, eof=eof)

            if eof:
                self._eof.set()
            else:
                await self._queue.put(value)

        async def recv(self) -> Any:
            get = asyncio.create_task(self._queue.get())
            eof = asyncio.create_task(self._eof.wait())

            done, pending = await asyncio.wait([get, eof], return_when=asyncio.FIRST_COMPLETED)

            # cancel get or eof, whichever is not finished
            for task in pending:
                task.cancel()

            if get in done:
                return get.result()
            else:
                raise EOFError

    class _PipeEnd(PipeEnd):
        def __init__(self, send: QueueStream, recv: QueueStream) -> None:
            super().__init__()
            self._send = send
            self._recv = recv

        def send_nowait(self, value: Any=PipeEnd._none, *, eof=False):
            self._send.send_nowait(value, eof=eof)

        async def send(self, value: Any=PipeEnd._none, *, eof=False):
            await self._send.send(value, eof=eof)

        async def recv(self)-> Any:
            return await self._recv.recv()

    a, b = QueueStream(maxsize, loop=loop), QueueStream(maxsize, loop=loop)
    return _PipeEnd(a, b), _PipeEnd(b, a)


class ConcurrentPipeEnd(PipeEnd):
    """
    Wraps a PipeEnd so that its async functions can be called from a different event loop.
    The synchronous `send_nowait` method is not supported.

    The `loop` to which the PipeEnd originally belongs must be given, as any async calls are
    scheduled onto that loop.
    Multiprocessing is not supported, as access to the actual event loop is required.
    Objects are transferred by reference; they are not pickled or otherwise serialized.
    """

    def __init__(self, pipe_end, *, loop) -> None:
        super().__init__()
        self._pipe_end = pipe_end
        self._loop = loop

    async def _run(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return await asyncio.wrap_future(future)

    async def send(self, value=PipeEnd._none, *, eof=False):
        await self._run(self._pipe_end.send(value, eof=eof))

    async def recv(self):
        return await self._run(self._pipe_end.recv())
