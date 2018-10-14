from abc import abstractmethod
import asyncio
import asyncio.locks


class EOFError(Exception): pass


class Pipe:
    _none = object()

    @staticmethod
    def check_send_args(value, *, eof):
        if value is Pipe._none and not eof:
            raise ValueError("Missing value or EOF")
        if value is not Pipe._none and eof:
            raise ValueError("value and EOF are mutually exclusive")

    @abstractmethod
    def send_nowait(self, value=_none, *, eof=False):
        raise NotImplemented  # pragma: nocover

    @abstractmethod
    async def send(self, value=_none, *, eof=False):
        raise NotImplemented  # pragma: nocover

    @abstractmethod
    async def recv(self):
        raise NotImplemented  # pragma: nocover

    async def request_sendnowait(self, value):
        self.send_nowait(value)
        return await self.recv()

    async def request(self, value):
        await self.send(value)
        return await self.recv()


def pipe(maxsize=0):
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
    task_utils.pipe.EOFError
    """

    class QueueStream:
        def __init__(self, maxsize=0):
            self._queue = asyncio.Queue(maxsize)
            self._eof = asyncio.locks.Event()

        def _check_send(self, value, *, eof):
            if self._eof.is_set():
                raise EOFError("Cannot send after EOF")
            Pipe.check_send_args(value, eof=eof)

        def send_nowait(self, value, *, eof):
            self._check_send(value, eof=eof)

            if eof:
                self._eof.set()
            else:
                self._queue.put_nowait(value)

        async def send(self, value, *, eof):
            self._check_send(value, eof=eof)

            if eof:
                self._eof.set()
            else:
                await self._queue.put(value)

        async def recv(self):
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

    class _Pipe(Pipe):
        def __init__(self, send, recv):
            super().__init__()
            self._send = send
            self._recv = recv

        def send_nowait(self, value=Pipe._none, *, eof=False):
            self._send.send_nowait(value, eof=eof)

        async def send(self, value=Pipe._none, *, eof=False):
            await self._send.send(value, eof=eof)

        async def recv(self):
            return await self._recv.recv()

    a, b = QueueStream(maxsize), QueueStream(maxsize)
    return _Pipe(a, b), _Pipe(b, a)
