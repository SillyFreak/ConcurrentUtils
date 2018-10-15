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


try:
    import zmq
except ImportError:  # pragma: nocover
    pass
else:
    try:  # pragma: nocover
        import cPickle
        pickle = cPickle
    except:  # pragma: nocover
        cPickle = None
        import pickle


    class ZmqPipeEnd(PipeEnd):
        """
        A PipeEnd backed by an asynchronous ZMQ socket.
        This PipeEnd can be used for multiprocessing; it will serialize objects that are transmitted.
        The default serialization mechanism is pickle,
        which can be customized by overriding the `_serialize` and `_deserialize` methods.
        The synchronous `send_nowait` method is not supported.
        """

        def __init__(self, ctx, socket_type, address, *, port, bind=False) -> None:
            super().__init__()
            if socket_type not in {zmq.DEALER, zmq.ROUTER}:
                raise ValueError("DEALER or ROUTER socket type required")

            self._sock = ctx.socket(socket_type)
            self._socket_type = socket_type
            if bind:
                if port is None:
                    self._sock.bind(address)
                elif port == 0:
                    port = self._sock.bind_to_random_port(address)
                else:
                    self._sock.bind(f'{address}:{port}')
            else:
                if port is None:
                    self._sock.connect(address)
                else:
                    self._sock.connect(f'{address}:{port}')
            self.port = port
            self._dealer_ident = None

            self._eof_sent = False
            self._eof_recvd = False

        async def initialize(self):
            if self._socket_type == zmq.ROUTER:
                self._dealer_ident, _ = await self._sock.recv_multipart()
            else:
                await self._sock.send_multipart([b''])

        async def _send(self, *parts):
            if self._socket_type == zmq.ROUTER:
                parts = [self._dealer_ident, *parts]
            await self._sock.send_multipart(parts)

        async def _recv(self):
            parts = await self._sock.recv_multipart()
            if self._socket_type == zmq.ROUTER:
                parts = parts[1:]
            return parts

        def _serialize(self, value):
            return pickle.dumps(value, pickle.DEFAULT_PROTOCOL)

        def _deserialize(self, msg):
            return pickle.loads(msg)

        async def send(self, value=PipeEnd._none, *, eof=False):
            if self._eof_sent:
                raise EOFError("Cannot send after EOF")
            PipeEnd.check_send_args(value, eof=eof)

            if eof:
                await self._send(b'')
                self._eof_sent = True
            else:
                msg = self._serialize(value)
                await self._send(b'', msg)

        async def recv(self):
            if self._eof_recvd:
                raise EOFError("Cannot receive after EOF")

            msg = await self._recv()
            if len(msg) == 1:
                self._eof_recvd = True
                raise EOFError
            else:
                _, msg = msg
                return self._deserialize(msg)


    def zmq_tcp_pipe_end(ctx, side, *, port=0):
        if side == 'a':
            return ZmqPipeEnd(ctx, zmq.DEALER, 'tcp://*', port=port, bind=True)
        elif side == 'b':
            if port == 0:
                raise ValueError("b side requires port argument")
            return ZmqPipeEnd(ctx, zmq.ROUTER, 'tcp://127.0.0.1', port=port)
        else:
            raise ValueError("side must be 'a' or 'b'")


    async def zmq_tcp_pipe(ctx, *, port=0):
        a = zmq_tcp_pipe_end(ctx, 'a', port=port)
        b = zmq_tcp_pipe_end(ctx, 'b', port=a.port)
        await a.initialize()
        await b.initialize()
        return a, b


    def zmq_inproc_pipe_end(ctx, side, endpoint):
        if side == 'a':
            return ZmqPipeEnd(ctx, zmq.DEALER, endpoint, port=None, bind=True)
        elif side == 'b':
            return ZmqPipeEnd(ctx, zmq.ROUTER, endpoint, port=None)
        else:
            raise ValueError("side must be 'a' or 'b'")


    async def zmq_inproc_pipe(ctx, endpoint):
        a = zmq_inproc_pipe_end(ctx, 'a', endpoint)
        b = zmq_inproc_pipe_end(ctx, 'b', endpoint)
        await a.initialize()
        await b.initialize()
        return a, b
