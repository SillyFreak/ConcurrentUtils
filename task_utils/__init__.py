import asyncio
import asyncio.locks


class EOFError(Exception): pass


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
    task_utils.EOFError
    """

    _none = object()

    class QueueStream:
        def __init__(self, maxsize=0):
            self.queue = asyncio.Queue(maxsize)
            self.eof = asyncio.locks.Event()

    class Pipe:
        def __init__(self, send, recv):
            self._send = send
            self._recv = recv

        def _check_send(self, value=_none, *, eof=False):
            if value is _none and not eof:
                raise ValueError("Missing value or EOF")
            if value is not _none and eof:
                raise ValueError("value and EOF are mutually exclusive")
            if self._send.eof.is_set():
                raise EOFError("Cannot send after EOF")

        def send_nowait(self, value=_none, *, eof=False):
            self._check_send(value, eof=eof)

            if eof:
                self._send.eof.set()
            else:
                self._send.queue.put_nowait(value)

        async def send(self, value=_none, *, eof=False):
            self._check_send(value, eof=eof)

            if eof:
                self._send.eof.set()
            else:
                await self._send.queue.put(value)

        async def recv(self):
            get = asyncio.create_task(self._recv.queue.get())
            eof = asyncio.create_task(self._recv.eof.wait())

            done, pending = await asyncio.wait([get, eof], return_when=asyncio.FIRST_COMPLETED)

            # cancel get or eof, whichever is not finished
            for task in pending:
                task.cancel()

            if get in done:
                return get.result()
            else:
                raise EOFError

        async def request_sendnowait(self, value):
            self.send_nowait(value)
            return await self.recv()

        async def request(self, value):
            await self.send(value)
            return await self.recv()


    a, b = QueueStream(maxsize), QueueStream(maxsize)
    return Pipe(a, b), Pipe(b, a)


class Component:
    """\
    A task extended with pipes for communication and a lifecycle.
    Pass a coroutine function and arguments to Component;
    upon `start()` the coroutine function will be started as a task with two additional pipe arguments:
    `commands` is for caller-initiated communication; `events` for task-initiated communication.

    There are two lifecycle requirements to the passed coroutine function:
    - when the task starts, `Component.EVENT_START` needs to be sent; and
    - when `Component.COMMAND_STOP` is received, the task should prepare for teardown.
      If the task refuses to stop, it needs to send an event of its choice, otherwise it must terminate.
    """

    class LifecycleError(Exception): pass
    class Success(Exception): pass
    class Failure(Exception): pass
    class EventException(Exception): pass

    EVENT_START = 'EVENT_START'
    COMMAND_STOP = 'COMMAND_STOP'

    def __init__(self, coro_func, *args, **kwargs):
        self._commands, commands = pipe()
        self._events, events = pipe()
        self._coro_func = lambda: self._coro_wrapper(coro_func, *args, commands=commands, events=events, **kwargs)
        self.task = None

    async def _coro_wrapper(self, coro_func, *args, commands, events, **kwargs):
        try:
            result = await coro_func(*args, commands=commands, events=events, **kwargs)
        except Exception as err:
            raise Component.Failure(err) from None
        else:
            raise Component.Success(result)
        finally:
            events.send_nowait(eof=True)

    async def start(self):
        """\
        Start the component. This waits for `Component.EVENT_START` to be sent from the task.
        If the task returns without an event, a `LifecycleError` is raised with a `Success` as its cause.
        If the task raises an exception before any event, that exception is raised.
        If the task sends a different event than `Component.EVENT_START`,
        the task is cancelled and a `LifecycleError` is raised.
        """
        self.task = asyncio.create_task(self._coro_func())
        try:
            start_event = await self.recv_event()
        except Component.Success as succ:
            raise Component.LifecycleError(f"component returned before start finished") from succ
        except Component.Failure as fail:
            # here we don't expect a wrapped result, so we unwrap the failure
            cause, = fail.args
            raise cause from None
        else:
            if start_event != Component.EVENT_START:
                self.task.cancel()
                raise Component.LifecycleError(f"Component must emit EVENT_START, was {start_event}")

    async def stop(self):
        """\
        Stop the component; sends `Component.COMMAND_STOP` and returns `result()`.
        """
        self.send(Component.COMMAND_STOP)
        return await self.result()

    async def result(self):
        """\
        Wait for the task's termination; either the result is returned or a raised exception is reraised.
        If an event is sent before the task terminates, an `EventException` is raised with the event as argument.
        """
        try:
            event = await self.recv_event()
        except Component.Success as succ:
            # success was thrown; return the result
            result, = succ.args
            return result
        except Component.Failure as fail:
            # here we don't expect a wrapped result, so we unwrap the failure
            cause, = fail.args
            raise cause
        else:
            # there was a regular event; shouldn't happen/is exceptional
            raise Component.EventException(event)

    def send(self, value):
        """\
        Sends a command to the task.
        """
        self._commands.send_nowait(value)

    async def recv(self):
        """\
        Receives a command reply from the task.
        """
        return await self._commands.recv()

    async def request(self, value):
        """\
        Sends a command to and receives the reply from the task.
        """
        return await self._commands.request(value)

    async def recv_event(self):
        """\
        Receives an event from the task.
        If the task terminates before another event, an exception is raised.
        A normal return is wrapped in a `Success` exception,
        other exceptions result in a `Failure` with the original exception as the cause.
        """
        try:
            return await self._events.recv()
        except EOFError:
            # component has terminated, raise the cause
            # either Success or a regular error
            self.task.result()
            assert False, f"EOF sent to event queue manually"

    def send_event_reply(self, value):
        """\
        Sends a reply for an event received from the task.
        """
        self._events.send_nowait(value)
