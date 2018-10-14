import asyncio

from .pipe import pipe, EOFError


class Component:
    """\
    A component encapsulates a task with pipes that allow for communication with the task's owner.
    Pass a coroutine function and arguments to Component;
    upon `start()` the coroutine function will be started as a task with two additional pipe arguments:
    `commands` is for owner-initiated communication; `events` for task-initiated communication.
    Replies to commands are sent task-to-owner on the command pipe,
    and replies to events are sent owner-to-task on the event pipe;
    the reserved event `Component.EVENT_START` and command `Component.COMMAND_STOP` do not expect a reply.

    The coroutine passed to Component is required to show the following behavior:
    - it must send `Component.EVENT_START` to its owner after the task is initialized;
    - it must not send EOF on the event pipe;
    - when `Component.COMMAND_STOP` is received, it should either stop eventually, or send an event to its owner;
    - when the task is cancelled, it should either stop eventually, or send an event to its owner.

    The latter two are soft requirements and only rule out the Component running forever
    without ever sending an event after a stop/cancellation request.
    A component may choose to ignore stop commands or cancellations, but should document if it does.
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
            try:
                events.send_nowait(eof=True)
            except EOFError as err:
                raise Component.LifecycleError("component closed events pipe manually") from err

    async def start(self):
        """\
        Start the component. This waits for `Component.EVENT_START` to be sent from the task.
        If the task returns without an event, a `LifecycleError` is raised with a `Success` as its cause.
        If the task raises an exception before any event, that exception is raised.
        If the task sends a different event than `Component.EVENT_START`,
        the task is cancelled (without waiting for the task to shut down) and a `LifecycleError` is raised.
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

    def stop_nowait(self):
        """\
        Stop the component; sends `Component.COMMAND_STOP` to the task.
        Stopping requires the component to receive the command and actively comply with it.
        It is a clean method of shutdown, but requires active cooperation.
        """
        self.send(Component.COMMAND_STOP)

    async def stop(self):
        """\
        Stop the component; calls `stop_nowait()` and returns `result()`.
        """
        self.stop_nowait()
        return await self.result()

    def cancel_nowait(self):
        """\
        Cancel the component.
        Cancelling raises a `CancelledError` into the task, which will normally terminate it.
        It is a forced method of shutdown, and only requires the component to not actively ignore cancellations.
        """
        self.task.cancel()

    async def cancel(self):
        """\
        Cancel the component; calls `cancel_nowait()` and returns `result()`.
        """
        self.cancel_nowait()
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
        self.send(value)
        return await self.recv()

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
            # either Success, Failure, or LifecycleError
            self.task.result()
            assert False  # pragma: nocover

    def send_event_reply(self, value):
        """\
        Sends a reply for an event received from the task.
        """
        self._events.send_nowait(value)
