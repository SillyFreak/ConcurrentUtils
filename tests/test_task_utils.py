import pytest
import asyncio

from task_utils import pipe, EOFError, Component


@pytest.mark.asyncio
async def test_pipe():
    a, b = pipe()

    a.send_nowait(1)
    assert await b.recv() == 1

    await b.send(2)
    assert await a.recv() == 2

    a.send_nowait(eof=True)
    with pytest.raises(EOFError):
        a.send_nowait('foo')
    with pytest.raises(EOFError):
        assert await b.recv()
    with pytest.raises(EOFError):
        assert await b.recv()


@pytest.mark.asyncio
async def test_component_start_return():
    async def component(x, *, commands, events):
        # return before START
        return x

    comp = Component(component, 1)
    with pytest.raises(Component.LifecycleError) as exc_info:
        await comp.start()
    result, = exc_info.value.__cause__.args
    assert result == 1


@pytest.mark.asyncio
async def test_component_start_raise():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        # raise before START
        raise Fail

    comp = Component(component, 1)
    with pytest.raises(Fail) as exc_info:
        await comp.start()


@pytest.mark.asyncio
async def test_component_start_event():
    EVENT = 'EVENT'

    flag = False

    async def component(x, *, commands, events):
        nonlocal flag

        # send WRONG event
        events.send_nowait(EVENT)
        await asyncio.sleep(0.05)
        flag = True


    comp = Component(component, 1)
    with pytest.raises(Component.LifecycleError):
        await comp.start()

    await asyncio.sleep(0.1)
    assert not flag

    with pytest.raises(Component.Failure) as exc_info:
        await comp.task
    exc, = exc_info.value.args
    with pytest.raises(asyncio.CancelledError):
        raise exc


@pytest.mark.asyncio
async def test_component_result_success():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # return
        return x

    comp = Component(component, 1)
    await comp.start()

    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_result_exception():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # raise
        raise Fail

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Fail):
        await comp.result()


@pytest.mark.asyncio
async def test_component_result_event():
    EVENT = 'EVENT'

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # send event
        events.send_nowait(EVENT)

        # return
        return x

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Component.EventException) as exc_info:
        await comp.result()
    event, = exc_info.value.args
    assert event == EVENT

    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_stop_success():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # receive STOP and return
        command = await commands.recv()
        assert command == Component.COMMAND_STOP
        return x

    comp = Component(component, 1)
    await comp.start()

    assert await comp.stop() == 1


@pytest.mark.asyncio
async def test_component_stop_exception():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # receive STOP and raise
        command = await commands.recv()
        assert command == Component.COMMAND_STOP
        raise Fail

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Fail):
        await comp.stop()


@pytest.mark.asyncio
async def test_component_stop_event():
    EVENT = 'EVENT'

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # receive STOP and send EVENT
        command = await commands.recv()
        assert command == Component.COMMAND_STOP
        events.send_nowait(EVENT)

        # receive STOP and return
        command = await commands.recv()
        assert command == Component.COMMAND_STOP
        return x

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Component.EventException) as exc_info:
        await comp.stop()
    event, = exc_info.value.args
    assert event == EVENT

    assert await comp.stop() == 1


@pytest.mark.asyncio
async def test_component_cancel_cancels():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # wait for cancellation
        await asyncio.sleep(1)

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(asyncio.CancelledError):
        await comp.cancel()


@pytest.mark.asyncio
async def test_component_cancel_success():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # wait for cancellation and return
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(1)
        return x

    comp = Component(component, 1)
    await comp.start()

    assert await comp.cancel() == 1


@pytest.mark.asyncio
async def test_component_cancel_exception():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # wait for cancellation and raise
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(1)
        raise Fail

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Fail):
        await comp.cancel()


@pytest.mark.asyncio
async def test_component_cancel_event():
    EVENT = 'EVENT'

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # wait for cancellation and send EVENT
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(1)
        events.send_nowait(EVENT)

        # wait for cancellation and return
        with pytest.raises(asyncio.CancelledError):
            await asyncio.sleep(1)
        return x

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Component.EventException) as exc_info:
        await comp.cancel()
    event, = exc_info.value.args
    assert event == EVENT

    assert await comp.cancel() == 1


@pytest.mark.asyncio
async def test_component_recv_event():
    EVENT = 'EVENT'

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # send event
        events.send_nowait(EVENT)

        # return
        return x

    comp = Component(component, 1)
    await comp.start()

    assert await comp.recv_event() == EVENT

    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_recv_event_return():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # return
        return x

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Component.Success) as exc_info:
        await comp.recv_event()
    result, = exc_info.value.args
    assert result == 1


@pytest.mark.asyncio
async def test_component_recv_event_raise():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # raise
        raise Fail

    comp = Component(component, 1)
    await comp.start()

    with pytest.raises(Component.Failure) as exc_info:
        await comp.recv_event()
    fail, = exc_info.value.args
    with pytest.raises(Fail):
        raise fail
