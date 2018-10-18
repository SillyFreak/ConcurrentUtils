import pytest
import asyncio

from task_utils import Component, start_component


@pytest.mark.asyncio
async def test_component_start_return():
    async def component(x, *, commands, events):
        # return before START
        return x

    with pytest.raises(Component.LifecycleError) as exc_info:
        await start_component(component, 1)
    result, = exc_info.value.__cause__.args
    assert result == 1


@pytest.mark.asyncio
async def test_component_start_raise():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        # raise before START
        raise Fail

    with pytest.raises(Fail) as exc_info:
        await start_component(component, 1)


@pytest.mark.asyncio
async def test_component_start_event():
    EVENT = 'EVENT'

    finished = False
    cancelled = False

    async def component(x, *, commands, events):
        nonlocal finished, cancelled

        try:
            # send WRONG event
            events.send_nowait(EVENT)
            await asyncio.sleep(0.05)
            finished = True
        except asyncio.CancelledError:
            cancelled = True
            raise


    with pytest.raises(Component.LifecycleError):
        await start_component(component, 1)

    await asyncio.sleep(0.1)
    assert not finished
    assert cancelled


@pytest.mark.asyncio
async def test_component_result_success_and_command():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # reply to command
        commands.send_nowait(await commands.recv() + 1)

        # return
        return x

    comp = await start_component(component, 1)

    assert await comp.request(1) == 2

    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_result_exception():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # raise
        raise Fail

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

    with pytest.raises(Component.EventException) as exc_info:
        await comp.cancel()
    event, = exc_info.value.args
    assert event == EVENT

    assert await comp.cancel() == 1


@pytest.mark.asyncio
async def test_component_recv_event_and_reply():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # send event
        assert await events.request_sendnowait(1) == 2

        # return
        return x

    comp = await start_component(component, 1)

    assert await comp.recv_event() == 1
    comp.send_event_reply(2)

    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_recv_event_return():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        # return
        return x

    comp = await start_component(component, 1)

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

    comp = await start_component(component, 1)

    with pytest.raises(Component.Failure) as exc_info:
        await comp.recv_event()
    fail, = exc_info.value.args
    with pytest.raises(Fail):
        raise fail


@pytest.mark.asyncio
async def test_component_manual_eof_event():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        ### startup complete

        events.send_nowait(eof=True)

    comp = await start_component(component, 1)

    with pytest.raises(Component.LifecycleError):
        await comp.recv_event()

    with pytest.raises(Component.LifecycleError):
        await comp.result()

    with pytest.raises(Component.LifecycleError):
        await comp.stop()
