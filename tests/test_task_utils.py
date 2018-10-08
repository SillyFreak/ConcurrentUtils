import pytest

from task_utils import pipe, Component


@pytest.mark.asyncio
async def test_pipe():
    a, b = pipe()

    a.send_nowait(1)
    assert await b.recv() == 1
    await b.send(2)
    assert await a.recv() == 2


@pytest.mark.asyncio
async def test_component_start_returns():
    async def component(x, *, commands, events):
        return x

    comp = Component(component, 1)
    with pytest.raises(Component.LifecycleError) as exc_info:
        await comp.start()

    result, = exc_info.value.__cause__.args
    assert result == 1


@pytest.mark.asyncio
async def test_component_start_raises():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        raise Fail

    comp = Component(component, 1)
    with pytest.raises(Fail) as exc_info:
        await comp.start()


@pytest.mark.asyncio
async def test_component_result_success():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        return x

    comp = Component(component, 1)
    await comp.start()
    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_result_exception():
    class Fail(Exception): pass

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        raise Fail

    comp = Component(component, 1)
    await comp.start()
    with pytest.raises(Fail):
        await comp.result()


@pytest.mark.asyncio
async def test_component_result_event():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        events.send_nowait('EVENT')
        return x

    comp = Component(component, 1)
    await comp.start()
    with pytest.raises(Component.EventException):
        await comp.result()
    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_stop_success():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
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
        command = await commands.recv()
        assert command == Component.COMMAND_STOP
        raise Fail

    comp = Component(component, 1)
    await comp.start()
    with pytest.raises(Fail):
        await comp.stop()


@pytest.mark.asyncio
async def test_component_stop_event():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        events.send_nowait('EVENT')
        return x

    comp = Component(component, 1)
    await comp.start()
    with pytest.raises(Component.EventException):
        await comp.stop()
    assert await comp.stop() == 1


@pytest.mark.asyncio
async def test_component_recv_event():
    EVENT = 'EVENT'

    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
        events.send_nowait(EVENT)
        return x

    comp = Component(component, 1)
    await comp.start()
    assert await comp.recv_event() == EVENT
    assert await comp.result() == 1


@pytest.mark.asyncio
async def test_component_recv_event_return():
    async def component(x, *, commands, events):
        events.send_nowait(Component.EVENT_START)
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
        raise Fail

    comp = Component(component, 1)
    await comp.start()
    with pytest.raises(Component.Failure) as exc_info:
        await comp.recv_event()

    fail, = exc_info.value.args
    with pytest.raises(Fail):
        raise fail
