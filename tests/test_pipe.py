import pytest
import asyncio
from concurrent.futures import ThreadPoolExecutor
import zmq.asyncio

from task_utils.pipe import pipe, ConcurrentPipeEnd, zmq_tcp_pipe, zmq_tcp_pipe_end


@pytest.mark.asyncio
async def test_pipe():
    a, b = pipe()

    # send the reply in the background
    async def reply():
        b.send_nowait(await b.recv() + 1)
        await b.send(await b.recv() + 1)

    asyncio.create_task(reply())
    assert await a.request_sendnowait(1) == 2
    assert await a.request(2) == 3

    with pytest.raises(ValueError):
        await a.send(1, eof=True)

    with pytest.raises(ValueError):
        await a.send()

    a.send_nowait(eof=True)
    with pytest.raises(EOFError):
        a.send_nowait('foo')
    with pytest.raises(EOFError):
        assert await b.recv()
    with pytest.raises(EOFError):
        assert await b.recv()


@pytest.mark.asyncio
async def test_concurrent_pipe():
    loop = asyncio.get_event_loop()
    p = ThreadPoolExecutor()

    a, b = pipe()
    b = ConcurrentPipeEnd(b, loop=loop)

    # send the reply in the background
    async def reply():
        await b.send(await b.recv() + 1)
        await b.send(await b.recv() + 1)

    loop.run_in_executor(p, asyncio.run, reply())
    assert await a.request_sendnowait(1) == 2
    assert await a.request(2) == 3

    with pytest.raises(ValueError):
        await a.send(1, eof=True)

    with pytest.raises(ValueError):
        await a.send()

    a.send_nowait(eof=True)
    with pytest.raises(EOFError):
        a.send_nowait('foo')
    with pytest.raises(EOFError):
        assert await b.recv()
    with pytest.raises(EOFError):
        assert await b.recv()


@pytest.mark.asyncio
async def test_zmq_tcp_pipe():
    ctx = zmq.asyncio.Context()
    a, b = await zmq_tcp_pipe(ctx)

    await b.send("foo")
    assert await a.recv() == "foo"

    await a.send(eof=True)
    with pytest.raises(EOFError):
        await b.recv()
    with pytest.raises(EOFError):
        await b.recv()
    with pytest.raises(EOFError):
        await a.send("bar")

    ctx.destroy()


def test_zmq_tcp_pipe_end_errors():
    ctx = zmq.asyncio.Context()

    with pytest.raises(ValueError):
        zmq_tcp_pipe_end(ctx, 'c')

    with pytest.raises(ValueError):
        zmq_tcp_pipe_end(ctx, 'b')


@pytest.mark.asyncio
async def test_zmq_tcp_pipe_separate():
    async def task():
        ctx = zmq.asyncio.Context()
        b = zmq_tcp_pipe_end(ctx, 'b', port=60123)
        await b.initialize()
        assert await b.recv() == "foo"
        ctx.destroy()

    task = asyncio.create_task(task())

    ctx = zmq.asyncio.Context()
    a = zmq_tcp_pipe_end(ctx, 'a', port=60123)
    await a.initialize()
    await a.send("foo")
    ctx.destroy()

    await task
