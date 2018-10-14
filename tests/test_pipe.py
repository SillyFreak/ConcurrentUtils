import pytest
import asyncio

from task_utils.pipe import pipe, EOFError


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
