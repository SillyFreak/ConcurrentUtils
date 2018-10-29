Concurrent Utils
================

Concurrency utilities for Python 3.7 + asyncio; the main portion is a component abstraction.
To support this, some pipe implementations for inter-task, inter-thread, and inter-process communication
and some serialization utilities are provided as well. Inter-process communication is based on ZeroMQ.

A "component" is code that is executing on its own, like an asyncio task, a thread, a worker thread's load, or a process.
Components process commands issued by their owner, and create events to be handler by their owner.
Components may also produce a result, and of course may communicate with other entities than their owner.

Although asyncio is used heavily, the connection between a workload and its owner decouples the two
to allow for any model of concurrency.
Here is an example, taken and adapted from the test suite::

    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    from concurrent_utils.component import Component, component_workload, start_component_in_thread


    async def test_thread_component_result_success_and_command():
        @component_workload
        async def component(x, *, commands, events):
            await events.send(Component.EVENT_START)
            ### startup complete

            # reply to command
            await commands.send(await commands.recv() + 1)

            # return
            return x

        e = ThreadPoolExecutor(1)
        comp = await start_component_in_thread(e, component, 1)

        assert await comp.request(1) == 2

        assert await comp.result() == 1


    asyncio.run(test_thread_component_result_success_and_command())

Although the component is defined as a coroutine, it is (in this case) not executed on the owner's event loop.
The same component could, without modification, be run on the owner's event loop,
or, with minor modifications to make the ``component`` function pickle-able, be run in a worker process instead.
