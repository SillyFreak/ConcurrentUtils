Concurrent Utils
===

Concurrency utilities; the main portion is a component abstraction.
To support this, some pipe implementation for inter-task, inter-thread, and inter-process communication
and some serialization utilities are provided as well.

A "component" is code that is executing on its own, like an asyncio task, a thread, a worker thread's load, or a process.
Components process commands issued by their owner, and create events to be handler by their owner.
Components may also produce a result, and of course may communicate with other entities than their owner.
