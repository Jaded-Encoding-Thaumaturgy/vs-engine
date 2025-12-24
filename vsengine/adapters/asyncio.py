# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2

import asyncio
import contextlib
import contextvars
from collections.abc import Callable, Iterator
from concurrent.futures import Future

from vsengine.loops import Cancelled, EventLoop


class AsyncIOLoop(EventLoop):
    """
    Bridges vs-engine to AsyncIO.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop

    def from_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        future = Future[R]()

        ctx = contextvars.copy_context()

        def _wrap() -> None:
            if not future.set_running_or_notify_cancel():
                return

            try:
                result = ctx.run(func, *args, **kwargs)
            except BaseException as e:
                future.set_exception(e)
            else:
                future.set_result(result)

        self.loop.call_soon_threadsafe(_wrap)
        return future

    def to_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        ctx = contextvars.copy_context()
        future = Future[R]()

        def _wrap() -> R:
            return ctx.run(func, *args, **kwargs)

        async def _run() -> None:
            try:
                result = await asyncio.to_thread(_wrap)
            except BaseException as e:
                future.set_exception(e)
            else:
                future.set_result(result)

        self.loop.create_task(_run())
        return future

    def next_cycle(self) -> Future[None]:
        future = Future[None]()
        task = asyncio.current_task()

        def continuation() -> None:
            if task is None or not task.cancelled():
                future.set_result(None)
            else:
                future.set_exception(Cancelled())

        self.loop.call_soon(continuation)
        return future

    async def await_future[T](self, future: Future[T]) -> T:
        with self.wrap_cancelled():
            return await asyncio.wrap_future(future, loop=self.loop)

    @contextlib.contextmanager
    def wrap_cancelled(self) -> Iterator[None]:
        try:
            yield
        except Cancelled:
            raise asyncio.CancelledError() from None
