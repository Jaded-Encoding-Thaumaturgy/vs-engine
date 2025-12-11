# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2

import contextlib
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from typing import Any

from trio import Cancelled as TrioCancelled
from trio import CancelScope, CapacityLimiter, Event, Nursery, to_thread
from trio.lowlevel import TrioToken, current_trio_token

from vsengine.loops import Cancelled, EventLoop


class TrioEventLoop(EventLoop):
    _scope: Nursery

    def __init__(self, nursery: Nursery, limiter: CapacityLimiter | None = None) -> None:
        if limiter is None:
            limiter = to_thread.current_default_thread_limiter()

        self.nursery = nursery
        self.limiter = limiter
        self._token: TrioToken | None = None

    def attach(self) -> None:
        """
        Called when set_loop is run.
        """
        self._token = current_trio_token()

    def detach(self) -> None:
        """
        Called when another event-loop should take over.
        """
        self.nursery.cancel_scope.cancel()

    def from_thread[T](self, func: Callable[..., T], *args: Any, **kwargs: Any) -> Future[T]:
        """
        Ran from vapoursynth threads to move data to the event loop.
        """
        assert self._token is not None

        fut = Future[T]()

        def _executor() -> None:
            if not fut.set_running_or_notify_cancel():
                return

            try:
                result = func(*args, **kwargs)
            except BaseException as e:
                fut.set_exception(e)
            else:
                fut.set_result(result)

        self._token.run_sync_soon(_executor)
        return fut

    async def to_thread(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:  # type: ignore
        """
        Run this function in a worker thread.
        """
        result = None
        error: BaseException | None = None

        def _executor() -> None:
            nonlocal result, error
            try:
                result = func(*args, **kwargs)
            except BaseException as e:
                error = e

        await to_thread.run_sync(_executor, limiter=self.limiter)

        if error is not None:
            # unreachable?
            assert isinstance(error, BaseException)
            raise error
        else:
            return result

    def next_cycle(self) -> Future[None]:
        scope = CancelScope()
        future = Future[None]()
        TrioEventLoop.to_thread

        def continuation() -> None:
            if scope.cancel_called:
                future.set_exception(Cancelled())
            else:
                future.set_result(None)

        self.from_thread(continuation)
        return future

    async def await_future[T](self, future: Future[T]) -> T:
        """
        Await a concurrent future.

        This function does not need to be implemented if the event-loop
        does not support async and await.
        """
        event = Event()

        result: T | None = None
        error: BaseException | None = None

        def _when_done(_: Future[T]) -> None:
            nonlocal error, result
            if (error := future.exception()) is not None:
                pass
            else:
                result = future.result()
            self.from_thread(event.set)

        future.add_done_callback(_when_done)
        try:
            await event.wait()
        except TrioCancelled:
            raise

        if error is not None:
            with self.wrap_cancelled():
                raise error
        else:
            return result  # type: ignore

    @contextlib.contextmanager
    def wrap_cancelled(self) -> Iterator[None]:
        """
        Wraps vsengine.loops.Cancelled into the native cancellation error.
        """
        try:
            yield
        except Cancelled:
            raise TrioCancelled.__new__(TrioCancelled) from None
