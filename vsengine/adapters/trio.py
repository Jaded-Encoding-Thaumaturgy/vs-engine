# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2

import contextlib
from collections.abc import Callable, Iterator
from concurrent.futures import Future

import trio

from vsengine.loops import Cancelled, EventLoop


class TrioEventLoop(EventLoop):
    """
    Bridges vs-engine to Trio.
    """

    def __init__(self, nursery: trio.Nursery, limiter: trio.CapacityLimiter | None = None) -> None:
        if limiter is None:
            limiter = trio.to_thread.current_default_thread_limiter()

        self.nursery = nursery
        self.limiter = limiter
        self._token: trio.lowlevel.TrioToken | None = None

    def attach(self) -> None:
        self._token = trio.lowlevel.current_trio_token()

    def detach(self) -> None:
        self.nursery.cancel_scope.cancel()

    def from_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        assert self._token is not None

        fut = Future[R]()

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

    def to_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        future = Future[R]()

        async def _run() -> None:
            def _executor() -> None:
                try:
                    result = func(*args, **kwargs)
                    future.set_result(result)
                except BaseException as e:
                    future.set_exception(e)

            await trio.to_thread.run_sync(_executor, limiter=self.limiter)

        self.nursery.start_soon(_run)
        return future

    def next_cycle(self) -> Future[None]:
        scope = trio.CancelScope()
        future = Future[None]()

        def continuation() -> None:
            if scope.cancel_called:
                future.set_exception(Cancelled())
            else:
                future.set_result(None)

        self.from_thread(continuation)
        return future

    async def await_future[T](self, future: Future[T]) -> T:
        event = trio.Event()

        def _when_done(_: Future[T]) -> None:
            self.from_thread(event.set)

        future.add_done_callback(_when_done)

        try:
            await event.wait()
        except trio.Cancelled:
            raise

        try:
            return future.result()
        except BaseException as exc:
            with self.wrap_cancelled():
                raise exc

    @contextlib.contextmanager
    def wrap_cancelled(self) -> Iterator[None]:
        try:
            yield
        except Cancelled:
            raise trio.Cancelled.__new__(trio.Cancelled) from None
