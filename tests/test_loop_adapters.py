# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""Tests for event loop adapters."""

import asyncio
import contextlib
import threading
from collections.abc import Generator, Iterator
from concurrent.futures import CancelledError, Future
from typing import Any

import pytest

from vsengine.adapters.asyncio import AsyncIOLoop
from vsengine.loops import NO_LOOP, Cancelled, EventLoop, _NoEventLoop, make_awaitable, set_loop


def make_async(func: Any) -> Any:
    """Decorator to run a generator-based test within a loop."""

    def _wrapped(self: AdapterTest, *args: Any, **kwargs: Any) -> Any:
        return self.run_within_loop(func, args, kwargs)

    return _wrapped


def is_async(func: Any) -> Any:
    """Decorator to run an async test within a loop."""

    def _wrapped(self: "AsyncAdapterTest", *args: Any, **kwargs: Any) -> Any:
        return self.run_within_loop_async(func, args, kwargs)

    return _wrapped


class AdapterTest:
    """Base class for event loop adapter tests."""

    @contextlib.contextmanager
    def with_loop(self) -> Iterator[EventLoop]:
        loop = self.make_loop()
        set_loop(loop)
        try:
            yield loop
        finally:
            set_loop(NO_LOOP)

    def make_loop(self) -> EventLoop:
        raise NotImplementedError

    def run_within_loop(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        raise NotImplementedError

    def resolve_to_thread_future(self, fut: Any) -> Generator[Any, None, Any]:
        raise NotImplementedError

    @contextlib.contextmanager
    def assert_cancelled(self) -> Iterator[None]:
        raise NotImplementedError

    @make_async
    def test_wrap_cancelled_without_cancellation(self) -> None:
        with self.with_loop() as loop, loop.wrap_cancelled():
            pass

    @make_async
    def test_wrap_cancelled_with_cancellation(self) -> Iterator[None]:
        with self.with_loop() as loop, self.assert_cancelled(), loop.wrap_cancelled():
            raise Cancelled

    @make_async
    def test_wrap_cancelled_with_other_exception(self) -> Iterator[None]:
        with self.with_loop() as loop, pytest.raises(RuntimeError), loop.wrap_cancelled():
            raise RuntimeError()
        yield

    @make_async
    def test_next_cycle_doesnt_throw_when_not_cancelled(self) -> Iterator[None]:
        with self.with_loop() as loop:
            fut = loop.next_cycle()
            yield
            assert fut.done()
            assert fut.result() is None

    @make_async
    def test_from_thread_with_success(self) -> Iterator[None]:
        def test_func() -> "AdapterTest":
            return self

        with self.with_loop() as loop:
            fut = loop.from_thread(test_func)
            yield
            assert fut.result(timeout=0.5) is self

    @make_async
    def test_from_thread_with_failure(self) -> Iterator[None]:
        def test_func() -> None:
            raise RuntimeError

        with self.with_loop() as loop:
            fut = loop.from_thread(test_func)
            yield
            with pytest.raises(RuntimeError):
                fut.result(timeout=0.5)

    @make_async
    def test_from_thread_forwards_correctly(self) -> Iterator[None]:
        a: tuple[Any, ...] | None = None
        k: dict[str, Any] | None = None

        def test_func(*args: Any, **kwargs: Any) -> None:
            nonlocal a, k
            a = args
            k = kwargs

        with self.with_loop() as loop:
            fut = loop.from_thread(test_func, 1, 2, 3, a="b", c="d")
            yield
            fut.result(timeout=0.5)
            assert a == (1, 2, 3)
            assert k == {"a": "b", "c": "d"}

    @make_async
    def test_to_thread_spawns_a_new_thread(self) -> Iterator[None]:
        def test_func() -> threading.Thread:
            return threading.current_thread()

        with self.with_loop() as loop:
            t2 = yield from self.resolve_to_thread_future(loop.to_thread(test_func))
            assert threading.current_thread() != t2

    @make_async
    def test_to_thread_runs_inline_with_failure(self) -> Iterator[None]:
        def test_func() -> None:
            raise RuntimeError

        with self.with_loop() as loop, pytest.raises(RuntimeError):
            yield from self.resolve_to_thread_future(loop.to_thread(test_func))

    @make_async
    def test_to_thread_forwards_correctly(self) -> Iterator[None]:
        a: tuple[Any, ...] | None = None
        k: dict[str, Any] | None = None

        def test_func(*args: Any, **kwargs: Any) -> None:
            nonlocal a, k
            a = args
            k = kwargs

        with self.with_loop() as loop:
            yield from self.resolve_to_thread_future(loop.to_thread(test_func, 1, 2, 3, a="b", c="d"))
            assert a == (1, 2, 3)
            assert k == {"a": "b", "c": "d"}


class AsyncAdapterTest(AdapterTest):
    """Base class for async event loop adapter tests."""

    def run_within_loop(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        async def wrapped(_: Any) -> None:
            result = func(self, *args, **kwargs)
            if hasattr(result, "__iter__"):
                for _ in result:
                    await self.next_cycle()

        self.run_within_loop_async(wrapped, (), {})

    def run_within_loop_async(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        raise NotImplementedError

    async def wait_for(self, coro: Any, timeout: float) -> Any:
        raise NotImplementedError

    async def next_cycle(self) -> None:
        pass

    @is_async
    async def test_await_future_success(self) -> None:
        with self.with_loop() as loop:
            fut: Future[int] = Future()

            def _setter() -> None:
                fut.set_result(1)

            threading.Thread(target=_setter).start()
            assert await self.wait_for(loop.await_future(fut), 0.5) == 1

    @is_async
    async def test_await_future_failure(self) -> None:
        with self.with_loop() as loop:
            fut: Future[int] = Future()

            def _setter() -> None:
                fut.set_exception(RuntimeError())

            threading.Thread(target=_setter).start()
            with pytest.raises(RuntimeError):
                await self.wait_for(loop.await_future(fut), 0.5)


class TestNoLoop(AdapterTest):
    """Tests for the no-event-loop adapter."""

    def make_loop(self) -> EventLoop:
        return _NoEventLoop()

    def run_within_loop(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        result = func(self, *args, **kwargs)
        if hasattr(result, "__iter__"):
            for _ in result:
                pass

    @contextlib.contextmanager
    def assert_cancelled(self) -> Iterator[None]:
        with pytest.raises(CancelledError):
            yield

    def resolve_to_thread_future(self, fut: Future[Any]) -> Generator[None, None, Any]:
        return fut.result(timeout=0.5)
        yield  # type: ignore[unreachable]


class TestAsyncIO(AsyncAdapterTest):
    """Tests for the asyncio event loop adapter."""

    def make_loop(self) -> AsyncIOLoop:
        return AsyncIOLoop()

    def run_within_loop_async(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        async def wrapped() -> None:
            await func(self, *args, **kwargs)

        asyncio.run(wrapped())

    async def next_cycle(self) -> None:
        await asyncio.sleep(0.01)

    async def wait_for(self, coro: Any, timeout: float) -> Any:
        return await asyncio.wait_for(coro, timeout)

    @contextlib.contextmanager
    def assert_cancelled(self) -> Iterator[None]:
        with pytest.raises(asyncio.CancelledError):
            yield

    def resolve_to_thread_future(self, fut: Any) -> Generator[None, None, Any]:
        while not fut.done():
            yield
        return fut.result()


try:
    import trio
except ImportError:
    print("Skipping trio tests")
else:
    from vsengine.adapters.trio import TrioEventLoop

    class TestTrio(AsyncAdapterTest):
        """Tests for the trio event loop adapter."""

        nursery: trio.Nursery

        def make_loop(self) -> TrioEventLoop:
            return TrioEventLoop(self.nursery)

        async def next_cycle(self) -> None:
            await trio.sleep(0.01)

        def run_within_loop_async(self, func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
            async def wrapped() -> None:
                async with trio.open_nursery() as nursery:
                    self.nursery = nursery
                    await func(self, *args, **kwargs)

            trio.run(wrapped)

        def resolve_to_thread_future(self, fut: Any) -> Generator[None, None, Any]:
            done = False
            result: Any = None
            error: BaseException | None = None

            async def _awaiter() -> None:
                nonlocal done, error, result
                try:
                    result = await make_awaitable(fut)
                except BaseException as e:
                    error = e
                finally:
                    done = True

            self.nursery.start_soon(_awaiter)

            while not done:
                yield

            if error is not None:
                raise error
            else:
                return result

        async def wait_for(self, coro: Any, timeout: float) -> Any:
            with trio.fail_after(timeout):
                return await coro

        @contextlib.contextmanager
        def assert_cancelled(self) -> Iterator[None]:
            with pytest.raises(trio.Cancelled):
                yield
