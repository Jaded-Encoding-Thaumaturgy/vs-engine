# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""Tests for the unified future system."""

import contextlib
import threading
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import Future
from typing import Any

import pytest

from vsengine._futures import UnifiedFuture, UnifiedIterator, unified
from vsengine.adapters.asyncio import AsyncIOLoop
from vsengine.loops import NO_LOOP, set_loop


def resolve(value: Any) -> Future[Any]:
    fut = Future[Any]()
    fut.set_result(value)
    return fut


def reject(err: BaseException) -> Future[Any]:
    fut = Future[Any]()
    fut.set_exception(err)
    return fut


def contextmanager_helper() -> Future[Any]:
    @contextlib.contextmanager
    def noop() -> Iterator[int]:
        yield 1

    return resolve(noop())


def asynccontextmanager_helper() -> Future[Any]:
    @contextlib.asynccontextmanager
    async def noop() -> AsyncIterator[int]:
        yield 2

    return resolve(noop())


def succeeds() -> Future[int]:
    return resolve(1)


def fails() -> Future[Any]:
    return reject(RuntimeError())


def fails_early() -> Future[Any]:
    raise RuntimeError()


def future_iterator() -> Iterator[Future[int]]:
    n = 0
    while True:
        yield resolve(n)
        n += 1


class WrappedUnifiedFuture(UnifiedFuture[Any]): ...


class WrappedUnifiedIterable(UnifiedIterator[Any]): ...


# UnifiedFuture tests


@pytest.mark.asyncio
async def test_unified_future_is_await() -> None:
    set_loop(AsyncIOLoop())
    await UnifiedFuture.from_call(succeeds)


@pytest.mark.asyncio
async def test_unified_future_awaitable() -> None:
    set_loop(AsyncIOLoop())
    await UnifiedFuture.from_call(succeeds).awaitable()


@pytest.mark.asyncio
async def test_unified_future_async_context_manager_async() -> None:
    set_loop(AsyncIOLoop())
    async with UnifiedFuture.from_call(asynccontextmanager_helper) as v:
        assert v == 2


@pytest.mark.asyncio
async def test_unified_future_context_manager_async() -> None:
    set_loop(AsyncIOLoop())
    async with UnifiedFuture.from_call(contextmanager_helper) as v:
        assert v == 1


def test_unified_future_context_manager() -> None:
    with UnifiedFuture.from_call(contextmanager_helper) as v:
        assert v == 1


def test_unified_future_map() -> None:
    def _crash(v: Any) -> str:
        raise RuntimeError(str(v))

    future = UnifiedFuture.from_call(succeeds)
    new_future = future.map(lambda v: str(v))
    assert new_future.result() == "1"

    new_future = future.map(_crash)
    assert isinstance(new_future.exception(), RuntimeError)

    future = UnifiedFuture.from_call(fails)
    new_future = future.map(lambda v: str(v))
    assert isinstance(new_future.exception(), RuntimeError)


def test_unified_future_catch() -> None:
    def _crash(_: BaseException) -> str:
        raise RuntimeError("test")

    future = UnifiedFuture.from_call(fails)
    new_future = future.catch(lambda e: e.__class__.__name__)
    assert new_future.result() == "RuntimeError"

    new_future = future.catch(_crash)
    assert isinstance(new_future.exception(), RuntimeError)

    future = UnifiedFuture.from_call(succeeds)
    new_future = future.catch(lambda v: str(v))
    # Result is 1 because the future succeeded (no exception to catch)
    result = new_future.result()
    assert result == 1


@pytest.mark.asyncio
async def test_unified_future_add_loop_callback() -> None:
    from vsengine.adapters.asyncio import AsyncIOLoop
    from vsengine.loops import set_loop

    set_loop(AsyncIOLoop())

    def _init_thread(fut: Future[threading.Thread]) -> None:
        fut.set_result(threading.current_thread())

    fut: Future[threading.Thread] = Future()
    thr = threading.Thread(target=lambda: _init_thread(fut))

    def _wrapper() -> Future[threading.Thread]:
        return fut

    unified_fut = UnifiedFuture.from_call(_wrapper)

    loop_thread: threading.Thread | None = None

    def _record_loop_thr(_: Any) -> None:
        nonlocal loop_thread
        loop_thread = threading.current_thread()

    unified_fut.add_loop_callback(_record_loop_thr)
    thr.start()
    cb_thread = await unified_fut

    assert cb_thread != loop_thread


# UnifiedIterator tests


def test_unified_iterator_run_as_completed_succeeds() -> None:
    set_loop(NO_LOOP)
    my_futures: list[Future[int]] = [Future(), Future()]
    results: list[int] = []

    def _add_to_result(f: Future[int]) -> None:
        results.append(f.result())

    state = UnifiedIterator(iter(my_futures)).run_as_completed(_add_to_result)
    assert not state.done()
    my_futures[1].set_result(2)
    assert not state.done()
    my_futures[0].set_result(1)
    assert state.done()
    assert state.result() is None
    assert results == [1, 2]


def test_unified_iterator_run_as_completed_forwards_errors() -> None:
    set_loop(NO_LOOP)
    my_futures: list[Future[int]] = [Future(), Future()]
    results: list[int] = []
    errors: list[BaseException] = []

    def _add_to_result(f: Future[int]) -> None:
        if exc := f.exception():
            errors.append(exc)
        else:
            results.append(f.result())

    iterator = iter(my_futures)
    state = UnifiedIterator(iterator).run_as_completed(_add_to_result)
    assert not state.done()
    my_futures[0].set_exception(RuntimeError())
    assert not state.done()
    my_futures[1].set_result(2)
    assert state.done()
    assert state.result() is None

    assert results == [2]
    assert len(errors) == 1


def test_unified_iterator_run_as_completed_cancels() -> None:
    set_loop(NO_LOOP)
    my_futures: list[Future[int]] = [Future(), Future()]
    results: list[int] = []

    def _add_to_result(f: Future[int]) -> bool:
        results.append(f.result())
        return False

    iterator = iter(my_futures)
    state = UnifiedIterator(iterator).run_as_completed(_add_to_result)
    assert not state.done()
    my_futures[0].set_result(1)
    assert state.done()
    assert state.result() is None
    assert results == [1]


def test_unified_iterator_run_as_completed_cancels_on_crash() -> None:
    set_loop(NO_LOOP)
    my_futures: list[Future[int]] = [Future(), Future()]
    err = RuntimeError("test")

    def _crash(_: Future[int]) -> None:
        raise err

    iterator = iter(my_futures)
    state = UnifiedIterator(iterator).run_as_completed(_crash)
    assert not state.done()
    my_futures[0].set_result(1)
    assert state.done()
    assert state.exception() is err
    assert next(iterator) is not None


def test_unified_iterator_run_as_completed_requests_as_needed() -> None:
    my_futures: list[Future[int]] = [Future(), Future()]
    requested: list[Future[int]] = []
    continued: list[Future[int]] = []

    def _add_to_result(f: Future[int]) -> None:
        pass

    def _it() -> Iterator[Future[int]]:
        for fut in my_futures:
            requested.append(fut)
            yield fut
            continued.append(fut)

    state = UnifiedIterator(_it()).run_as_completed(_add_to_result)
    assert not state.done()
    assert requested == [my_futures[0]]
    assert continued == []

    my_futures[0].set_result(1)
    assert not state.done()
    assert requested == [my_futures[0], my_futures[1]]
    assert continued == [my_futures[0]]

    my_futures[1].set_result(1)
    assert state.done()
    assert requested == [my_futures[0], my_futures[1]]
    assert continued == [my_futures[0], my_futures[1]]


def test_unified_iterator_run_as_completed_cancels_on_iterator_crash() -> None:
    err = RuntimeError("test")

    def _it() -> Iterator[Future[int]]:
        if False:
            yield Future[int]()  # type:ignore[unreachable]
        raise err

    def _noop(_: Future[int]) -> None:
        pass

    state = UnifiedIterator(_it()).run_as_completed(_noop)
    assert state.done()
    assert state.exception() is err


def test_unified_iterator_can_iter_futures() -> None:
    n = 0
    for fut in UnifiedIterator.from_call(future_iterator).futures:
        assert n == fut.result()
        n += 1
        if n > 100:
            break


def test_unified_iterator_can_iter() -> None:
    n = 0
    for n2 in UnifiedIterator.from_call(future_iterator):
        assert n == n2
        n += 1
        if n > 100:
            break


@pytest.mark.asyncio
async def test_unified_iterator_can_aiter() -> None:
    set_loop(AsyncIOLoop())
    n = 0
    async for n2 in UnifiedIterator.from_call(future_iterator):
        assert n == n2
        n += 1
        if n > 100:
            break


# unified decorator tests


def test_unified_auto_future_return_a_unified_future() -> None:
    @unified()
    def test_func() -> Future[int]:
        return resolve(9999)

    f = test_func()
    assert isinstance(f, UnifiedFuture)
    assert f.result() == 9999


def test_unified_auto_generator_return_a_unified_iterable() -> None:
    @unified()
    def test_func() -> Iterator[Future[int]]:
        yield resolve(1)
        yield resolve(2)

    f = test_func()
    assert isinstance(f, UnifiedIterator)
    assert next(f) == 1
    assert next(f) == 2


def test_unified_generator_accepts_other_iterables() -> None:
    @unified(kind="generator")
    def test_func() -> Iterator[Future[int]]:
        return iter((resolve(1), resolve(2)))

    f = test_func()
    assert isinstance(f, UnifiedIterator)
    assert next(f) == 1
    assert next(f) == 2


def test_unified_custom_future() -> None:
    @unified(future_class=WrappedUnifiedFuture)
    def test_func() -> Future[int]:
        return resolve(9999)

    f = test_func()
    assert isinstance(f, WrappedUnifiedFuture)


def test_unified_custom_generator() -> None:
    @unified(iterable_class=WrappedUnifiedIterable)
    def test_func() -> Iterator[Future[int]]:
        yield resolve(9999)

    f = test_func()
    assert isinstance(f, WrappedUnifiedIterable)
