# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Iterator
from concurrent.futures import Future
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import isgeneratorfunction
from types import TracebackType
from typing import Any, Literal, Self, overload

from vsengine.loops import Cancelled, get_loop, keep_environment


class UnifiedFuture[T](Future[T], AbstractContextManager[T, Any], AbstractAsyncContextManager[T, Any], Awaitable[T]):
    @classmethod
    def from_call[**P](cls, func: Callable[P, Future[T]], *args: P.args, **kwargs: P.kwargs) -> Self:
        try:
            future = func(*args, **kwargs)
        except Exception as e:
            return cls.reject(e)

        return cls.from_future(future)

    @classmethod
    def from_future(cls, future: Future[T]) -> Self:
        if isinstance(future, cls):
            return future

        result = cls()

        def _receive(fn: Future[T]) -> None:
            if (exc := future.exception()) is not None:
                result.set_exception(exc)
            else:
                result.set_result(future.result())

        future.add_done_callback(_receive)
        return result

    @classmethod
    def resolve(cls, value: T) -> Self:
        future = cls()
        future.set_result(value)
        return future

    @classmethod
    def reject(cls, error: BaseException) -> Self:
        future = cls()
        future.set_exception(error)
        return future

    # Adding callbacks
    def add_done_callback(self, fn: Callable[[Future[T]], Any]) -> None:
        # The done_callback should inherit the environment of the current call.
        super().add_done_callback(keep_environment(fn))

    def add_loop_callback(self, func: Callable[[Future[T]], None]) -> None:
        def _wrapper(future: Future[T]) -> None:
            get_loop().from_thread(func, future)

        self.add_done_callback(_wrapper)

    # Manipulating futures
    @overload
    def then[V](self, success_cb: Callable[[T], V], err_cb: None) -> UnifiedFuture[V]: ...
    @overload
    def then[V](self, success_cb: None, err_cb: Callable[[BaseException], V]) -> UnifiedFuture[T | V]: ...
    def then[V](
        self, success_cb: Callable[[T], V] | None, err_cb: Callable[[BaseException], V] | None
    ) -> UnifiedFuture[V] | UnifiedFuture[T | V]:
        result = UnifiedFuture[T | V]()

        def _run_cb(cb: Callable[[Any], V], v: Any) -> None:
            try:
                r = cb(v)
            except BaseException as e:
                result.set_exception(e)
            else:
                result.set_result(r)

        def _done(fn: Future[T]) -> None:
            if (exc := self.exception()) is not None:
                if err_cb is not None:
                    _run_cb(err_cb, exc)
                else:
                    result.set_exception(exc)
            else:
                if success_cb is not None:
                    _run_cb(success_cb, self.result())
                else:
                    result.set_result(self.result())

        self.add_done_callback(_done)
        return result

    def map[V](self, cb: Callable[[T], V]) -> UnifiedFuture[V]:
        return self.then(cb, None)

    def catch[V](self, cb: Callable[[BaseException], V]) -> UnifiedFuture[T | V]:
        return self.then(None, cb)

    # Nicer Syntax
    def __enter__(self) -> T:
        obj = self.result()

        if isinstance(obj, AbstractContextManager):
            return obj.__enter__()

        raise NotImplementedError("(async) with is not implemented for this object")

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        obj = self.result()

        if isinstance(obj, AbstractContextManager):
            return obj.__exit__(exc, val, tb)

        raise NotImplementedError("(async) with is not implemented for this object")

    async def awaitable(self) -> T:
        return await get_loop().await_future(self)

    def __await__(self) -> Generator[Any, None, T]:
        return self.awaitable().__await__()

    async def __aenter__(self) -> T:
        result = await self.awaitable()

        if isinstance(result, AbstractAsyncContextManager):
            return await result.__aenter__()
        if isinstance(result, AbstractContextManager):
            return result.__enter__()

        raise NotImplementedError("(async) with is not implemented for this object")

    async def __aexit__(
        self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None
    ) -> None:
        result = await self.awaitable()

        if isinstance(result, AbstractAsyncContextManager):
            return await result.__aexit__(exc, val, tb)
        if isinstance(result, AbstractContextManager):
            return result.__exit__(exc, val, tb)

        raise NotImplementedError("(async) with is not implemented for this object")


class UnifiedIterator[T](Iterator[T], AsyncIterator[T]):
    def __init__(self, future_iterable: Iterator[Future[T]]) -> None:
        self.future_iterable = future_iterable

    @classmethod
    def from_call[**P](cls, func: Callable[P, Iterator[Future[T]]], *args: P.args, **kwargs: P.kwargs) -> Self:
        return cls(func(*args, **kwargs))

    @property
    def futures(self) -> Iterator[Future[T]]:
        return self.future_iterable

    def run_as_completed(self, callback: Callable[[Future[T]], Any]) -> UnifiedFuture[None]:
        state = UnifiedFuture[None]()

        def _is_done_or_cancelled() -> bool:
            if state.done():
                return True
            if state.cancelled():
                state.set_exception(Cancelled())
                return True
            return False

        def _get_next_future() -> Future[T] | None:
            if _is_done_or_cancelled():
                return None

            try:
                next_future = self.future_iterable.__next__()
            except StopIteration:
                state.set_result(None)
                return None
            except BaseException as e:
                state.set_exception(e)
                return None
            return next_future

        def _run_callbacks() -> None:
            try:
                while (future := _get_next_future()) is not None:
                    # Wait for the future to finish.
                    if not future.done():
                        future.add_done_callback(_continuation_in_foreign_thread)
                        return

                    # Run the callback.
                    if not _run_single_callback(future):
                        return

                    # Try to give control back to the event loop.
                    next_cycle = get_loop().next_cycle()
                    if not next_cycle.done():
                        next_cycle.add_done_callback(_continuation_from_next_cycle)
                        return

                    # We do not have a real event loop here.
                    # If the next_cycle causes an error to bubble, forward it to the state future.
                    if next_cycle.exception() is not None:
                        state.set_exception(next_cycle.exception())
                        return
            except Exception as e:
                import traceback

                traceback.print_exception(e)
                state.set_exception(e)

        def _continuation_from_next_cycle(fut: Future[None]) -> None:
            if fut.exception() is not None:
                state.set_exception(fut.exception())
            else:
                _run_callbacks()

        def _continuation_in_foreign_thread(fut: Future[T]) -> None:
            # Optimization, see below.
            get_loop().from_thread(_continuation, fut)

        def _continuation(fut: Future[T]) -> None:
            if _run_single_callback(fut):
                _run_callbacks()

        @keep_environment
        def _run_single_callback(fut: Future[T]) -> bool:
            # True   => Schedule next future.
            # False  => Cancel the loop.
            if _is_done_or_cancelled():
                return False

            try:
                result = callback(fut)
            except BaseException as e:
                state.set_exception(e)
                return False
            else:
                if result is None or bool(result):
                    return True
                else:
                    state.set_result(None)
                    return False

        # Optimization:
        # We do not need to inherit any kind of environment as
        # _run_single_callback will automatically set the environment for us.
        get_loop().from_thread(_run_callbacks)
        return state

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> T:
        fut = self.future_iterable.__next__()
        return fut.result()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        try:
            fut = self.future_iterable.__next__()
        except StopIteration:
            raise StopAsyncIteration
        return await get_loop().await_future(fut)


@overload
def unified[T, **P](
    *,
    kind: Literal["generator"],
) -> Callable[
    [Callable[P, Iterator[Future[T]]]],
    Callable[P, UnifiedIterator[T]],
]: ...


@overload
def unified[T, **P](
    *,
    kind: Literal["future"],
) -> Callable[
    [Callable[P, Future[T]]],
    Callable[P, UnifiedFuture[T]],
]: ...


@overload
def unified[T, **P](
    *,
    kind: Literal["generator"],
    iterable_class: type[UnifiedIterator[T]],
) -> Callable[
    [Callable[P, Iterator[Future[T]]]],
    Callable[P, UnifiedIterator[T]],
]: ...


@overload
def unified[T, **P](
    *,
    kind: Literal["future"],
    future_class: type[UnifiedFuture[T]],
) -> Callable[
    [Callable[P, Future[T]]],
    Callable[P, UnifiedFuture[T]],
]: ...


@overload
def unified[T, **P](
    *,
    kind: Literal["auto"] = "auto",
    iterable_class: type[UnifiedIterator[Any]] = ...,
    future_class: type[UnifiedFuture[Any]] = ...,
) -> Callable[
    [Callable[P, Future[T] | Iterator[Future[T]]]],
    Callable[P, UnifiedFuture[T] | UnifiedIterator[T]],
]: ...


# Implementation
def unified[T, **P](
    *,
    kind: str = "auto",
    iterable_class: type[UnifiedIterator[Any]] = UnifiedIterator[Any],
    future_class: type[UnifiedFuture[Any]] = UnifiedFuture[Any],
) -> Any:
    """
    Decorator to normalize functions returning Future[T] or Iterator[Future[T]]
    into functions returning UnifiedFuture[T] or UnifiedIterator[T].
    """

    def _decorator_generator(func: Callable[P, Iterator[Future[T]]]) -> Callable[P, UnifiedIterator[T]]:
        @wraps(func)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> UnifiedIterator[T]:
            return iterable_class.from_call(func, *args, **kwargs)

        return _wrapped

    def _decorator_future(func: Callable[P, Future[T]]) -> Callable[P, UnifiedFuture[T]]:
        @wraps(func)
        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> UnifiedFuture[T]:
            return future_class.from_call(func, *args, **kwargs)

        return _wrapped

    def decorator(
        func: Callable[P, Iterator[Future[T]]] | Callable[P, Future[T]],
    ) -> Callable[P, UnifiedIterator[T]] | Callable[P, UnifiedFuture[T]]:
        if kind == "auto":
            if isgeneratorfunction(func):
                return _decorator_generator(func)
            return _decorator_future(func)  # type:ignore[arg-type]

        if kind == "generator":
            return _decorator_generator(func)  # type:ignore[arg-type]

        if kind == "future":
            return _decorator_future(func)  # type:ignore[arg-type]

        raise NotImplementedError

    return decorator
