# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2

"""This module provides an abstraction layer to integrate VapourSynth with any event loop (asyncio, Qt, Trio, etc.)."""

import threading
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures import CancelledError, Future
from contextlib import contextmanager
from functools import wraps

import vapoursynth as vs

__all__ = ["Cancelled", "EventLoop", "from_thread", "get_loop", "keep_environment", "set_loop", "to_thread"]


class Cancelled(BaseException):
    """Exception raised when an operation has been cancelled."""


@contextmanager
def _noop() -> Iterator[None]:
    yield


DONE = Future[None]()
DONE.set_result(None)


class EventLoop:
    """
    Abstract base class for event loop integration.

    These functions must be implemented to bridge VapourSynth with the event-loop of your choice (e.g., asyncio, Qt).
    """

    def attach(self) -> None:
        """
        Initialize the event loop hooks.

        Called automatically when :func:`set_loop` is run.
        """

    def detach(self) -> None:
        """
        Clean up event loop hooks.

        Called when another event-loop takes over, or when the application
        is shutting down/restarting.
        """

    @abstractmethod
    def from_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Schedule a function to run on the event loop (usually the main thread).

        This is typically called from VapourSynth threads to move data or
        logic back to the main application loop.

        :param func: The callable to execute.
        :param args: Positional arguments for the callable.
        :param kwargs: Keyword arguments for the callable.
        :return: A Future representing the execution result.
        """

    def to_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """
        Run a function in a separate worker thread.

        This is used to offload blocking operations from the main event loop.
        The default implementation utilizes :class:`threading.Thread`.

        :param func: The callable to execute.
        :param args: Positional arguments for the callable.
        :param kwargs: Keyword arguments for the callable.
        :return: A Future representing the execution result.
        """
        fut = Future[R]()

        def wrapper() -> None:
            if not fut.set_running_or_notify_cancel():
                return

            try:
                result = func(*args, **kwargs)
            except BaseException as e:
                fut.set_exception(e)
            else:
                fut.set_result(result)

        threading.Thread(target=wrapper).start()

        return fut

    def next_cycle(self) -> Future[None]:
        """
        Pass control back to the event loop.

        This allows the event loop to process pending events.

        * If there is **no** event-loop, the function returns an immediately resolved future.
        * If there **is** an event-loop, the function returns a pending future that
            resolves after the next cycle.

        :raises vsengine.loops.Cancelled: If the operation has been cancelled.
        :return: A Future that resolves when the cycle is complete.
        """
        future = Future[None]()
        self.from_thread(future.set_result, None)
        return future

    def await_future[T](self, future: Future[T]) -> Awaitable[T]:
        """
        Convert a concurrent Future into an Awaitable compatible with this loop.

        This function does not need to be implemented if the event-loop
        does not support ``async`` and ``await`` syntax.

        :param future: The concurrent.futures.Future to await.
        :return: An awaitable object.
        """
        raise NotImplementedError

    @contextmanager
    def wrap_cancelled(self) -> Iterator[None]:
        """
        Context manager to translate cancellation exceptions.

        Wraps :exc:`vsengine.loops.Cancelled` into the native cancellation
        error of the specific event loop implementation (e.g., ``asyncio.CancelledError``).
        """
        try:
            yield
        except Cancelled:
            raise CancelledError from None


class _NoEventLoop(EventLoop):
    """
    The default event-loop implementation.

    This is used when no specific loop is attached. It runs operations synchronously/inline.
    """

    def from_thread[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        fut = Future[R]()
        try:
            result = func(*args, **kwargs)
        except BaseException as e:
            fut.set_exception(e)
        else:
            fut.set_result(result)
        return fut

    def next_cycle(self) -> Future[None]:
        return DONE


NO_LOOP = _NoEventLoop()
_current_loop: EventLoop = NO_LOOP


def get_loop() -> EventLoop:
    """
    Retrieve the currently active event loop.

    :return: The currently running EventLoop instance.
    """
    return _current_loop


def set_loop(loop: EventLoop) -> None:
    """
    Set the currently running event loop.

    This function will detach the previous loop first. If attaching the new
    loop fails, it reverts to the ``_NoEventLoop`` implementation which runs
    everything inline.

    :param loop: The EventLoop instance to attach.
    """
    global _current_loop
    _current_loop.detach()

    try:
        _current_loop = loop
        loop.attach()
    except:
        _current_loop = NO_LOOP
        raise


def keep_environment[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """
    Decorate a function to preserve the VapourSynth environment.

    The returned function captures the VapourSynth environment active
    at the moment the decorator is applied and restores it when the
    function is executed.

    :param func: The function to decorate.
    :return: A wrapped function that maintains the captured environment.
    """
    try:
        environment = vs.get_current_environment().use
    except RuntimeError:
        environment = _noop

    @wraps(func)
    def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with environment():
            return func(*args, **kwargs)

    return _wrapper


def from_thread[**P, R](func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
    """
    Run a function inside the current event-loop.

    This preserves the currently running VapourSynth environment (if any).

    .. note::
       Depending on the loop implementation, the function might be called inline.

    :param func: The function to call inside the current event loop.
    :param args: The arguments for the function.
    :param kwargs: The keyword arguments to pass to the function.
    :return: A Future that resolves or rejects depending on the outcome.
    """

    @keep_environment
    def _wrapper() -> R:
        return func(*args, **kwargs)

    return get_loop().from_thread(_wrapper)


def to_thread[**P, R](func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
    """
    Run a function in a dedicated thread or worker.

    This preserves the currently running VapourSynth environment (if any).

    :param func: The function to call in a worker thread.
    :param args: The arguments for the function.
    :param kwargs: The keyword arguments to pass to the function.
    :return: A Future representing the execution result.
    """

    @keep_environment
    def _wrapper() -> R:
        return func(*args, **kwargs)

    return get_loop().to_thread(_wrapper)


async def make_awaitable[T](future: Future[T]) -> T:
    """
    Make a standard concurrent Future awaitable in the current loop.

    :param future: The future object to make awaitable.
    :return: The result of the future, once awaited.
    """
    return await get_loop().await_future(future)
