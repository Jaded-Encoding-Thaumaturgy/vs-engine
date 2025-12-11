# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
from collections.abc import Iterable, Iterator
from concurrent.futures import Future
from threading import RLock

from vapoursynth import RawFrame, core


def buffer_futures[FrameT: RawFrame](
    futures: Iterable[Future[FrameT]], prefetch: int = 0, backlog: int | None = None
) -> Iterator[Future[FrameT]]:
    if prefetch == 0:
        prefetch = core.num_threads
    if backlog is None:
        backlog = prefetch * 3
    if backlog < prefetch:
        backlog = prefetch

    enum_fut = enumerate(futures)

    finished = False
    running = 0
    lock = RLock()
    reorder = dict[int, Future[FrameT]]()

    def _request_next() -> None:
        nonlocal finished, running
        with lock:
            if finished:
                return

            ni = next(enum_fut, None)
            if ni is None:
                finished = True
                return

            running += 1

            idx, fut = ni
            reorder[idx] = fut
            fut.add_done_callback(_finished)

    def _finished(f: Future[FrameT]) -> None:
        nonlocal finished, running
        with lock:
            running -= 1
            if finished:
                return

            if f.exception() is not None:
                finished = True
                return

            _refill()

    def _refill() -> None:
        if finished:
            return

        with lock:
            # Two rules: 1. Don't exceed the concurrency barrier.
            #            2. Don't exceed unused-frames-backlog
            while (not finished) and (running < prefetch) and len(reorder) < backlog:
                _request_next()

    _refill()

    sidx = 0
    try:
        while (not finished) or (len(reorder) > 0) or running > 0:
            if sidx not in reorder:
                # Spin. Reorder being empty should never happen.
                continue

            # Get next requested frame
            fut = reorder[sidx]
            del reorder[sidx]
            sidx += 1
            _refill()

            yield fut

    finally:
        finished = True


def close_when_needed[FrameT: RawFrame](future_iterable: Iterable[Future[FrameT]]) -> Iterator[Future[FrameT]]:
    def copy_future_and_run_cb_before(fut: Future[FrameT]) -> Future[FrameT]:
        f = Future[FrameT]()

        def _as_completed(_: Future[FrameT]) -> None:
            try:
                r = fut.result()
            except Exception as e:
                f.set_exception(e)
            else:
                new_r = r.__enter__()
                f.set_result(new_r)

        fut.add_done_callback(_as_completed)
        return f

    def close_fut(f: Future[FrameT]) -> None:
        def _do_close(_: Future[FrameT]) -> None:
            if f.exception() is None:
                f.result().__exit__(None, None, None)

        f.add_done_callback(_do_close)

    for fut in future_iterable:
        yield copy_future_and_run_cb_before(fut)
        close_fut(fut)
