# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""
vsengine.render renders video frames for you.
"""

from collections.abc import Iterator, Sequence
from concurrent.futures import Future

import vapoursynth as vs

from vsengine._futures import UnifiedFuture, unified
from vsengine._helpers import use_inline
from vsengine._nodes import buffer_futures, close_when_needed
from vsengine.policy import ManagedEnvironment


@unified(kind="future")
def frame(
    node: vs.VideoNode, frameno: int, env: vs.Environment | ManagedEnvironment | None = None
) -> Future[vs.VideoFrame]:
    with use_inline("frame", env):
        return node.get_frame_async(frameno)


@unified(kind="future")
def planes(
    node: vs.VideoNode,
    frameno: int,
    env: vs.Environment | ManagedEnvironment | None = None,
    *,
    planes: Sequence[int] | None = None,
) -> Future[tuple[bytes, ...]]:
    def _extract(frame: vs.VideoFrame) -> tuple[bytes, ...]:
        try:
            # This might be a variable format clip.
            # extract the plane as late as possible.
            ps = range(len(frame)) if planes is None else planes
            return tuple(bytes(frame[p]) for p in ps)
        finally:
            frame.close()

    return frame(node, frameno, env).map(_extract)


@unified(kind="generator")
def frames(
    node: vs.VideoNode,
    env: vs.Environment | ManagedEnvironment | None = None,
    *,
    prefetch: int = 0,
    backlog: int | None = None,
    # Unlike the implementation provided by VapourSynth,
    # we don't have to care about backwards compatibility and
    # can just do the right thing from the beginning.
    close: bool = True,
) -> Iterator[Future[vs.VideoFrame]]:
    with use_inline("frames", env):
        length = len(node)

    it = (frame(node, n, env) for n in range(length))

    # If backlog is zero, skip.
    if backlog is None or backlog > 0:
        it = buffer_futures(it, prefetch=prefetch, backlog=backlog)

    if close:
        it = close_when_needed(it)

    return it


@unified(kind="generator")
def render(
    node: vs.VideoNode,
    env: vs.Environment | ManagedEnvironment | None = None,
    *,
    prefetch: int = 0,
    backlog: int | None = 0,
    y4m: bool = False,
) -> Iterator[Future[tuple[int, bytes]]]:
    frame_count = len(node)

    if y4m:
        match node.format.color_family:
            case vs.GRAY:
                y4mformat = "mono"
            case vs.YUV:
                match (node.format.subsampling_w, node.format.subsampling_h):
                    case (1, 1):
                        y4mformat = "420"
                    case (1, 0):
                        y4mformat = "422"
                    case (0, 0):
                        y4mformat = "444"
                    case (2, 2):
                        y4mformat = "410"
                    case (2, 0):
                        y4mformat = "411"
                    case (0, 1):
                        y4mformat = "440"
                    case _:
                        raise NotImplementedError
            case _:
                raise ValueError("Can only use GRAY and YUV for V4M-Streams")

        if node.format.bits_per_sample > 8:
            y4mformat += f"p{node.format.bits_per_sample}"

        y4mformat = "C" + y4mformat + " "

        data = "YUV4MPEG2 {y4mformat}W{width} H{height} F{fps_num}:{fps_den} Ip A0:0 XLENGTH={length}\n".format(
            y4mformat=y4mformat,
            width=node.width,
            height=node.height,
            fps_num=node.fps_num,
            fps_den=node.fps_den,
            length=frame_count,
        )
        yield UnifiedFuture.resolve((0, data.encode("ascii")))

    current_frame = 0

    def render_single_frame(frame: vs.VideoFrame) -> tuple[int, bytes]:
        buf = list[bytes]()

        if y4m:
            buf.append(b"FRAME\n")

        for plane in iter(frame):
            buf.append(bytes(plane))

        return current_frame, b"".join(buf)

    for frame, fut in enumerate(frames(node, env, prefetch=prefetch, backlog=backlog).futures, 1):
        current_frame = frame
        yield UnifiedFuture.from_future(fut).map(render_single_frame)
