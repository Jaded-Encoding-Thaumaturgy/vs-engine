# vs-engine
# Copyright (C) 2022  cid-chan
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""
vsengine.render renders video frames for you.
"""

from collections.abc import Iterator, Sequence
from concurrent.futures import Future

import vapoursynth

from vsengine._futures import UnifiedFuture, unified
from vsengine._helpers import EnvironmentTypes, use_inline
from vsengine._nodes import buffer_futures, close_when_needed


@unified()
def frame(
    node: vapoursynth.VideoNode, frameno: int, env: EnvironmentTypes | None = None
) -> Future[vapoursynth.VideoFrame]:
    with use_inline("frame", env):
        return node.get_frame_async(frameno)


@unified()
def planes(
    node: vapoursynth.VideoNode,
    frameno: int,
    env: EnvironmentTypes | None = None,
    *,
    planes: Sequence[int] | None = None,
) -> Future[tuple[bytes, ...]]:
    def _extract(frame: vapoursynth.VideoFrame) -> tuple[bytes, ...]:
        try:
            # This might be a variable format clip.
            # extract the plane as late as possible.
            ps = range(len(frame)) if planes is None else planes
            return tuple(bytes(frame[p]) for p in ps)
        finally:
            frame.close()

    return frame(node, frameno, env).map(_extract)


@unified(type="generator")
def frames(
    node: vapoursynth.VideoNode,
    env: EnvironmentTypes | None = None,
    *,
    prefetch: int = 0,
    backlog: int | None = None,
    # Unlike the implementation provided by VapourSynth,
    # we don't have to care about backwards compatibility and
    # can just do the right thing from the beginning.
    close: bool = True,
) -> Iterator[Future[vapoursynth.VideoFrame]]:
    with use_inline("frames", env):
        length = len(node)

    it = (frame(node, n, env) for n in range(length))

    # If backlog is zero, skip.
    if backlog is None or backlog > 0:
        it = buffer_futures(it, prefetch=prefetch, backlog=backlog)

    if close:
        it = close_when_needed(it)
    return it


@unified(type="generator")
def render(
    node: vapoursynth.VideoNode,
    env: int | None = None,
    *,
    prefetch: int = 0,
    backlog: int | None = 0,
    y4m: bool = False,
) -> Iterator[Future[tuple[int, bytes]]]:
    frame_count = len(node)

    if y4m:
        y4mformat = ""
        if node.format.color_family == vapoursynth.GRAY:
            y4mformat = "mono"
            if node.format.bits_per_sample > 8:
                y4mformat = y4mformat + str(node.format.bits_per_sample)
        elif node.format.color_family == vapoursynth.YUV:
            if node.format.subsampling_w == 1 and node.format.subsampling_h == 1:
                y4mformat = "420"
            elif node.format.subsampling_w == 1 and node.format.subsampling_h == 0:
                y4mformat = "422"
            elif node.format.subsampling_w == 0 and node.format.subsampling_h == 0:
                y4mformat = "444"
            elif node.format.subsampling_w == 2 and node.format.subsampling_h == 2:
                y4mformat = "410"
            elif node.format.subsampling_w == 2 and node.format.subsampling_h == 0:
                y4mformat = "411"
            elif node.format.subsampling_w == 0 and node.format.subsampling_h == 1:
                y4mformat = "440"
            if node.format.bits_per_sample > 8:
                y4mformat = y4mformat + "p" + str(node.format.bits_per_sample)
        else:
            raise ValueError("Can only use GRAY and YUV for V4M-Streams")

        if len(y4mformat) > 0:
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

    def render_single_frame(frame: vapoursynth.VideoFrame) -> tuple[int, bytes]:
        buf = []
        if y4m:
            buf.append(b"FRAME\n")

        for plane in frame:
            buf.append(bytes(plane))

        return current_frame, b"".join(buf)

    for frame, fut in enumerate(frames(node, env, prefetch=prefetch, backlog=backlog).futures, 1):
        current_frame = frame
        yield UnifiedFuture.from_future(fut).map(render_single_frame)
