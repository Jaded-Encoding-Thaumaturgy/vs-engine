# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""Tests for the video module."""

from collections.abc import Iterator

import pytest
from vapoursynth import GRAY8, RGB24, PresetVideoFormat, VideoFormat, VideoFrame, VideoNode, core

from tests._testutils import use_standalone_policy
from vsengine.video import frame, frames, planes, render

AnyFormat = PresetVideoFormat | VideoFormat


@pytest.fixture(autouse=True)
def standalone_policy() -> Iterator[None]:
    """Set up a standalone policy for video tests."""
    use_standalone_policy()
    yield


def generate_video(length: int = 3, width: int = 1, height: int = 1, format: AnyFormat = GRAY8) -> VideoNode:
    """Generate a test video clip with frame numbers in props."""
    clip = core.std.BlankClip(length=length, width=width, height=height, format=format, fpsden=1001, fpsnum=24000)

    def _add_frameno(n: int, f: VideoFrame) -> VideoFrame:
        fout = f.copy()
        fout.props["FrameNumber"] = n
        return fout

    clip = core.std.ModifyFrame(clip=clip, clips=clip, selector=_add_frameno)
    return clip


def test_planes() -> None:
    clip_a = core.std.BlankClip(length=1, color=[0, 1, 2], width=1, height=1, format=RGB24)
    clip_b = core.std.BlankClip(length=1, color=[3, 4, 5], width=1, height=1, format=RGB24)

    clip = core.std.Splice([clip_a, clip_b])

    assert list(planes(clip, 0).result()) == [b"\x00", b"\x01", b"\x02"]
    assert list(planes(clip, 0, planes=[0]).result()) == [b"\x00"]
    assert list(planes(clip, 0, planes=[1]).result()) == [b"\x01"]
    assert list(planes(clip, 0, planes=[2]).result()) == [b"\x02"]
    assert list(planes(clip, 0, planes=[2, 1, 0]).result()) == [b"\x02", b"\x01", b"\x00"]

    assert list(planes(clip, 1).result()) == [b"\x03", b"\x04", b"\x05"]
    assert list(planes(clip, 1, planes=[0]).result()) == [b"\x03"]
    assert list(planes(clip, 1, planes=[1]).result()) == [b"\x04"]
    assert list(planes(clip, 1, planes=[2]).result()) == [b"\x05"]
    assert list(planes(clip, 1, planes=[2, 1, 0]).result()) == [b"\x05", b"\x04", b"\x03"]


def test_planes_default_supports_multiformat_clips() -> None:
    clip_a = core.std.BlankClip(length=1, color=[0, 1, 2], width=1, height=1, format=RGB24)
    clip_b = core.std.BlankClip(length=1, color=[3], width=1, height=1, format=GRAY8)

    clip = core.std.Splice([clip_a, clip_b], mismatch=True)
    assert list(planes(clip, 0).result()) == [b"\x00", b"\x01", b"\x02"]
    assert list(planes(clip, 1).result()) == [b"\x03"]


def test_single_frame() -> None:
    clip = generate_video()
    with frame(clip, 0).result(timeout=0.1) as f:
        assert f.props["FrameNumber"] == 0

    with frame(clip, 1).result(timeout=0.1) as f:
        assert f.props["FrameNumber"] == 1

    with frame(clip, 2).result(timeout=0.1) as f:
        assert f.props["FrameNumber"] == 2


def test_multiple_frames() -> None:
    clip = generate_video()
    for nf, f in enumerate(frames(clip)):
        assert f.props["FrameNumber"] == nf


def test_multiple_frames_closes_after_iteration() -> None:
    clip = generate_video()

    it = iter(frames(clip))
    f1 = next(it)

    try:
        f2 = next(it)
    except Exception:
        f1.close()
        raise

    try:
        with pytest.raises(RuntimeError):
            _ = f1.props
    finally:
        f2.close()
        next(it).close()


def test_multiple_frames_without_closing() -> None:
    clip = generate_video()
    for nf, f in enumerate(frames(clip, close=False)):
        assert f.props["FrameNumber"] == nf
        f.close()


def test_render() -> None:
    clip = generate_video()
    data = b"".join(f[1] for f in render(clip))
    assert data == b"\0\0\0"


def test_render_y4m() -> None:
    clip = generate_video()
    data = b"".join(f[1] for f in render(clip, y4m=True))
    assert data == b"YUV4MPEG2 Cmono W1 H1 F24000:1001 Ip A0:0 XLENGTH=3\nFRAME\n\0FRAME\n\0FRAME\n\0"


def test_render_y4m_yuv420() -> None:
    from vapoursynth import YUV420P8

    clip = core.std.BlankClip(length=1, width=2, height=2, format=YUV420P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C420 W2 H2 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_yuv422() -> None:
    from vapoursynth import YUV422P8

    clip = core.std.BlankClip(length=1, width=2, height=2, format=YUV422P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C422 W2 H2 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_yuv444() -> None:
    from vapoursynth import YUV444P8

    clip = core.std.BlankClip(length=1, width=2, height=2, format=YUV444P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C444 W2 H2 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_yuv410() -> None:
    from vapoursynth import YUV410P8

    clip = core.std.BlankClip(length=1, width=4, height=4, format=YUV410P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C410 W4 H4 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_yuv411() -> None:
    from vapoursynth import YUV411P8

    clip = core.std.BlankClip(length=1, width=4, height=4, format=YUV411P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C411 W4 H4 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_yuv440() -> None:
    from vapoursynth import YUV440P8

    clip = core.std.BlankClip(length=1, width=4, height=4, format=YUV440P8)
    data = b"".join(f[1] for f in render(clip, y4m=True))
    header = b"YUV4MPEG2 C440 W4 H4 F24:1 Ip A0:0 XLENGTH=1\n"
    assert data.startswith(header)


def test_render_y4m_high_bitdepth() -> None:
    from vapoursynth import YUV420P10, YUV420P16

    # 10-bit
    clip_10 = core.std.BlankClip(length=1, width=2, height=2, format=YUV420P10)
    data_10 = b"".join(f[1] for f in render(clip_10, y4m=True))
    assert b"C420p10 " in data_10

    # 16-bit
    clip_16 = core.std.BlankClip(length=1, width=2, height=2, format=YUV420P16)
    data_16 = b"".join(f[1] for f in render(clip_16, y4m=True))
    assert b"C420p16 " in data_16


def test_render_y4m_gray_high_bitdepth() -> None:
    from vapoursynth import GRAY10, GRAY16

    clip_10 = core.std.BlankClip(length=1, width=2, height=2, format=GRAY10)
    data_10 = b"".join(f[1] for f in render(clip_10, y4m=True))
    assert b"Cmonop10 " in data_10

    clip_16 = core.std.BlankClip(length=1, width=2, height=2, format=GRAY16)
    data_16 = b"".join(f[1] for f in render(clip_16, y4m=True))
    assert b"Cmonop16 " in data_16


def test_render_y4m_rgb_raises() -> None:
    clip = core.std.BlankClip(length=1, width=2, height=2, format=RGB24)
    with pytest.raises(ValueError, match="Can only use GRAY and YUV"):
        list(render(clip, y4m=True))


def test_render_frame_numbers() -> None:
    clip = generate_video(length=5)
    frame_numbers = [f[0] for f in render(clip)]
    assert frame_numbers == [1, 2, 3, 4, 5]


def test_render_frame_numbers_y4m() -> None:
    """Test that render with Y4M returns correct frame numbers (0 for header, then 1-based)."""
    clip = generate_video(length=3)
    frame_numbers = [f[0] for f in render(clip, y4m=True)]
    # First tuple is the header (frame 0), then frame 1, 2, 3
    assert frame_numbers == [0, 1, 2, 3]


def test_render_with_prefetch() -> None:
    clip = generate_video(length=5)
    data = b"".join(f[1] for f in render(clip, prefetch=2))
    assert data == b"\0\0\0\0\0"


def test_render_with_backlog() -> None:
    clip = generate_video(length=5)
    # backlog=None means no limit on queued frames
    data = b"".join(f[1] for f in render(clip, backlog=None))
    assert data == b"\0\0\0\0\0"


def test_render_with_prefetch_and_backlog() -> None:
    clip = generate_video(length=5)
    data = b"".join(f[1] for f in render(clip, prefetch=3, backlog=5))
    assert data == b"\0\0\0\0\0"


def test_render_single_frame() -> None:
    clip = core.std.BlankClip(length=1, width=1, height=1, format=GRAY8)
    data = list(render(clip))
    assert len(data) == 1
    assert data[0][0] == 1
    assert data[0][1] == b"\0"


def test_render_single_frame_y4m() -> None:
    clip = core.std.BlankClip(length=1, width=1, height=1, format=GRAY8)
    data = list(render(clip, y4m=True))
    # Should have header at index 0, frame at index 1
    assert len(data) == 2
    assert data[0][0] == 0  # Header is frame 0
    assert data[0][1].startswith(b"YUV4MPEG2")
    assert data[1][0] == 1  # First frame is 1
    assert b"FRAME\n" in data[1][1]
