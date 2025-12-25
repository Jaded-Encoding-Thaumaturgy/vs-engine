# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""Tests for the hospice module (delayed object cleanup)."""

import contextlib
import gc
import logging
import weakref
from collections.abc import Iterator
from typing import Any

import pytest

from vsengine import _hospice
from vsengine._hospice import admit_environment, any_alive, freeze, unfreeze


@pytest.fixture(autouse=True)
def reset_hospice_state() -> Iterator[None]:
    """Reset hospice module state before each test to ensure isolation."""
    # Clear the mock timings registry
    _mock_timings_registry.clear()
    # Clear all hospice state before test
    _hospice.stage1.clear()
    _hospice.stage2.clear()
    _hospice.stage2_to_add.clear()
    _hospice.hold.clear()
    _hospice.cores.clear()
    _hospice.refnanny.clear()
    yield
    # Clean up after test as well
    _mock_timings_registry.clear()
    _hospice.stage1.clear()
    _hospice.stage2.clear()
    _hospice.stage2_to_add.clear()
    _hospice.hold.clear()
    _hospice.cores.clear()
    _hospice.refnanny.clear()


# Global registry to simulate CoreTimings holding references to cores
# This adds the extra reference needed for the > 3 refcount check
_mock_timings_registry = list[Any]()


class MockCore:
    """
    Mock Core object that simulates the CoreTimings reference behavior.

    Real VapourSynth Core has a CoreTimings object that holds a reference to it,
    so getrefcount(core) is at least 3:
    - 1 from cores dict in hospice
    - 1 from CoreTimings
    - 1 from getrefcount() temporary

    We simulate this by registering each MockCore in a global registry.
    """

    def __init__(self) -> None:
        # Register self to simulate CoreTimings reference
        _mock_timings_registry.append(self)


class MockEnv:
    """Mock EnvironmentData object."""


@contextlib.contextmanager
def hide_logs() -> Iterator[None]:
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)


def test_hospice_delays_connection() -> None:
    o1 = MockEnv()
    o2 = MockCore()
    o2r = weakref.ref(o2)

    admit_environment(o1, o2)  # type:ignore[arg-type]

    # Remove local ref to o2, but registry still holds it
    del o2
    del o1

    # Clear the mock registry to release the "CoreTimings" reference
    _mock_timings_registry.clear()

    assert o2r() is not None

    gc.collect()
    assert o2r() is not None

    # Stage-2 add-queue + Stage 2 proper
    gc.collect()
    gc.collect()

    assert o2r() is None


def test_hospice_is_delayed_on_alive_objects(caplog: pytest.LogCaptureFixture) -> None:
    o1 = MockEnv()
    o2 = MockCore()
    o2r = weakref.ref(o2)

    admit_environment(o1, o2)  # type:ignore[arg-type]
    del o1

    # o2 is still held by local var AND registry, so refcount > 3
    with caplog.at_level(logging.WARN, logger="vsengine._hospice"):
        gc.collect()
        gc.collect()

    assert len(caplog.records) > 0

    # Delete local ref but keep registry - still should delay collection
    del o2
    assert o2r() is not None

    # Now clear registry to allow collection
    _mock_timings_registry.clear()

    gc.collect()
    gc.collect()
    gc.collect()

    assert o2r() is None


def test_hospice_reports_alive_objects_correctly() -> None:
    o1 = MockEnv()
    o2 = MockCore()
    admit_environment(o1, o2)  # type:ignore[arg-type]
    del o1

    # o2 is still alive (local var + registry)
    with hide_logs():
        assert any_alive(), "The hospice did report that all objects are not alive anymore. This is obviously not true."

    # Delete local ref but keep registry - still delays as "alive" due to CoreTimings-like ref
    del o2

    # Now clear the registry to allow collection
    _mock_timings_registry.clear()

    assert not any_alive(), "The hospice did report that there are some objects left alive. This is obviously not true."


def test_hospice_can_forget_about_cores_safely() -> None:
    o1 = MockEnv()
    o2 = MockCore()
    admit_environment(o1, o2)  # type:ignore[arg-type]
    del o1

    with hide_logs():
        assert any_alive(), "The hospice did report that all objects are not alive anymore. This is obviously not true."
    freeze()
    assert not any_alive(), "The hospice did report that there are some objects left alive. This is obviously not true."

    unfreeze()
    with hide_logs():
        assert any_alive(), "The hospice did report that all objects are not alive anymore. This is obviously not true."
    del o2
    _mock_timings_registry.clear()

    gc.collect()
    gc.collect()
