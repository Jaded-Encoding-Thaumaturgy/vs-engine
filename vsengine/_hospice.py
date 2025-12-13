# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
import gc
import logging
import sys
import threading
import weakref
from typing import Literal

from vapoursynth import Core, EnvironmentData

logger = logging.getLogger(__name__)


lock = threading.Lock()
refctr = 0
refnanny = dict[int, weakref.ReferenceType[EnvironmentData]]()
cores = dict[int, Core]()

stage2_to_add = set[int]()
stage2 = set[int]()
stage1 = set[int]()

hold = set[int]()


def admit_environment(environment: EnvironmentData, core: Core) -> None:
    global refctr

    with lock:
        ident = refctr
        refctr += 1

    ref = weakref.ref(environment, lambda _: _add_tostage1(ident))
    cores[ident] = core
    refnanny[ident] = ref

    logger.debug("Admitted environment %r and %r as with ID:%s.", environment, core, ident)


def any_alive() -> bool:
    if bool(stage1) or bool(stage2) or bool(stage2_to_add):
        gc.collect()
    if bool(stage1) or bool(stage2) or bool(stage2_to_add):
        gc.collect()
    if bool(stage1) or bool(stage2) or bool(stage2_to_add):
        gc.collect()
    return bool(stage1) or bool(stage2) or bool(stage2_to_add)


def freeze() -> None:
    logger.debug("Freezing the hospice. Cores won't be collected anyore.")

    hold.update(stage1)
    hold.update(stage2)
    hold.update(stage2_to_add)
    stage1.clear()
    stage2.clear()
    stage2_to_add.clear()


def unfreeze() -> None:
    stage1.update(hold)
    hold.clear()


def _is_core_still_used(ident: int) -> bool:
    return sys.getrefcount(cores[ident]) > 3


def _add_tostage1(ident: int) -> None:
    logger.debug("Environment has died. Keeping core for a few gc-cycles. ID:%s", ident)

    with lock:
        stage1.add(ident)


def _collectstage1(phase: Literal["start", "stop"], _: dict[str, int]) -> None:
    if phase != "stop":
        return

    with lock:
        for ident in tuple(stage1):
            if _is_core_still_used(ident):
                logger.warning("Core is still in use. ID:%s", ident)
                continue

            stage1.remove(ident)
            stage2_to_add.add(ident)


def _collectstage2(phase: Literal["start", "stop"], _: dict[str, int]) -> None:
    global stage2_to_add

    if phase != "stop":
        return

    garbage = []
    with lock:
        for ident in tuple(stage2):
            if _is_core_still_used(ident):
                logger.warning("Core is still in use in stage 2. ID:%s", ident)
                continue

            stage2.remove(ident)
            garbage.append(cores.pop(ident))
            logger.debug("Marking core %r for collection", ident)

        stage2.update(stage2_to_add)
        stage2_to_add = set()

    garbage.clear()


gc.callbacks.append(_collectstage2)
gc.callbacks.append(_collectstage1)
