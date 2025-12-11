# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""
vsengine.policy implements a basic object-oriented implementation of
EnvironmentPolicies.


Here a quick run-down in how to use it, (but be sure to read on to select
the best store-implementation for you):

    >>> import vapoursynth as vs
    >>> policy = Policy(GlobalStore())
    >>> policy.register()
    >>> with policy.new_environment() as env:
    ...     with env.use():
    ...         vs.core.std.BlankClip().set_output()
    ...     print(env.outputs)
    {"0": <vapoursynth.VideoNode ...>}
    >>> policy.unregister()


To use it, you first have to pick an EnvironmentStore implementation.
A EnvironmentStore is just a simple object implementing the methods
set_current_environment and get_current_environment.
These actually implement the state an EnvironmentPolicy is responsible
for managing.

For convenience, three EnvironmentStores have already been implemented,
tailored for different uses and concurrency needs:

- The GlobalStore is useful when you are ever only using one Environment
  at the same time

- ThreadLocalStore is useful when you writing a multi-threaded applications,
  that can run multiple environments at once. This one behaves like vsscript.

- ContextVarStore is useful when you are using event-loops like asyncio,
  curio, and trio. When using this store, make sure to reuse the store
  between successive Policy-instances as otherwise the old store might
  leak objects. More details are written in the documentation of the
  contextvars module of the standard library.

All three implementations can be instantiated without any arguments.


The instance of the EnvironmentStore is then passed to Policy, on which
you then call register on.

You can create ManagedEnvironment-instances by calling
policy.new_environment(). These instances can then be used to switch to
the given environment, retrieve its outputs or get its core.

Be aware that ManagedEnvironment-instances must call dispose() when
you are done using them. Failing to do so will result in a warning.
ManagedEnvironment is also a context-manager which does it for you.

When reloading the application, you can call policy.unregister()
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import threading
import weakref
from collections.abc import Iterator, Mapping
from types import TracebackType
from typing import TYPE_CHECKING, Self

import vapoursynth as vs
from vapoursynth import Environment, EnvironmentData, EnvironmentPolicy, EnvironmentPolicyAPI, register_policy

from vsengine._hospice import admit_environment

__all__ = ["ContextVarStore", "GlobalStore", "ManagedEnvironment", "Policy", "ThreadLocalStore"]


logger = logging.getLogger(__name__)


class EnvironmentStore:
    """
    Environment Stores manage which environment is currently active.
    """

    def set_current_environment(self, environment: weakref.ReferenceType[EnvironmentData] | None) -> None:
        """
        Set the current environment in the store.
        """
        ...

    def get_current_environment(self) -> weakref.ReferenceType[EnvironmentData] | None:
        """
        Retrieve the current environment from the store (if any)
        """
        ...


class GlobalStore(EnvironmentStore):
    """
    This is the simplest store: It just stores the environment in a variable.
    """

    _current: weakref.ReferenceType[EnvironmentData] | None
    __slots__ = ("_current",)

    def set_current_environment(self, environment: weakref.ReferenceType[EnvironmentData] | None) -> None:
        self._current = environment

    def get_current_environment(self) -> weakref.ReferenceType[EnvironmentData] | None:
        return getattr(self, "_current", None)


class ThreadLocalStore(EnvironmentStore):
    """
    For simple threaded applications, use this store.

    It will store the environment in a thread-local variable.
    """

    _current: threading.local

    def __init__(self) -> None:
        self._current = threading.local()

    def set_current_environment(self, environment: weakref.ReferenceType[EnvironmentData] | None) -> None:
        self._current.environment = environment

    def get_current_environment(self) -> weakref.ReferenceType[EnvironmentData] | None:
        return getattr(self._current, "environment", None)


class ContextVarStore(EnvironmentStore):
    """
    If you are using AsyncIO or similar frameworks, use this store.
    """

    _current: contextvars.ContextVar[weakref.ReferenceType[EnvironmentData] | None]

    def __init__(self, name: str = "vapoursynth") -> None:
        self._current = contextvars.ContextVar(name)

    def set_current_environment(self, environment: weakref.ReferenceType[EnvironmentData] | None) -> None:
        self._current.set(environment)

    def get_current_environment(self) -> weakref.ReferenceType[EnvironmentData] | None:
        return self._current.get(None)


class _ManagedPolicy(EnvironmentPolicy):
    """
    This class directly interfaces with VapourSynth.
    """

    __slots__ = ("_api", "_local", "_mutex", "_store")

    def __init__(self, store: EnvironmentStore) -> None:
        self._store = store
        self._mutex = threading.Lock()
        self._local = threading.local()

    # For engine-calls that require vapoursynth but
    # should not make their switch observable from the outside.

    # Start the section.
    def inline_section_start(self, environment: EnvironmentData) -> None:
        self._local.environment = environment

    # End the section.
    def inline_section_end(self) -> None:
        del self._local.environment

    @property
    def api(self) -> EnvironmentPolicyAPI:
        if hasattr(self, "_api"):
            return self._api

        raise RuntimeError("Invalid state: No access to the current API")

    def on_policy_registered(self, special_api: EnvironmentPolicyAPI) -> None:
        logger.debug("Successfully registered policy with VapourSynth.")
        self._api = special_api

    def on_policy_cleared(self) -> None:
        del self._api
        logger.debug("Policy cleared.")

    def get_current_environment(self) -> EnvironmentData | None:
        # For small segments, allow switching the environment inline.
        # This is useful for vsengine-functions that require access to the
        # vapoursynth api, but don't want to invoke the store for it.
        if (env := getattr(self._local, "environment", None)) is not None and self.is_alive(env):
            return env

        # We wrap everything in a mutex to make sure
        # no context-switch can reliably happen in this section.
        with self._mutex:
            current_environment = self._store.get_current_environment()
            if current_environment is None:
                return None

            if current_environment() is None:
                logger.warning(f"Got dead environment: {current_environment()!r}")
                self._store.set_current_environment(None)
                return None

            received_environment = current_environment()

            if TYPE_CHECKING:
                assert received_environment

            if not self.is_alive(received_environment):
                logger.warning(f"Got dead environment: {received_environment!r}")
                # Remove the environment.
                self._store.set_current_environment(None)
                return None

            return received_environment

    def set_environment(self, environment: EnvironmentData | None) -> EnvironmentData | None:
        with self._mutex:
            previous_environment = self._store.get_current_environment()

            if environment is not None and not self.is_alive(environment):
                logger.warning(f"Got dead environment: {environment!r}")
                self._store.set_current_environment(None)
            else:
                logger.debug(f"Setting environment: {environment!r}")
                if environment is None:
                    self._store.set_current_environment(None)
                else:
                    self._store.set_current_environment(weakref.ref(environment))

            if previous_environment is not None:
                return previous_environment()

        return None


class ManagedEnvironment(contextlib.AbstractContextManager["ManagedEnvironment"]):
    __slots__ = ("_data", "_environment", "_policy")

    def __init__(self, environment: Environment, data: EnvironmentData, policy: Policy) -> None:
        self._environment = environment
        self._data = data
        self._policy = policy

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        self.dispose()

    @property
    def vs_environment(self) -> Environment:
        """
        Returns the vapoursynth.Environment-object representing this environment.
        """
        return self._environment

    @property
    def core(self) -> vs.Core:
        """
        Returns the core representing this environment.
        """
        with self.inline_section():
            return vs.core.core

    @property
    def outputs(self) -> Mapping[int, vs.VideoOutputTuple | vs.AudioNode]:
        """
        Returns the output within this environment.
        """
        with self.inline_section():
            return vs.get_outputs()

    @contextlib.contextmanager
    def inline_section(self) -> Iterator[None]:
        """
        Private API!

        Switches to the given environment within the block without
        notifying the store.

        If you follow the rules below, switching the environment
        will be invisible to the caller.

        Rules for safely calling this function:
        - Do not suspend greenlets within the block!
        - Do not yield or await within the block!
        - Do not use __enter__ and __exit__ directly.
        - This function is not reentrant.
        """
        self._policy.managed.inline_section_start(self._data)
        try:
            yield
        finally:
            self._policy.managed.inline_section_end()

    @contextlib.contextmanager
    def use(self) -> Iterator[None]:
        """
        Switches to this environment within a block.
        """
        # prev_environment = self._policy.managed._store.get_current_environment()
        with self._environment.use():
            yield

        # FIXME
        # # Workaround: On 32bit systems, environment policies do not reset.
        # self._policy.managed.set_environment(prev_environment)

    def switch(self) -> None:
        """
        Switches to the given environment without storing
        which environment has been defined previously.
        """
        self._environment.use().__enter__()

    def dispose(self) -> None:
        if self.disposed:
            return

        logger.debug(f"Disposing environment {self._data!r}.")
        admit_environment(self._data, self.core)
        self._policy.api.destroy_environment(self._data)
        del self._data

    @property
    def disposed(self) -> bool:
        """
        Checks if the environment is disposed
        """
        return hasattr(self, "_data")

    def __del__(self) -> None:
        if self.disposed:
            return

        import warnings

        warnings.warn(f"Disposing {self!r} inside __del__. This might cause leaks.", ResourceWarning)
        self.dispose()


class Policy(contextlib.AbstractContextManager["Policy"]):
    """
    A managed policy is a very simple policy that just stores the environment
    data within the given store.

    For convenience (especially for testing), this is a context manager that
    makes sure policies are being unregistered when leaving a block.
    """

    _managed: _ManagedPolicy

    def __init__(self, store: EnvironmentStore) -> None:
        self._managed = _ManagedPolicy(store)

    def register(self) -> None:
        """
        Registers the policy with VapourSynth.
        """
        register_policy(self._managed)

    def unregister(self) -> None:
        """
        Unregisters the policy from VapourSynth.
        """
        self._managed.api.unregister_policy()

    def __enter__(self) -> Self:
        self.register()
        return self

    def __exit__(self, _: type[BaseException] | None, __: BaseException | None, ___: TracebackType | None) -> None:
        self.unregister()

    def new_environment(self) -> ManagedEnvironment:
        """
        Creates a new VapourSynth core.

        You need to call `dispose()` on this environment when you are done
        using the new environment.

        For convenience, a managed environment will also serve as a
        context-manager that disposes the environment automatically.
        """
        data = self.api.create_environment()
        env = self.api.wrap_environment(data)
        logger.debug("Created new environment")
        return ManagedEnvironment(env, data, self)

    @property
    def api(self) -> EnvironmentPolicyAPI:
        """
        Returns the API instance for more complex interactions.

        You will rarely need to use this directly.
        """
        return self._managed.api

    @property
    def managed(self) -> _ManagedPolicy:
        """
        Returns the actual policy within VapourSynth.

        You will rarely need to use this directly.
        """
        return self._managed
