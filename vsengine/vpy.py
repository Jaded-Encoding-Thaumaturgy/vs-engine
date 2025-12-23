# vs-engine
# Copyright (C) 2022  cid-chan
# Copyright (C) 2025  Jaded-Encoding-Thaumaturgy
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""
vsengine.vpy runs vpy-scripts for you.

    >>> load_script("/path/to/my/script").result()
    >>> load_code("print('Hello, World!')").result()

load_script() and load_code() will create a Script-object which allows
you to run the script and access its environment.

load_script() takes a path as the first argument while load_code() accepts
code (either compiled, parsed or as a string/bytes) and returns the Script-
object.

Both methods accept an optional second argument which can either be an
environment or a policy. If it's an environment, it will run the script
in that environment. If it's a policy, it will create a new environment and
store the environment within the environment-attribute of the Script-instance,
which you have to dispose manually.

Additional keyword arguments include inline, which defaults to true, will
run the script in a separate worker thread, when set to false. Another
keyword argument is chdir, which will change the current directory during
execution.

A Script object has the function run() which returns a future which will
reject with ExecutionFailed or with resolve with None.


A Script-instance is awaitable, in which it will await the completion of the
script.
"""

from __future__ import annotations

import ast
import os
import runpy
import textwrap
import traceback
from collections.abc import Awaitable, Buffer, Callable, Generator
from concurrent.futures import Future
from contextlib import AbstractContextManager
from types import CodeType, ModuleType, NoneType, TracebackType
from typing import Any, Concatenate, Self, overload

import vapoursynth as vs

from ._futures import UnifiedFuture, unified
from .loops import make_awaitable, to_thread
from .policy import ManagedEnvironment, Policy

__all__ = ["ExecutionFailed", "load_code", "load_script"]

type Runner[R] = Callable[[Callable[[], R]], Future[R]]
type Executor[T] = Callable[[WrapAllErrors, ModuleType], T]


class ExecutionFailed(Exception):  # noqa: N818
    """
    Exception raised when script execution fails.
    """

    #: It contains the actual exception that has been raised.
    parent_error: BaseException

    def __init__(self, parent_error: BaseException) -> None:
        """
        Initialize the ExecutionFailed exception.

        :param parent_error: The original exception that occurred.
        """
        msg = textwrap.indent(self.extract_traceback(parent_error), "| ")
        super().__init__(f"An exception was raised while running the script.\n{msg}")
        self.parent_error = parent_error

    @staticmethod
    def extract_traceback(error: BaseException) -> str:
        """
        Extract and format the traceback from an exception.

        :param error: The exception to extract the traceback from.
        :return: A formatted string containing the traceback.
        """
        msg = traceback.format_exception(type(error), error, error.__traceback__)
        msg = "".join(msg)
        return msg


class WrapAllErrors(AbstractContextManager[None]):
    """
    Context manager that wraps exceptions in ExecutionFailed.
    """

    def __enter__(self) -> None: ...

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        if val is not None:
            raise ExecutionFailed(val) from None


def inline_runner[T](func: Callable[[], T]) -> Future[T]:
    """
    Runs a function inline and returns the result as a Future.

    :param func: The function to run.
    :return: A future containing the result or exception of the function.
    """
    fut = Future[T]()
    try:
        result = func()
    except BaseException as e:
        fut.set_exception(e)
    else:
        fut.set_result(result)
    return fut


def chdir_runner[**P, R](
    dir: str | os.PathLike[str], parent: Runner[R]
) -> Callable[Concatenate[Callable[P, R], P], Future[R]]:
    """
    Wraps a runner to change the current working directory during execution.

    :param dir: The directory to change to.
    :param parent: The runner to wrap.
    :return: A wrapped runner function.
    """

    def runner(func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        def _wrapped() -> R:
            current = os.getcwd()
            os.chdir(dir)

            try:
                f = func(*args, **kwargs)
                return f
            except Exception as e:
                print(e)
                raise
            finally:
                os.chdir(current)

        return parent(_wrapped)

    return runner


class AbstractScript[EnvironmentT: (vs.Environment, ManagedEnvironment)](Awaitable[None]):
    """
    Base class for VapourSynth script wrappers.

    Handles execution and variable retrieval for scripts.
    """

    environment: EnvironmentT

    _future: Future[None]

    def __init__(
        self,
        executor: Executor[None],
        module: ModuleType,
        environment: EnvironmentT,
        runner: Runner[None],
    ) -> None:
        self.executor = executor
        self.environment = environment
        self.runner = runner
        self.module = module

    def __await__(self) -> Generator[Any, None, None]:
        """
        Runs the script and waits until the script has completed.
        """
        return self.run_async().__await__()

    async def run_async(self) -> None:
        """
        Runs the script asynchronously, but it returns a coroutine.
        """
        return await make_awaitable(self.run())

    def run(self) -> Future[None]:
        """
        Runs the script.

        It returns a future which completes when the script completes.
        When the script fails, it raises a ExecutionFailed.
        """
        if hasattr(self, "_future"):
            return self._future

        self._future = self.runner(self._run_inline)

        return self._future

    def result(self) -> None:
        """
        Runs the script and blocks until the script has finished running.
        """
        return self.run().result()

    @overload
    @unified(kind="future")
    def get_variable(self, name: str, default: None = None) -> Future[Any | None]: ...
    @overload
    @unified(kind="future")
    def get_variable[T](self, name: str, default: T) -> Future[Any | T]: ...
    @unified(kind="future")
    def get_variable(self, name: str, default: Any = None) -> Future[Any]:
        """
        Retrieve a variable from the script's module.

        :param name: The name of the variable to retrieve.
        :param default: The default value if the variable is not found.
        :return: A future that resolves to the variable's value.
        """
        return UnifiedFuture[Any].resolve(getattr(self.module, name, default))

    def _run_inline(self) -> None:
        with self.environment.use():
            self.executor(WrapAllErrors(), self.module)


class Script(AbstractScript[vs.Environment]):
    """
    A VapourSynth script wrapper for unmanaged environments.
    """


class ManagedScript(AbstractScript[ManagedEnvironment], AbstractContextManager["ManagedScript"]):
    """
    A VapourSynth script wrapper for managed environments.
    """

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        self.dispose()

    def dispose(self) -> None:
        """
        Disposes the managed environment.
        """
        self.environment.dispose()


@overload
def load_script(
    script: str | os.PathLike[str],
    environment: vs.Environment | None = None,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> Script: ...


@overload
def load_script(
    script: str | os.PathLike[str],
    environment: Script,
    *,
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> Script: ...


@overload
def load_script(
    script: str | os.PathLike[str],
    environment: Policy | ManagedEnvironment,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> ManagedScript: ...


@overload
def load_script(
    script: str | os.PathLike[str],
    environment: ManagedScript,
    *,
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> ManagedScript: ...


def load_script(
    script: str | os.PathLike[str],
    environment: Policy | vs.Environment | Script | ManagedEnvironment | ManagedScript | None = None,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> AbstractScript[Any]:
    """
    Runs the script at the given path.

    :param script: The path to the script file to run.
    :param environment: Defines the environment in which the code should run. If passed
                        a Policy, it will create a new environment from the policy, which
                        can be accessed using the environment attribute.
    :param module: The name the module should get. Defaults to __vapoursynth__.
    :param inline: Run the code inline, e.g. not in a separate thread.
    :param chdir: Change the currently running directory while the script is running.
                  This is unsafe when running multiple scripts at once.
    :returns: A script object. The script starts running when you call run() on it,
              or await it.
    """

    def _execute(ctx: WrapAllErrors, module: ModuleType) -> None:
        with ctx:
            runpy.run_path(str(script), module.__dict__, module.__name__)

    return _load(_execute, environment, module, inline, chdir)


@overload
def load_code(
    script: str | Buffer | ast.Module | ast.Expression | ast.Interactive | CodeType,
    environment: vs.Environment | None = None,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> Script: ...


@overload
def load_code(
    script: str | Buffer | ast.Module | ast.Expression | ast.Interactive | CodeType,
    environment: Script,
    *,
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> Script: ...


@overload
def load_code(
    script: str | Buffer | ast.Module | ast.Expression | ast.Interactive | CodeType,
    environment: Policy | ManagedEnvironment,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> ManagedScript: ...


@overload
def load_code(
    script: str | Buffer | ast.Module | ast.Expression | ast.Interactive | CodeType,
    environment: ManagedScript,
    *,
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> ManagedScript: ...


def load_code(
    script: str | Buffer | ast.Module | ast.Expression | ast.Interactive | CodeType,
    environment: Policy | vs.Environment | Script | ManagedEnvironment | ManagedScript | None = None,
    *,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> AbstractScript[Any]:
    """
    Runs the given code snippet.

    :param script: The code to run. Can be a string, bytes, AST, or compiled code.
    :param environment: Defines the environment in which the code should run. If passed
                        a Policy, it will create a new environment from the policy, which
                        can be accessed using the environment attribute. If the environment
                        is another Script, it will take the environment and module of the
                        script.
    :param module: The name the module should get. Defaults to __vapoursynth__.
    :param inline: Run the code inline, e.g. not in a separate thread.
    :param chdir: Change the currently running directory while the script is running.
                  This is unsafe when running multiple scripts at once.
    :returns: A script object. The script starts running when you call run() on it,
              or await it.
    """

    def _execute(ctx: WrapAllErrors, module: ModuleType) -> None:
        nonlocal script

        with ctx:
            if isinstance(script, CodeType):
                code = script
            else:
                code = compile(script, filename="<runvpy>", dont_inherit=True, flags=0, mode="exec")

            exec(code, module.__dict__, module.__dict__)

    return _load(_execute, environment, module, inline, chdir)


def _load(
    executor: Executor[None],
    environment: Policy | vs.Environment | Script | ManagedEnvironment | ManagedScript | None = None,
    module: str | ModuleType = "__vapoursynth__",
    inline: bool = True,
    chdir: str | os.PathLike[str] | None = None,
) -> AbstractScript[Any]:
    runner = inline_runner if inline else to_thread

    if chdir is not None:
        runner = chdir_runner(chdir, runner)

    if isinstance(environment, AbstractScript):
        module = environment.module
        environment = environment.environment

    if isinstance(module, str):
        module = ModuleType(module)

    if isinstance(environment, (vs.Environment, NoneType)):
        if environment is None:
            environment = vs.get_current_environment()

        return Script(executor, module, environment, runner)

    if isinstance(environment, Policy):
        environment = environment.new_environment()

    return ManagedScript(executor, module, environment, runner)
