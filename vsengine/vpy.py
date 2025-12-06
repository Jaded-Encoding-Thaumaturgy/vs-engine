# vs-engine
# Copyright (C) 2022  cid-chan
# This project is licensed under the EUPL-1.2
# SPDX-License-Identifier: EUPL-1.2
"""
vsengine.vpy runs vpy-scripts for you.

    >>> script("/path/to/my/script").result()
    >>> code("print('Hello, World!')").result()

script() and code() will create a Script-object which allows
you to run the script and access its environment.

script() takes a path as the first argument while code() accepts
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

A convenience function called execute() which will block
until the script has run.

A Script-instance is awaitable, in which it will await the completion of the
script.
"""

import ast
import os
import runpy
import textwrap
import traceback
import types
from collections.abc import Awaitable, Callable, Generator, Mapping
from concurrent.futures import Future
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, Self

from vapoursynth import Environment, get_current_environment

from vsengine._futures import UnifiedFuture, unified
from vsengine.loops import make_awaitable, to_thread
from vsengine.policy import ManagedEnvironment, Policy

type Runner[T] = Callable[[Callable[[], T]], Future[T]]
Executor = Callable[[AbstractContextManager[None], types.ModuleType], None]


__all__ = ["ExecutionFailed", "code", "script", "variables"]


class ExecutionFailed(Exception):  # noqa: N818
    #: It contains the actual exception that has been raised.
    parent_error: BaseException

    def __init__(self, parent_error: BaseException) -> None:
        msg = textwrap.indent(self.extract_traceback(parent_error), "| ")
        super().__init__(f"An exception was raised while running the script.\n{msg}")
        self.parent_error = parent_error

    @staticmethod
    def extract_traceback(error: BaseException) -> str:
        msg = traceback.format_exception(type(error), error, error.__traceback__)
        msg = "".join(msg)
        return msg


class WrapAllErrors(AbstractContextManager[None]):
    def __enter__(self) -> None: ...

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        if val is not None:
            raise ExecutionFailed(val) from None


def inline_runner[T](func: Callable[[], T]) -> Future[T]:
    fut = Future[T]()
    try:
        result = func()
    except BaseException as e:
        fut.set_exception(e)
    else:
        fut.set_result(result)
    return fut


def chdir_runner[T](dir: os.PathLike[str], parent: Runner[T]) -> Runner[T]:
    def runner(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Future[T]:
        def _wrapped() -> T:
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


class Script(AbstractContextManager["Script"], Awaitable["Script"]):
    environment: Environment | ManagedEnvironment

    def __init__(
        self,
        what: Executor,
        module: types.ModuleType,
        environment: Environment | ManagedEnvironment,
        runner: Runner[Self],
    ) -> None:
        self.what = what
        self.environment = environment
        self.runner = runner
        self.module = module
        self._future: Future[Self] | None = None

    def _run_inline(self) -> Self:
        with self.environment.use():
            self.what(WrapAllErrors(), self.module)
        return self

    ###
    # Public API

    @unified(kind="future")
    def get_variable(self, name: str, default: str | None = None) -> Future[str | None]:
        return UnifiedFuture.resolve(getattr(self.module, name, default))

    def run(self) -> Future[Self]:
        """
        Runs the script.

        It returns a future which completes when the script completes.
        When the script fails, it raises a ExecutionFailed.
        """
        if self._future is None:
            self._future = self.runner(self._run_inline)
        return self._future

    def result(self) -> Self:
        """
        Runs the script and blocks until the script has finished running.
        """
        return self.run().result()

    def dispose(self) -> None:
        """
        Disposes the managed environment.
        """
        if not isinstance(self.environment, ManagedEnvironment):
            raise ValueError("You can only scripts backed by managed environments")
        self.environment.dispose()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc: type[BaseException] | None, val: BaseException | None, tb: TracebackType | None) -> None:
        if isinstance(self.environment, ManagedEnvironment):
            self.dispose()

    async def run_async(self) -> Self:
        """
        Runs the script asynchronously, but it returns a coroutine.
        """
        return await make_awaitable(self.run())

    def __await__(self) -> Generator[Any, None, Self]:
        """
        Runs the script and waits until the script has completed.
        """
        return self.run_async().__await__()


EnvironmentType = Environment | ManagedEnvironment | Policy | Script


def script(
    script: os.PathLike[str],
    environment: EnvironmentType | None = None,
    *,
    module_name: str = "__vapoursynth__",
    inline: bool = True,
    chdir: os.PathLike[str] | None = None,
) -> Script:
    """
    Runs the script at the given path.

    :param path: If path is a path, the interpreter will run the file behind that path.
                 Otherwise it will execute it itself.
    :param environment: Defines the environment in which the code should run. If passed
                        a Policy, it will create a new environment from the policy, which
                        can be acessed using the environment attribute.
    :param module_name: The name the module should get. Defaults to __vapoursynth__.
    :param inline: Run the code inline, e.g. not in a separate thread.
    :param chdir: Change the currently running directory while the script is running.
                  This is unsafe when running multiple scripts at once.
    :returns: A script object. It script starts running when you call start() on it,
              or await it.
    """

    def _execute(ctx: AbstractContextManager[None], module: types.ModuleType) -> None:
        with ctx:
            runpy.run_path(str(script), module.__dict__, module.__name__)

    return _load(_execute, environment, module_name=module_name, inline=inline, chdir=chdir)


def variables(
    variables: Mapping[str, str],
    environment: EnvironmentType | None = None,
    *,
    module_name: str = "__vapoursynth__",
    inline: bool = True,
    chdir: os.PathLike[str] | None = None,
) -> Script:
    """
    Sets variables to the module.

    :param path: If path is a path, the interpreter will run the file behind that path.
                 Otherwise it will execute it itself.
    :param environment: Defines the environment in which the code should run. If passed
                        a Policy, it will create a new environment from the policy, which
                        can be acessed using the environment attribute. If the environment
                        is another Script, it will take the environment and module of the
                        script.
    :param module_name: The name the module should get. Defaults to __vapoursynth__.
    :param inline: Run the code inline, e.g. not in a separate thread.
    :param chdir: Change the currently running directory while the script is running.
                  This is unsafe when running multiple scripts at once.
    :returns: A script object. It script starts running when you call start() on it,
              or await it.
    """

    def _execute(ctx: AbstractContextManager[None], module: types.ModuleType) -> None:
        with ctx:
            for k, v in variables.items():
                setattr(module, k, v)

    return _load(_execute, environment, module_name=module_name, inline=inline, chdir=chdir)


def code(
    script: str | bytes | ast.Module | types.CodeType,
    environment: EnvironmentType | None = None,
    *,
    module_name: str = "__vapoursynth__",
    inline: bool = True,
    chdir: os.PathLike[str] | None = None,
) -> Script:
    """
    Runs the given code snippet.

    :param path: If path is a path, the interpreter will run the file behind that path.
                 Otherwise it will execute it itself.
    :param environment: Defines the environment in which the code should run. If passed
                        a Policy, it will create a new environment from the policy, which
                        can be acessed using the environment attribute. If the environment
                        is another Script, it will take the environment and module of the
                        script.
    :param module_name: The name the module should get. Defaults to __vapoursynth__.
    :param inline: Run the code inline, e.g. not in a separate thread.
    :param chdir: Change the currently running directory while the script is running.
                  This is unsafe when running multiple scripts at once.
    :returns: A script object. It script starts running when you call start() on it,
              or await it.
    """

    def _execute(ctx: AbstractContextManager[None], module: types.ModuleType) -> None:
        nonlocal script

        with ctx:
            if isinstance(script, types.CodeType):
                code = script
            else:
                code = compile(script, filename="<runvpy>", dont_inherit=True, flags=0, mode="exec")
            exec(code, module.__dict__, module.__dict__)

    return _load(_execute, environment, module_name=module_name, inline=inline, chdir=chdir)


def _load(
    script: Executor,
    environment: EnvironmentType | None = None,
    *,
    module_name: str = "__vapoursynth__",
    inline: bool = True,
    chdir: os.PathLike[str] | None = None,
) -> Script:
    runner = inline_runner if inline else to_thread

    module = environment.module if isinstance(environment, Script) else types.ModuleType(module_name)

    if isinstance(environment, Script):
        environment = environment.environment
    elif isinstance(environment, Policy):
        environment = environment.new_environment()
    elif environment is None:
        environment = get_current_environment()

    if chdir is not None:
        runner = chdir_runner(chdir, runner)

    return Script(script, module, environment, runner)
