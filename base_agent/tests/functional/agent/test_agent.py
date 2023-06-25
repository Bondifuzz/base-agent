from typing import Callable
import multiprocessing as mp
from time import sleep
import os

from base_agent.app.abstract import Agent, AgentMode
from base_agent.app.errors import RelocRequiredError
from base_agent.app.errors.codes import (
    E_RESOURCE_LIMIT_EXCEEDED,
    E_PROGRAM_ABORTED,
    E_UNHANDLED_ERROR,
    E_NO_ERROR,
)

from base_agent.app.settings import AppSettings, FuzzerMode
from base_agent.app.storage.local.errors import LimitExceededError
from base_agent.app.storage.s3 import ObjectStorage
from base_agent.app.entry import agent_entry_inner

from .conftest import (
    default_agent_output,
    MyAgentRunner,
)


########################################
# Agent normal launch
########################################


class MyAgentMode(AgentMode):

    _settings: AppSettings

    def __init__(self, settings: AppSettings):
        self._settings = settings

    def run(
        self,
        storage: ObjectStorage,
        check_aborted: Callable,
        second_chance: bool,
    ):
        assert isinstance(storage, ObjectStorage)
        assert isinstance(check_aborted, Callable)
        assert isinstance(second_chance, bool)
        return default_agent_output()

    def abort(self):
        pass


class MyAgent(Agent):
    def select_mode(
        self,
        mode: FuzzerMode,
        settings: AppSettings,
    ) -> AgentMode:
        return MyAgentMode(settings)


def test_agent():

    """
    Description
        Ensure concrete implementation
        of `Agent` abstract class runs correctly

    Succeeds
        If no error occurred
    """

    agent_entry_inner(MyAgent())


########################################
# Agent launch with unhandled error
########################################


class MyAgentModeUnhandledError(AgentMode):

    _settings: AppSettings

    def __init__(self, settings: AppSettings):
        self._settings = settings

    def run(
        self,
        storage: ObjectStorage,
        check_aborted: Callable,
        second_chance: bool,
    ):
        raise Exception("Unhandled error")

    def abort(self):
        pass


def test_agent_unhandled_error(settings: AppSettings, storage: ObjectStorage):

    """
    Description
        Ensure `AgentRunner` catches unhandled errors
        which may happen inside `run` method of `AgentMode` class

    Succeeds
        If raised Exception is catched
        and output with error code is available
    """

    mode = MyAgentModeUnhandledError(settings)
    with MyAgentRunner(mode, args=(storage,)) as runner:

        runner.try_run(second_chance=False)

        output = runner.get_output()
        assert output and output.status.code == E_UNHANDLED_ERROR


########################################
# Agent launch interrupted
########################################


class MyAgentModeAborted(AgentMode):

    _settings: AppSettings
    _cleanup_called: bool

    def __init__(self, settings: AppSettings):
        self._settings = settings
        self._cleanup_called = False

    def run(
        self,
        storage: ObjectStorage,
        check_aborted: Callable,
        second_chance: bool,
    ):
        # Wait for SIGTERM
        sleep(2)

        # Ensure `abort` has been called
        assert self._cleanup_called == True

        # Ensure that provided checker is working
        # It must raise ProgramAbortedError
        check_aborted()

        # If checker is working we can't be there
        assert False, "Unreachable"

    def abort(self):
        self._cleanup_called = True


def agent_entry_aborted(settings: AppSettings, storage: ObjectStorage):

    mode = MyAgentModeAborted(settings)
    with MyAgentRunner(mode, args=(storage,)) as runner:

        runner.try_run(second_chance=False)
        assert runner.is_aborted()

        output = runner.get_output()
        assert output and output.status.code == E_PROGRAM_ABORTED


def test_agent_aborted(settings: AppSettings, storage: ObjectStorage):

    """
    Description
        Ensure `AgentRunner` is able to handle
        interrupts which may occur during its run

    Succeeds
        If interrupt has been catched and handled correctly
    """

    process = mp.Process(target=agent_entry_aborted, args=(settings, storage))
    process.start(); sleep(1); process.terminate(); process.join()  # fmt: skip
    assert process.exitcode == 0


########################################
# Agent launch with disk limit exceeded
########################################


class MyAgentModeDiskLimit(AgentMode):

    _settings: AppSettings

    def __init__(self, settings: AppSettings):
        self._settings = settings

    def run(
        self,
        storage: ObjectStorage,
        check_aborted: Callable,
        second_chance: bool,
    ):

        try:
            filename = "MyAgentModeDiskLimit"
            with open(filename, "wb") as f:
                f.write(b"A" * 100000)

            storage.refresh_consumed_space()

        except LimitExceededError as e:
            raise RelocRequiredError(str(e)) from e

        return default_agent_output()

    def abort(self):
        pass


def test_agent_disk_unlimited(settings: AppSettings, storage: ObjectStorage):

    """
    Description
        Ensure `AgentRunner` is able to handle
        local storage overflows may occur during its run

    Succeeds
        If interrupt has been catched and handled correctly
    """

    agent_mode = MyAgentModeDiskLimit(settings)
    with MyAgentRunner(agent_mode, args=(storage,)) as runner:

        #
        # Set limit on current dir and run mode
        # Method try_run will return true because
        # disk limit not exceeded and no retry will need
        #

        second_chance = runner.try_run(second_chance=True)
        assert second_chance == False

        #
        # Since no error occurred,
        # output with results will be returned
        #

        output = runner.get_output()
        assert output and output.status.code == E_NO_ERROR


def test_agent_disk_limited_with_retry(settings: AppSettings, storage: ObjectStorage):

    """
    Description
        Ensure `AgentRunner` is able to handle
        local storage overflows may occur during its run.

    Succeeds
        If overflow has been catched and handled correctly
    """

    agent_mode = MyAgentModeDiskLimit(settings)
    with MyAgentRunner(agent_mode, args=(storage,)) as runner:

        #
        # Set limit on current dir and run mode
        # Method try_run will return true because
        # disk limit exceeded and retry is allowed
        #

        storage.switch_workdir(os.getcwd(), 10000)  # Set limit on current dir
        second_chance = runner.try_run(second_chance=True)
        assert second_chance == True

        #
        # Since retry is allowed,
        # no output will be returned
        #

        output = runner.get_output()
        assert not output


def test_agent_disk_limited_without_retry(
    settings: AppSettings, storage: ObjectStorage
):

    """
    Description
        Ensure `AgentRunner` is able to handle
        local storage overflows may occur during its run.

    Succeeds
        If overflow has been catched and handled correctly
    """

    agent_mode = MyAgentModeDiskLimit(settings)
    with MyAgentRunner(agent_mode, args=(storage,)) as runner:

        #
        # Set limit on current dir and run mode
        # Method try_run will return false because
        # disk limit exceeded and retry is not allowed
        #

        storage.switch_workdir(os.getcwd(), 10000)
        second_chance = runner.try_run(second_chance=False)
        assert second_chance == False

        #
        # Since retry is not allowed,
        # output with error message will be returned
        #

        output = runner.get_output()
        assert output and output.status.code == E_RESOURCE_LIMIT_EXCEEDED
