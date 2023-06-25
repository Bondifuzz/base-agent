from contextlib import suppress
from tempfile import mkdtemp
from shutil import rmtree
from typing import Optional
import pytest
import os

from base_agent.app.abstract import AgentMode
from base_agent.app.entry import AgentRunner

from base_agent.app.errors.codes import E_SUCCESS
from base_agent.app.errors import AgentError

from base_agent.app.utils import rfc3339_now
from base_agent.app.settings import AppSettings, load_app_settings
from base_agent.app.storage.s3.storage import ObjectStorage
from base_agent.app.output import (
    AgentOutput,
    FuzzingResults,
    Metrics,
    Statistics,
    Status,
)

# fmt: off
with suppress(ModuleNotFoundError):
    import dotenv; dotenv.load_dotenv() # type: ignore
# fmt: on


@pytest.fixture(autouse=True)
def tempdir_for_tests():
    tmp = mkdtemp()
    old_cwd = os.getcwd()
    os.chdir(tmp)
    yield tmp
    os.chdir(old_cwd)
    rmtree(tmp)


@pytest.fixture(scope="session")
def settings():
    return load_app_settings()


@pytest.fixture(scope="session", autouse=True)
def storage(settings: AppSettings):

    storage = ObjectStorage(settings)
    bucket_fuzzers = settings.object_storage.buckets.fuzzers
    bucket_data = settings.object_storage.buckets.data

    storage.clear_bucket(bucket_fuzzers)
    storage.clear_bucket(bucket_data)
    yield storage


def default_agent_output():
    return AgentOutput(
        status=Status(code=E_SUCCESS),
        fuzzing=FuzzingResults(
            metrics=Metrics(disk=0, ram=0, time=0),
            statistics=Statistics(
                start_time=rfc3339_now(),
                finish_time=rfc3339_now(),
            ),
        ),
    )


class MyAgentRunner(AgentRunner):

    _output: Optional[AgentOutput]

    def __init__(self, agent_mode: AgentMode, args: tuple):
        super().__init__(agent_mode, args)
        self._output = None

    def _dump_ok(self, output: AgentOutput):
        self._output = output

    def _dump_err(self, e: AgentError, exc_info=False):
        self._output = AgentOutput(status=Status(code=e.code, message=e.message))

    def get_output(self):
        return self._output

    def is_aborted(self):
        return self._catched
