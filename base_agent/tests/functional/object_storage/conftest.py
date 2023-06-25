from contextlib import suppress
from tempfile import mkdtemp
import pytest
import shutil
import os

from pydantic import (
    Field,
    BaseSettings,
    BaseModel,
    AnyUrl,
)

from base_agent.app.storage.s3 import ObjectStorage
from base_agent.app import utils


# fmt: off
with suppress(ModuleNotFoundError):
    import dotenv; dotenv.load_dotenv() # type: ignore
# fmt: on

FILE_SIZE = 1000


class ObjectStorageSettings(BaseSettings):
    url: AnyUrl = Field(env="S3_URL")
    access_key: str = Field(env="S3_ACCESS_KEY")
    secret_key: str = Field(env="S3_SECRET_KEY")
    test_bucket: str = Field(env="S3_TEST_BUCKET")


class AppSettings(BaseModel):
    object_storage: ObjectStorageSettings


@pytest.fixture(autouse=True)
def tempdir_for_tests():
    tmp = mkdtemp()
    old_cwd = os.getcwd()
    os.chdir(tmp)
    yield tmp
    os.chdir(old_cwd)
    shutil.rmtree(tmp)


@pytest.fixture(scope="session")
def settings():
    return AppSettings(object_storage=ObjectStorageSettings())


@pytest.fixture(scope="session")
def bucket(settings: AppSettings):
    return settings.object_storage.test_bucket


@pytest.fixture(scope="session")
def storage(settings: AppSettings, bucket: str):
    storage = ObjectStorage(settings)
    storage.clear_bucket(bucket)
    yield storage


def random_string():
    return utils.random_string(16)


def gen_bytes(size: int):
    return utils.random_string(size).encode()


def gen_file(size: int):

    filename = random_string()
    with open(filename, "w", encoding="utf-8") as f:
        f.write(utils.random_string(size))

    return filename


def gen_many_files(dir_name: str, file_cnt: int):

    files = []
    for _ in range(file_cnt):
        file = gen_file(FILE_SIZE)
        shutil.move(file, dir_name)
        files.append(file)

    return files
