from io import BytesIO
from tempfile import mkdtemp
from shutil import rmtree
import tarfile
import pytest
import os

from base_agent.app.storage.local.storage import LocalStorage

VOLUME_SIZE = 50000


@pytest.fixture(autouse=True)
def tempdir_for_tests():
    tmp = mkdtemp()
    old_cwd = os.getcwd()
    os.chdir(tmp)
    yield tmp
    os.chdir(old_cwd)
    rmtree(tmp)


@pytest.fixture()
def tempdir():
    tmp = mkdtemp()
    yield tmp
    rmtree(tmp)


@pytest.fixture()
def storage():
    storage = LocalStorage(VOLUME_SIZE)
    storage.clear()
    yield storage
    storage.clear()


def gen_file(size: int):
    return BytesIO(b"A" * size)


def gen_archive(filepath: str) -> BytesIO:

    out = BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        tar.add(filepath)

    out.seek(0)
    return out


def gen_large_archive() -> BytesIO:

    size = VOLUME_SIZE * 2
    tarinfo = tarfile.TarInfo("big-tarfile")
    tarinfo.size = size

    out = BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tar:
        tar.addfile(tarinfo, BytesIO(os.urandom(size)))

    out.seek(0)
    return out
