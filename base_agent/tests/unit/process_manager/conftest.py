from tempfile import mkdtemp
from shutil import rmtree
from time import sleep
import pytest
import os


@pytest.fixture(autouse=True)
def tempdir_for_tests():
    tmp = mkdtemp()
    old_cwd = os.getcwd()
    os.chdir(tmp)
    yield tmp
    os.chdir(old_cwd)
    rmtree(tmp)


def eat_ram(amount: int, chunk_size: int, hold_time: int):

    pages = []
    chunk_size = 10 ** 6
    cnt = amount // chunk_size + 1

    for _ in range(cnt):
        pages.append(b"A" * chunk_size)

    sleep(hold_time)


def eat_disk(amount: int, chunk_size: int, hold_time: int):

    chunk_size = 10 ** 6
    cnt = amount // chunk_size + 1

    for i in range(cnt):
        with open(f"file{i}", "wb") as f:
            f.write(b"A" * chunk_size)

    sleep(hold_time)


def eat_time(amount: int):
    sleep(amount)
