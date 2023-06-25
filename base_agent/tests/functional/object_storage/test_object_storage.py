import os
import shutil
import pytest
import filecmp

from base_agent.app.storage.s3.errors import ObjectNotFoundError
from .conftest import (
    gen_file,
    gen_bytes,
    gen_many_files,
    random_string,
    ObjectStorage,
    FILE_SIZE,
)

def test_upload_download_bytes(storage: ObjectStorage, bucket: str):

    bytes_before = gen_bytes(FILE_SIZE)
    bytes_after = bytes()

    key = random_string()
    storage.upload_bytes(bytes_before, bucket, key)
    bytes_after = storage.download_bytes(bucket, key)

    assert bytes_before == bytes_after


def test_upload_download_file(storage: ObjectStorage, bucket: str):

    file_before = gen_file(FILE_SIZE)
    file_after = f"{file_before}-after"

    key = random_string()
    storage.upload_file(file_before, bucket, key)
    storage.download_file(bucket, key, file_after)

    assert filecmp.cmp(file_before, file_after, shallow=False)


def test_download_not_found(storage: ObjectStorage, bucket: str):

    key = "no-such-key"
    filename = "filename"

    with pytest.raises(ObjectNotFoundError):
        storage.download_file(bucket, key, filename)

    with pytest.raises(ObjectNotFoundError):
        storage.download_file_gzipped(bucket, key, filename)

    with pytest.raises(ObjectNotFoundError):
        storage.download_archive(bucket, key, filename)

    storage.download_many_files(bucket, key, filename)
    storage.download_many_archives(bucket, key, filename)


def test_upload_download_file_gzipped(storage: ObjectStorage, bucket: str):

    file_before = gen_file(FILE_SIZE)
    file_after = f"{file_before}-after"

    key = random_string()
    storage.upload_file_gzipped(file_before, bucket, key)
    storage.download_file_gzipped(bucket, key, file_after)

    assert filecmp.cmp(file_before, file_after, shallow=False)


@pytest.mark.parametrize("n", [1, 2, 5])
def test_upload_download_archive(storage: ObjectStorage, bucket: str, n: int):

    dir_before = "dir-before"
    dir_after = "dir-after"
    key = random_string()

    os.mkdir(dir_before)
    files = gen_many_files(dir_before, n)

    storage.upload_archive(dir_before, bucket, key)
    storage.download_archive(bucket, key, dir_after)

    eq, diff, _ = filecmp.cmpfiles(dir_before, dir_after, files, False)
    assert len(diff) == 0
    assert len(eq) == n


@pytest.mark.parametrize("n", [1, 2, 5])
def test_upload_download_many_files(storage: ObjectStorage, bucket: str, n: int):

    dir_before = "dir-before"
    dir_after = "dir-after"
    prefix = random_string()

    os.mkdir(dir_before)
    files = gen_many_files(dir_before, n)

    for file in files:
        key = f"{prefix}/{file}"
        filepath = f"{dir_before}/{file}"
        storage.upload_file(filepath, bucket, key)

    os.mkdir(dir_after)
    storage.download_many_files(bucket, prefix, dir_after)

    eq, diff, _ = filecmp.cmpfiles(dir_before, dir_after, files, False)
    assert len(diff) == 0
    assert len(eq) == n


@pytest.mark.parametrize(
    argnames=["n_files","n_archives"],
    argvalues=[(1, 1), (1, 2), (2, 2), (5, 5)]
)
def test_upload_download_many_archives(
    storage: ObjectStorage,
    bucket: str,
    n_files: int,
    n_archives,
):

    dir_before = "dir-before"
    dir_after = "dir-after"
    prefix = random_string()
    os.mkdir(dir_before)

    files = []
    for i in range(n_archives):

        dir_name = f"{dir_before}-{i}"
        key = f"{prefix}/{dir_name}"

        os.mkdir(dir_name)
        dir_files = gen_many_files(dir_name, n_files)
        storage.upload_archive(dir_name, bucket, key)
        files.extend(dir_files)

        for file in dir_files:
            shutil.move(f"{dir_name}/{file}", dir_before)

    os.mkdir(dir_after)
    storage.download_many_archives(bucket, prefix, dir_after)

    eq, diff, _ = filecmp.cmpfiles(dir_before, dir_after, files, False)
    assert len(eq) == n_archives * n_files
    assert len(diff) == 0
