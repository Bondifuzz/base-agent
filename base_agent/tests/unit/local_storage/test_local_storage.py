from tarfile import TarError
from io import BytesIO
import pytest
import os

from base_agent.app.storage.local import LocalStorage
from base_agent.app.storage.local.errors import LimitExceededError

from .conftest import (
    VOLUME_SIZE,
    gen_large_archive,
    gen_archive,
    gen_file,
)


########################################
# Storage itself
########################################


def test_local_storage_clear(storage: LocalStorage):

    """
    Description
        Write files to storage, then clear it

    Succeeds
        If storage was cleared successfully
    """

    dir1 = "some-dir"
    dir2 = "another-dir"
    filepath = "local_storage_move_contents"
    filesize = storage.capacity // 10

    # Create 4 files in storage
    os.mkdir(dir1)
    os.mkdir(dir2)  # fmt: off
    storage.add_file(f"{dir1}/{filepath}", gen_file(filesize))
    storage.add_file(filepath, gen_file(filesize))

    # Ensure, files exist before clear
    assert storage.consumed > 0
    assert os.path.exists(f"{filepath}")
    assert os.path.exists(f"{dir1}/{filepath}")
    assert os.path.exists(f"{dir1}")
    assert os.path.exists(f"{dir2}")

    # Clear storage
    storage.clear()

    # Ensure, files exist before clear
    assert storage.consumed == 0
    assert not os.path.exists(f"{filepath}")
    assert not os.path.exists(f"{dir1}/{filepath}")
    assert not os.path.exists(f"{dir1}")
    assert not os.path.exists(f"{dir2}")


def test_local_storage_move_contents(storage: LocalStorage, tempdir: str):

    """
    Description
        Move contents of storage to another directory

    Succeeds
        If files were moved successfully
    """

    dir1 = "some-dir"
    dir2 = "another-dir"
    filepath = "local_storage_move_contents"
    filesize = storage.capacity // 10

    # Create 4 files in storage
    os.mkdir(dir1)
    os.mkdir(dir2)  # fmt: off
    storage.add_file(f"{dir1}/{filepath}", gen_file(filesize))
    storage.add_file(filepath, gen_file(filesize))

    # Ensure, files exist before move
    assert storage.consumed > 0
    assert os.path.exists(f"{filepath}")
    assert os.path.exists(f"{dir1}/{filepath}")
    assert os.path.exists(f"{dir1}")
    assert os.path.exists(f"{dir2}")

    # Move files
    storage.move_contents(tempdir)

    # Ensure files were moved
    assert storage.consumed == 0
    assert len(os.listdir()) == 0
    assert os.path.exists(f"{tempdir}/{filepath}")
    assert os.path.exists(f"{tempdir}/{dir1}/{filepath}")
    assert os.path.exists(f"{tempdir}/{dir1}")
    assert os.path.exists(f"{tempdir}/{dir2}")


def test_local_storage_refresh(storage: LocalStorage):

    """
    Description
        Perform file system change operations, bypassing
        local storage monitoring mechanism.
        Then manually refresh consumed space

    Succeeds
        If changes were found after refresh operation
    """

    dir1 = "some-dir"
    filepath = "local_storage_refresh"
    filesize = storage.capacity // 10

    # Storage is empty
    assert storage.consumed == 0

    # Do external writes
    os.mkdir(dir1)

    with open(filepath, "wb") as f:
        f.write(b"A" * filesize)

    with open(f"{dir1}/{filepath}", "wb") as f:
        f.write(b"A" * filesize)

    # Storage has not tracked write operation
    assert storage.consumed == 0

    # Time to refresh
    storage.refresh()
    assert storage.consumed == filesize * 2


def test_local_storage_refresh_limit_exceeded(storage: LocalStorage):

    """
    Description
        Perform file system change operations, bypassing
        local storage monitoring mechanism. Trigger overflow.
        Then manually refresh consumed space

    Succeeds
        If refresh operation raised an exception
    """

    # Storage is empty
    assert storage.consumed == 0
    data = b"A" * (VOLUME_SIZE + 1)

    # Do external write
    filepath = "local_storage_refresh"
    with open(filepath, "wb") as f:
        f.write(data)

    # Time to refresh (boom!)
    with pytest.raises(LimitExceededError):
        storage.refresh()


########################################
# add_file
########################################


def test_add_file_path_invalid(storage: LocalStorage):

    """
    Description
        Try to add file using non-existent path

    Succeeds
        If exception raised
    """

    filename = os.path.join("a", "b")
    with pytest.raises(FileNotFoundError):
        storage.add_file(filename, BytesIO())


def test_add_file_empty(storage: LocalStorage):

    """
    Description
        Try to create empty file using provided API

    Succeeds
        If file was created successfully
    """

    filename = "add_file_empty"
    storage.add_file(filename, BytesIO())
    assert os.path.isfile(filename)


def test_add_file_normal(storage: LocalStorage):

    """
    Description
        Try to create file using provided API

    Succeeds
        If file was created successfully
    """

    filename = "add_file_normal"
    filesize = storage.capacity // 2
    storage.add_file(filename, gen_file(filesize))

    assert os.path.isfile(filename)
    assert os.path.getsize(filename) == filesize
    assert storage.consumed == filesize


def test_add_file_overwrite(storage: LocalStorage):

    """
    Description
        Try to create file using provided API.
        Then overwrite this file with another files.

    Succeeds
        If file is always being overwritten
        and storage consumed space is calculated correctly
    """

    filename = "add_file_overwrite"
    filesize = storage.capacity // 2

    # Add file
    storage.add_file(filename, gen_file(filesize))
    assert storage.consumed == filesize

    # Overwrite file with same file
    storage.add_file(filename, gen_file(filesize))
    assert storage.consumed == filesize

    # Overwrite file with larger file
    storage.add_file(filename, gen_file(filesize + 1))
    assert storage.consumed == filesize + 1

    # Overwrite file with smaller file
    storage.add_file(filename, gen_file(filesize - 1))
    assert storage.consumed == filesize - 1


@pytest.mark.parametrize("n", [2, 3, 5])
def test_add_file_many(storage: LocalStorage, n):

    """
    Description
        Try to create many files of different size using provided API.
        Ensure, storage tracks consumed space

    Succeeds
        If files were created successfully
        and consumed space calculated correctly
    """

    total_size = 0
    file_size = storage.capacity // (n * 10)

    for i in range(n):
        filename = f"add_file_many_{i}"
        storage.add_file(filename, gen_file(file_size + i * 10))
        total_size += os.path.getsize(filename)

    assert storage.consumed == total_size


def test_add_file_limit_exceeded(storage: LocalStorage):

    """
    Description
        Try to create file which exceeds storage limit.

    Succeeds
        If file creation failed and consumed space not changed
    """

    filename = "add_file_limit_exceeded"
    f = gen_file(storage.capacity + 1)

    # Storage is empty
    assert storage.consumed == 0

    # Do exhausting write
    with pytest.raises(LimitExceededError):
        storage.add_file(filename, f)

    # Nothing was written to storage
    assert storage.consumed == 0


@pytest.mark.parametrize("n", [2, 3, 5])
def test_add_file_many_limit_exceeded(storage: LocalStorage, n):

    """
    Description
        Try to create many files which exceed storage limit

    Succeeds
        Ensure, storage will not allow to create
        the file, which exceeds storage limit
    """

    total_size = 0
    file_size = storage.capacity // n

    # Do exhausting writes
    with pytest.raises(LimitExceededError):
        for i in range(n + 1):
            filename = f"add_file_many_limit_exceeded_{i}"
            storage.add_file(filename, gen_file(file_size))
            total_size += os.path.getsize(filename)

    # Ensure consumed space is less than limit
    assert storage.consumed <= storage.capacity
    assert storage.consumed == total_size


########################################
# add_archive
########################################


def test_add_archive_invalid(storage: LocalStorage):

    """
    Description
        Try to add files to storage from archive.
        But archive is malformed

    Succeeds
        If file was created successfully
    """

    with pytest.raises(TarError):
        storage.add_archive(BytesIO(b"abc"))


def test_add_archive_normal(storage: LocalStorage):

    """
    Description
        Try to add files to storage from archive.

    Succeeds
        If file was created successfully
    """

    dir1 = "some-dir"
    dir2 = "another-dir"
    filepath = "add_archive_normal"
    filesize = storage.capacity // 10

    0  # Add files to storage
    os.mkdir(dir1); os.mkdir(dir2)  # fmt: skip
    storage.add_file(filepath, gen_file(filesize))
    storage.add_file(f"{dir1}/{filepath}", gen_file(filesize))
    old_consumed = storage.consumed

    # Create in-memory archive
    # And clear storage
    f = gen_archive(".")
    storage.clear()

    # Ensure storage is empty
    assert len(os.listdir()) == 0

    # Add files from archive
    storage.add_archive(f)

    # Ensure files added
    assert storage.consumed == old_consumed
    assert os.path.isfile(f"{dir1}/{filepath}")
    assert os.path.isfile(filepath)
    assert os.path.isdir(dir1)
    assert os.path.isdir(dir2)

def test_add_archive_overwrite(storage: LocalStorage):

    """
    Description
        Try to add files to storage from archive.
        Then overwrite archive files with another files.

    Succeeds
        If files are always being overwritten
        and storage consumed space is calculated correctly
    """

    dir1 = "some-dir"
    dir2 = "another-dir"
    filepath = "add_archive_overwrite"
    filesize = storage.capacity // 10

    0  # Add files to storage
    os.mkdir(dir1); os.mkdir(dir2)  # fmt: skip
    storage.add_file(filepath, gen_file(filesize))
    storage.add_file(f"{dir1}/{filepath}", gen_file(filesize))
    old_consumed = storage.consumed

    # Create in-memory archive
    # And clear storage
    f = gen_archive(".")
    storage.clear()

    # Ensure storage is empty
    assert len(os.listdir()) == 0

    # Add files from archive
    storage.add_archive(f)
    assert storage.consumed == old_consumed
    f.seek(0)

    # Overwrite files from archive
    storage.add_archive(f)
    assert storage.consumed == old_consumed


@pytest.mark.parametrize("n_files, n_archives", [(2, 1), (2, 2), (5, 5)])
def test_add_archive_many(storage: LocalStorage, n_archives: int, n_files: int):

    """
    Description
        Try to add files to storage from many archives

    Succeeds
        If files were created successfully
        and consumed space calculated correctly
    """

    total_size = 0
    file_size = storage.capacity // (n_files * n_archives * 10)
    archives = []

    for i in range(n_archives):
        for j in range(n_files):
            filepath = f"add_archive_many_{i}_{j}"
            storage.add_file(filepath, gen_file(file_size))
            total_size += os.path.getsize(filepath)

        archives.append(gen_archive("."))
        storage.clear()

    for archive in archives:
        storage.add_archive(archive)

    assert len(os.listdir()) == n_files * n_archives
    assert storage.consumed == total_size


def test_add_archive_limit_exceeded(storage: LocalStorage):

    """
    Description
        Try to exceed storage limit adding large archive.

    Succeeds
        If adding files from archive failed
        and consumed space not changed
    """

    assert storage.consumed == 0

    filename = "add_archive_limit_exceeded"
    with pytest.raises(LimitExceededError):
        storage.add_archive(gen_large_archive(), filename)

    assert not os.path.exists(filename)
    assert storage.consumed == 0


@pytest.mark.parametrize("n_files, n_archives", [(2, 1), (2, 2), (5, 5)])
def test_add_archive_many_limit_exceeded(
    storage: LocalStorage, n_archives: int, n_files: int
):

    """
    Description
        Try to exceed storage limit adding many archives.

    Succeeds
        Ensure, storage will not allow to
        add archive, which exceeds storage limit
    """

    total_size = 0
    file_size = storage.capacity // (n_files * n_archives * 10)
    archives = []

    for i in range(n_archives):
        for j in range(n_files):
            filepath = f"add_archive_many_limit_exceeded_{i}_{j}"
            storage.add_file(filepath, gen_file(file_size))
            total_size += os.path.getsize(filepath)

        archives.append(gen_archive("."))
        storage.clear()

    for archive in archives:
        storage.add_archive(archive)

    filename = "add_archive_limit_exceeded"
    with pytest.raises(LimitExceededError):
        storage.add_archive(gen_large_archive(), filename)

    assert not os.path.exists(filename)
    assert storage.consumed == total_size


########################################
# remove
########################################


def test_remove_not_found(storage: LocalStorage):

    """
    Description
        Try to delete non-existent fs object

    Succeeds
        If exception raised
    """

    with pytest.raises(FileNotFoundError):
        storage.remove("no-such-file")


def test_remove_file(storage: LocalStorage):

    """
    Description
        Try to delete file from storage

    Succeeds
        If delete operation was successful
    """

    filepath = "remove_file"
    filesize = VOLUME_SIZE // 2
    storage.add_file(filepath, gen_file(filesize))
    assert storage.consumed > 0

    storage.remove(filepath)
    assert not os.path.exists(filepath)
    assert storage.consumed == 0


def test_remove_folder(storage: LocalStorage):

    """
    Description
        Try to delete folder from storage

    Succeeds
        If delete operation was successful
    """

    dirname = "dir"
    filename = "remove_folder"
    filesize = VOLUME_SIZE // 2

    # Create file and move it to folder
    os.mkdir(dirname)
    f = gen_file(filesize)
    storage.add_file(f"{dirname}/{filename}", f)
    assert storage.consumed > 0

    # Ensure, folder was removed recursively
    storage.remove(dirname)
    assert not os.path.exists(dirname)
    assert not os.path.exists(filename)
    assert storage.consumed == 0
