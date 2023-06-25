from logging import Logger, getLogger
from typing import List

from .settings import AppSettings

from .storage.s3 import (
    ObjectStorage,
    ObjectNotFoundError,
    ObjectStorageError,
)

from .errors import (
    RemoteFileDeleteError,
    RemoteFileLookupError,
    FileDownloadError,
    FileUploadError,
)


class BucketFuzzers:

    """Object paths in `fuzzers` bucket"""

    def __init__(self, name: str):
        self.name = name

    def binaries(self, fuzzer_id, fuzzer_rev, ext=".tar.gz"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/binaries{ext}"

    def seeds(self, fuzzer_id, fuzzer_rev, ext=".tar.gz"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/seeds{ext}"

    def config(self, fuzzer_id, fuzzer_rev, ext=".json"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/config{ext}"

    def fuzzer_dir(self, fuzzer_id):
        return self.name, fuzzer_id

    def revision_dir(self, fuzzer_id, fuzzer_rev):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}"


class BucketData:

    """Object paths in `data` bucket"""

    def __init__(self, name: str):
        self.name = name

    def merged_corpus(self, fuzzer_id, fuzzer_rev, ext=".tar.gz"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/corpus/corpus{ext}"

    def unmerged_corpus(self, fuzzer_id, fuzzer_rev, run_id, ext=".tar.gz"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/corpus/tmp/{run_id}{ext}"

    def unmerged_corpus_list(self, fuzzer_id, fuzzer_rev):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/corpus/tmp"

    def crash(self, fuzzer_id, fuzzer_rev, input_id, ext=".bin"):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}/crashes/{input_id}{ext}"

    def fuzzer_dir(self, fuzzer_id):
        return self.name, fuzzer_id

    def revision_dir(self, fuzzer_id, fuzzer_rev):
        return self.name, f"{fuzzer_id}/{fuzzer_rev}"


class FileTransfer:

    _bucket_fuzzers: BucketFuzzers
    _bucket_data: BucketData

    _storage: ObjectStorage
    _logger: Logger

    def __init__(self, storage: ObjectStorage, settings: AppSettings):

        bucket_fuzzers = settings.object_storage.buckets.fuzzers
        bucket_data = settings.object_storage.buckets.data
        self._bucket_fuzzers = BucketFuzzers(bucket_fuzzers)
        self._bucket_data = BucketData(bucket_data)

        self._fuzzer_id = settings.fuzzer.id
        self._fuzzer_rev = settings.fuzzer.rev
        self._logger = getLogger("transfer")
        self._storage = storage

    def download_binaries(self, dir_save_to: str):

        bucket, key = self._bucket_fuzzers.binaries(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._logger.info("Downloading binaries")
            self._storage.download_archive(bucket, key, dir_save_to)

        except ObjectNotFoundError as e:
            raise RemoteFileLookupError("binaries") from e

        except ObjectStorageError as e:
            self._logger.error("Failed to download binaries: '%s'", e)
            raise FileDownloadError("binaries") from e

        self._logger.info("Binaries downloaded to '%s'", dir_save_to)

    def download_seeds(self, dir_save_to: str):

        bucket, key = self._bucket_fuzzers.seeds(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._logger.info("Downloading seeds")
            self._storage.download_archive(bucket, key, dir_save_to)

        except ObjectNotFoundError as e:
            raise RemoteFileLookupError("seeds") from e

        except ObjectStorageError as e:
            self._logger.error("Failed to download seeds: '%s'", e)
            raise FileDownloadError("seeds") from e

        self._logger.info("Seeds downloaded to '%s'", dir_save_to)

    def download_config(self, path_save_to: str):

        bucket, key = self._bucket_fuzzers.config(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._logger.info("Downloading config")
            self._storage.download_file(bucket, key, path_save_to)

        except ObjectNotFoundError as e:
            raise RemoteFileLookupError("config") from e

        except ObjectStorageError as e:
            self._logger.error("Failed to download config: '%s'", e)
            raise FileDownloadError("config") from e

        self._logger.info("Config downloaded to '%s'", path_save_to)

    def download_merged_corpus(self, dir_save_to: str):

        bucket, key = self._bucket_data.merged_corpus(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._logger.info("Downloading merged corpus")
            self._storage.download_archive(bucket, key, dir_save_to)

        except ObjectNotFoundError as e:
            raise RemoteFileLookupError("merged_corpus") from e

        except ObjectStorageError as e:
            self._logger.error("Failed to download merged corpus: '%s'", e)
            raise FileDownloadError("merged_corpus") from e

        self._logger.info("Merged corpus downloaded to '%s'", dir_save_to)

    def upload_crash(self, input_id: str, input_bytes: bytes):

        self._logger.info("Uploading crash")

        bucket, key = self._bucket_data.crash(
            self._fuzzer_id,
            self._fuzzer_rev,
            input_id,
        )

        try:
            self._storage.upload_bytes(input_bytes, bucket, key)

        except ObjectStorageError as e:
            self._logger.error("Failed to upload crash: '%s'", e)
            raise FileUploadError("crash") from e

    def upload_unmerged_corpus(self, run_id: str, source_dir: str):

        self._logger.info("Uploading unmerged corpus from '%s'", source_dir)

        bucket, key = self._bucket_data.unmerged_corpus(
            self._fuzzer_id,
            self._fuzzer_rev,
            run_id,
        )

        try:
            self._storage.upload_archive(source_dir, bucket, key)
        except ObjectStorageError as e:
            self._logger.error("Failed to upload unmerged corpus: '%s'", e)
            raise FileUploadError("unmerged_corpus") from e

    def upload_merged_corpus(self, source_dir: str):

        self._logger.info("Uploading merged corpus from '%s'", source_dir)

        bucket, key = self._bucket_data.merged_corpus(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._storage.upload_archive(source_dir, bucket, key)
        except ObjectStorageError as e:
            self._logger.error("Failed to upload merged corpus: '%s'", e)
            raise FileUploadError("merged_corpus") from e

    def download_unmerged_corpus(self, dir_save_to: str):

        bucket, prefix = self._bucket_data.unmerged_corpus_list(
            self._fuzzer_id,
            self._fuzzer_rev,
        )

        try:
            self._logger.info("Downloading unmerged corpus archives")
            keys = self._storage.download_many_archives(bucket, prefix, dir_save_to)

        except ObjectStorageError as e:
            self._logger.error("Failed to download unmerged corpus: '%s'", e)
            raise FileDownloadError("unmerged_corpus") from e

        self._logger.info("Unmerged corpus downloaded to '%s'", dir_save_to)
        return keys

    def delete_unmerged_corpus(self, keys: List[str]):

        self._logger.info("Deleting unmerged corpus files")

        def delete_object(key):
            try:
                bucket = self._bucket_data.name
                self._storage.delete_object(bucket, key)
            except ObjectNotFoundError:
                msg = "Object '%s' does not exist. Deleted externally?"
                self._logger.warning(msg, key)

        try:
            for key in keys:
                delete_object(key)

        except ObjectStorageError as e:
            self._logger.error("Failed to delete unmerged corpus files: '%s'", e)
            raise RemoteFileDeleteError("unmerged_corpus") from e

    @property
    def storage(self):
        return self._storage
