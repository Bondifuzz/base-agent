from contextlib import suppress
from typing import TYPE_CHECKING, List
from gzip import GzipFile
from io import BytesIO
import tarfile
import os

from ...settings import AppSettings
from ...utils import testing_only

from .errors import ObjectStorageError, maybe_not_found, maybe_unknown_error
from .initializer import BucketCheck, ObjectStorageInitializer


if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource, S3Client
else:
    S3ServiceResource = object
    S3Client = object


class ObjectStorage:

    _client: S3Client
    _s3: S3ServiceResource

    def __init__(
        self,
        settings: AppSettings,
        checks: List[BucketCheck] = [],
    ):

        initializer = ObjectStorageInitializer(settings, checks)
        initializer.do_init()

        self._s3 = initializer.s3
        self._client = initializer.s3.meta.client

    @maybe_unknown_error
    def upload_bytes(self, source: bytes, bucket: str, key: str):
        self._client.upload_fileobj(BytesIO(source), bucket, key)

    @maybe_unknown_error
    @maybe_not_found
    def download_bytes(self, bucket: str, key: str) -> bytes:
        out = BytesIO()
        self._client.download_fileobj(bucket, key, out)
        return out.getvalue()

    @maybe_unknown_error
    def upload_file(self, source: str, bucket: str, key: str):
        self._client.upload_file(source, bucket, key)

    @maybe_unknown_error
    @maybe_not_found
    def download_file(self, bucket: str, key: str, save_to: str):
        self._client.download_file(Bucket=bucket, Key=key, Filename=save_to)

    @maybe_unknown_error
    def upload_file_gzipped(self, source: str, bucket: str, key: str):

        out = BytesIO()
        with GzipFile(fileobj=out, mode="wb") as gz:
            with open(source, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    gz.write(chunk)

        out.seek(0)
        self._client.upload_fileobj(out, bucket, key)

    @maybe_unknown_error
    @maybe_not_found
    def download_file_gzipped(self, bucket: str, key: str, save_to: str):
        obj = self._client.get_object(Bucket=bucket, Key=key)
        with GzipFile(fileobj=obj["Body"], mode="rb") as gz:
            with open(save_to, "wb") as f:
                for chunk in iter(lambda: gz.read(8192), b""):
                    f.write(chunk)

    @maybe_unknown_error
    def upload_archive(self, source_dir: str, bucket: str, key: str):

        out = BytesIO()
        with tarfile.open(fileobj=out, mode="w:gz") as tar:
            for filename in os.listdir(source_dir):
                filepath = os.path.join(source_dir, filename)
                tar.add(filepath, arcname=filename)

        out.seek(0)
        self._client.upload_fileobj(out, bucket, key)

    @maybe_unknown_error
    @maybe_not_found
    def download_archive(self, bucket: str, key: str, dir_save_to: str):
        obj = self._client.get_object(Bucket=bucket, Key=key)
        with tarfile.open(fileobj=obj["Body"], mode="r:gz") as tar:
            tar.extractall(dir_save_to)

    def download_many_archives(self, bucket: str, prefix: str, dir_save_to: str):

        keys: List[str] = []
        bucket_wrapper = self._s3.Bucket(bucket)
        for obj in bucket_wrapper.objects.filter(Prefix=prefix):
            with suppress(ObjectStorageError):
                self.download_archive(bucket, obj.key, dir_save_to)
                keys.append(obj.key)

        return keys

    def download_many_files(self, bucket: str, prefix: str, dir_save_to: str):

        keys: List[str] = []
        bucket_wrapper = self._s3.Bucket(bucket)
        for obj in bucket_wrapper.objects.filter(Prefix=prefix):
            with suppress(ObjectStorageError):
                save_to = f"{dir_save_to}/{os.path.basename(obj.key)}" # TODO: os.path.join?
                self.download_file(bucket, obj.key, save_to)
                keys.append(obj.key)

        return keys

    @maybe_unknown_error
    @maybe_not_found
    def delete_object(self, bucket: str, key: str):
        self._client.head_object(Bucket=bucket, Key=key)
        self._client.delete_object(Bucket=bucket, Key=key)

    @testing_only
    @maybe_unknown_error
    def clear_bucket(self, bucket_name: str):
        bucket = self._s3.Bucket(bucket_name)
        bucket.objects.all().delete()

