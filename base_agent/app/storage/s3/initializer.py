from typing import TYPE_CHECKING, List
from dataclasses import dataclass
import logging
import io

import boto3
from botocore.exceptions import (
    EndpointConnectionError,
    ClientError,
)

from .errors import ObjectStorageInitError
from ...settings import AppSettings

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
else:
    S3ServiceResource = object

########################################
# Object Storage Base Initializer
########################################


@dataclass
class BucketCheck:
    bucket_name: str
    check_read: bool
    check_write: bool


class ObjectStorageInitializer:

    _s3: S3ServiceResource
    _checks: List[BucketCheck]
    _logger: logging.Logger

    def _verify_auth(self):
        try:
            self._s3.meta.client.list_buckets()
        except EndpointConnectionError as e:
            raise ObjectStorageInitError(str(e))
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "InvalidAccessKeyId":
                raise ObjectStorageInitError(f"Invalid access key")
            elif error_code == "SignatureDoesNotMatch":
                raise ObjectStorageInitError(f"Invalid secret key")
            else:
                raise ObjectStorageInitError(str(e))

    def _check_bucket_exists(self, bucket_name):
        try:
            self._s3.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if error_code == 404:
                raise ObjectStorageInitError(f'Bucket "{bucket_name}" does not exist')
            elif error_code == 403:
                raise ObjectStorageInitError(
                    f'Not enough rights to read bucket "{bucket_name}"'
                )
            else:
                raise ObjectStorageInitError(str(e))

    def _check_for_read_permissions(self, bucket_name):

        """
        Check bucket read access in two steps:
        1) Try to list objects in bucket.
        2) Try to download file. No access -> will get 403 first
        """

        try:
            self._s3.meta.client.list_objects(Bucket=bucket_name, MaxKeys=1)

            try:
                f = io.BytesIO()
                self._s3.meta.client.download_fileobj(bucket_name, "test_read", f)

            except ClientError as e:
                if int(e.response["Error"]["Code"]) != 404:
                    raise e

            finally:
                f.close()

        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 403:
                raise ObjectStorageInitError(
                    f'Not enough rights to read contents of bucket "{bucket_name}"'
                )
            else:
                raise ObjectStorageInitError(
                    f'Failed to read contents of bucket "{bucket_name}". {str(e)}'
                )

    def _check_for_write_permissions(self, bucket_name):

        bucket = self._s3.Bucket(bucket_name)
        obj = bucket.Object("mykey")

        try:
            obj.upload_fileobj(io.BytesIO(b"write-test"))
            obj.delete()

        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 403:
                raise ObjectStorageInitError(
                    f'Not enough rights to write to bucket "{bucket_name}"'
                )
            else:
                raise ObjectStorageInitError(
                    f'Failed to write to bucket "{bucket_name}". {str(e)}'
                )

    def _check_bucket(self, name, check_read, check_write):

        msg = "Required permissions: read=%s, write=%s"
        self._logger.info("Checking bucket '%s'", name)
        self._logger.info(msg, check_read, check_write)

        self._check_bucket_exists(name)
        self._logger.info("Bucket '%s' exists. Checking permissions...", name)

        if check_read:
            self._check_for_read_permissions(name)

        if check_write:
            self._check_for_write_permissions(name)

    def do_init(self):

        self._verify_auth()
        for chk in self._checks:
            self._check_bucket(
                chk.bucket_name,
                chk.check_read,
                chk.check_write,
            )

    def __init__(self, settings: AppSettings, checks: List[BucketCheck] = []):

        self._url = settings.object_storage.url
        self._access_key = settings.object_storage.access_key
        self._secret_key = settings.object_storage.secret_key
        self._checks = checks

        self._logger = logging.getLogger("s3.init")
        self._logger.info(f'Using access key "{self._access_key}"')

        self._s3 = boto3.resource(
            service_name="s3",
            endpoint_url=self._url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    @property
    def s3(self):
        return self._s3
