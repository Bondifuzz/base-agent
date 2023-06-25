# aws s3 --endpoint-url=$EP sync s3://bondifuzz-data-dev s3://bondifuzz-data-tmp --exclude "*.log.gz"

from botocore.exceptions import ClientError
import boto3
from typing import TYPE_CHECKING, DefaultDict, List, Set

from settings import AppSettings, load_app_settings

from collections import defaultdict

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3ServiceResource
else:
    S3ServiceResource = object


class S3Helper:

    _s3: S3ServiceResource
    _bucket_data_name: str
    _bucket_fuzzers_name: str

    def __init__(self, settings: AppSettings) -> None:

        self._s3 = boto3.resource(
            service_name="s3",
            endpoint_url=settings.object_storage.url,
            aws_access_key_id=settings.object_storage.access_key,
            aws_secret_access_key=settings.object_storage.secret_key,
        )

        self._bucket_data_name = settings.object_storage.buckets.data
        self._bucket_fuzzers_name = settings.object_storage.buckets.fuzzers

    def all_fuzzers_with_revisions(self) -> DefaultDict[str, Set[str]]:

        fuzzers = defaultdict(set)
        bucket_data = self._s3.Bucket(self._bucket_data_name)
        bucket_fuzzers = self._s3.Bucket(self._bucket_fuzzers_name)

        def level1_value(key: str):
            return key.split("/", 1)[0]

        def level2_value(key: str):
            return key.split("/", 2)[1]

        def is_revision(key: str):
            return key not in ["corpus_tmp", "corpus.tar.gz"]

        fuzzers_with_corpus = []
        for obj in bucket_data.objects.all():

            fuzzer_id = level1_value(obj.key)
            value = level2_value(obj.key)

            if value == "corpus.tar.gz" and fuzzer_id not in fuzzers_with_corpus:
                fuzzers_with_corpus.append(fuzzer_id)

        for obj in bucket_fuzzers.objects.all():

            fuzzer_id = level1_value(obj.key)
            value = level2_value(obj.key)

            if fuzzer_id in fuzzers_with_corpus and is_revision(value):
                fuzzers[fuzzer_id].add(value)

        return fuzzers

    def _copy_object(self, src_key: str, dst_key: str):

        try:
            copy_source = {"Bucket": self._bucket_data_name, "Key": src_key}
            self._s3.meta.client.copy(copy_source, self._bucket_data_name, dst_key)

        except ClientError as e:
            print(str(e))

    def _delete_object(self, key: str):

        try:
            kw = {"Bucket": self._bucket_data_name, "Key": key}
            self._s3.meta.client.delete_object(**kw)

        except ClientError as e:
            print(str(e))

    def _migrate_merged_corpus(self, fuzzer_id: str, fuzzer_rev: str):
        src_key = f"{fuzzer_id}/corpus.tar.gz"
        dst_key = f"{fuzzer_id}/{fuzzer_rev}/corpus/corpus.tar.gz"
        self._copy_object(src_key, dst_key)

    def _migrate_unmerged_corpus(self, fuzzer_id: str, fuzzer_rev: str):

        bucket_data = self._s3.Bucket(self._bucket_data_name)

        kw = {"Prefix": f"{fuzzer_id}/corpus_tmp"}
        for obj in bucket_data.objects.filter(**kw):
            dst_key = obj.key.replace("corpus_tmp", f"{fuzzer_rev}/corpus/tmp")
            self._copy_object(obj.key, dst_key)

    def migrate_files(self, fuzzer_id: str, fuzzer_rev: str):
        self._migrate_merged_corpus(fuzzer_id, fuzzer_rev)
        self._migrate_unmerged_corpus(fuzzer_id, fuzzer_rev)

    def _delete_old_merged_corpus(self, fuzzer_id: str):
        self._delete_object(f"{fuzzer_id}/corpus.tar.gz")

    def _delete_old_unmerged_corpus(self, fuzzer_id: str):

        bucket_data = self._s3.Bucket(self._bucket_data_name)

        kw = {"Prefix": f"{fuzzer_id}/corpus_tmp"}
        for obj in bucket_data.objects.filter(**kw):
            self._delete_object(obj.key)

    def delete_old_files(self, fuzzer_id: str):
        self._delete_old_merged_corpus(fuzzer_id)
        self._delete_old_unmerged_corpus(fuzzer_id)


def main():

    settings = load_app_settings()
    s3_helper = S3Helper(settings)
    fuzzers = s3_helper.all_fuzzers_with_revisions()

    for fuzzer, revisions in fuzzers.items():
        for revision in revisions:
            print(f"Migrating <fuzzer-id={fuzzer}, revision={revision}>")
            s3_helper.migrate_files(fuzzer, revision)

    for fuzzer in fuzzers.keys():
        print(f"Deleting old files <fuzzer-id={fuzzer}>")
        s3_helper.delete_old_files(fuzzer)


if __name__ == "__main__":
    main()
