from typing import Optional
from pydantic import BaseModel, Field, AnyUrl, validator
from functools import lru_cache
from contextlib import suppress
from enum import Enum
import os


# fmt: off
with suppress(ModuleNotFoundError):
    import dotenv; dotenv.load_dotenv() # type: ignore
# fmt: on


def get_config_variable(name):

    var = os.environ.get(name)
    if not var:
        raise Exception(f"Variable '{name}' is not set")

    os.unsetenv(name)
    return var


class FuzzerMode(str, Enum):
    firstrun = "firstrun"
    fuzzing = "fuzzing"
    merge = "merge"

class FuzzerLang(str, Enum):
    go = "Go"
    cpp = "Cpp"
    rust = "Rust"
    # java = "Java"
    python = "Python"
    # javascript = "JavaScript"


class Buckets(BaseModel):
    fuzzers: str
    data: str


class ObjectStorage(BaseModel):
    url: AnyUrl
    access_key: str
    secret_key: str
    buckets: Buckets


class Volume(BaseModel):
    path: str = Field(min_length=1)
    limit: int = Field(gt=0)

    @validator("path")
    def path_valid(value):

        if not os.path.exists(value):
            raise ValueError("Directory does not exist")

        if not os.path.isdir(value):
            raise ValueError("Provided path is not a directory")

        return value


class Volumes(BaseModel):
    tmpfs: Volume
    disk: Volume


class Fuzzer(BaseModel):
    id: str
    rev: str
    lang: FuzzerLang
    engine: str
    pool_id: str
    instance_id: str
    ram_limit: int = Field(gt=0)
    time_limit: int = Field(gt=0)
    num_iters: int = Field(gt=0)
    time_limit_fr: int = Field(gt=0)
    num_iters_fr: int = Field(gt=0)
    max_crash_size: int = Field(gt=0)


class Agent(BaseModel):
    mode: FuzzerMode
    drop_permissions: bool
    default_target: str
    fuzzer: Fuzzer


class MessageQueues(BaseModel):
    scheduler: str
    crash_analyzer: str


class MessageQueueSettings(BaseModel):
    username: str
    password: str
    region: str
    url: Optional[AnyUrl]
    queues: MessageQueues
    broker: str = Field(regex=r"^sqs$")


class AppSettings(BaseModel):
    environment: str = Field(regex=r"dev|prod|test")
    message_queue: MessageQueueSettings
    object_storage: ObjectStorage
    volumes: Volumes
    agent: Agent


@lru_cache()
def load_app_settings():
    return AppSettings(
        environment=get_config_variable("ENVIRONMENT"),
        message_queue=MessageQueueSettings(
            username=get_config_variable("MQ_USERNAME"),
            password=get_config_variable("MQ_PASSWORD"),
            region=get_config_variable("MQ_REGION"),
            url=get_config_variable("MQ_URL"),
            broker=get_config_variable("MQ_BROKER"),
            queues=MessageQueues(
                scheduler=get_config_variable("MQ_QUEUE_SCHEDULER"),
                crash_analyzer=get_config_variable("MQ_QUEUE_CRASH_ANALYZER"),
            )
        ),
        object_storage=ObjectStorage(
            url=get_config_variable("S3_URL"),
            access_key=get_config_variable("S3_ACCESS_KEY"),
            secret_key=get_config_variable("S3_SECRET_KEY"),
            buckets=Buckets(
                fuzzers=get_config_variable("S3_BUCKET_FUZZERS"),
                data=get_config_variable("S3_BUCKET_DATA"),
            ),
        ),
        volumes=Volumes(
            tmpfs=Volume(
                path=get_config_variable("TMPFS_VOLUME_PATH"),
                limit=get_config_variable("TMPFS_VOLUME_LIMIT"),
            ),
            disk=Volume(
                path=get_config_variable("DISK_VOLUME_PATH"),
                limit=get_config_variable("DISK_VOLUME_LIMIT"),
            ),
        ),
        agent=Agent(
            fuzzer=Fuzzer(
                id=get_config_variable("FUZZER_ID"),
                rev=get_config_variable("FUZZER_REVISION"),
                pool_id=get_config_variable("FUZZER_POOL_ID"),
                instance_id=get_config_variable("FUZZER_INSTANCE_ID"),
                lang=FuzzerLang(get_config_variable("FUZZER_LANG")),
                engine=get_config_variable("FUZZER_ENGINE"),
                ram_limit=get_config_variable("FUZZER_RAM_LIMIT"),
                time_limit=get_config_variable("FUZZER_RUN_TIME_LIMIT"),
                num_iters=get_config_variable("FUZZER_NUM_ITERATIONS"),
                num_iters_fr=get_config_variable("FUZZER_NUM_ITERATIONS_FIRSTRUN"),
                time_limit_fr=get_config_variable("FUZZER_RUN_TIME_LIMIT_FIRSTRUN"),
                max_crash_size=get_config_variable("FUZZER_CRASH_MAX_SIZE"),
            ),
            mode=FuzzerMode(get_config_variable("AGENT_MODE")),
            default_target=get_config_variable("AGENT_DEFAULT_TARGET"),
            drop_permissions=get_config_variable("AGENT_DROP_PERMISSIONS"),
        ),
    )
