from typing import Optional
from pydantic import BaseSettings, Field, AnyUrl, validator
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


# TODO: rename to AgentMode
class FuzzerMode(str, Enum):
    firstrun = "firstrun"
    fuzzing = "fuzzing"
    merge = "merge"


class EngineID(str, Enum):
    # afl binding
    afl = "afl"
    afl_rs = "afl.rs"
    sharpfuzz_afl = "sharpfuzz-afl"

    # libfuzzer binding
    libfuzzer = "libfuzzer"
    jazzer = "jazzer"
    atheris = "atheris"
    cargo_fuzz = "cargo-fuzz"
    go_fuzz_libfuzzer = "go-fuzz-libfuzzer"
    sharpfuzz_libfuzzer = "sharpfuzz-libfuzzer"


class LangID(str, Enum):
    go = "go" # go-fuzz-libfuzzer
    cpp = "cpp" # afl, libfuzzer
    rust = "rust" # afl.rs, cargo-fuzz
    java = "java" # jqf, jazzer
    swift = "swift" # libfuzzer
    python = "python" # atheris
    # javascript = "javascript" # libfuzzer


class Buckets(BaseSettings):
    fuzzers: str
    data: str

    class Config:
        env_prefix = "S3_BUCKET_"


class ObjectStorage(BaseSettings):
    url: AnyUrl
    access_key: str
    secret_key: str
    buckets: Buckets

    class Config:
        env_prefix = "S3_"


class FuzzerSettings(BaseSettings):
    session_id: str
    user_id: str
    project_id: str
    pool_id: str
    id: str
    rev: str
    lang: LangID
    engine: EngineID
    ram_limit: int = Field(gt=0)
    time_limit: int = Field(gt=0)
    time_limit_firstrun: int = Field(gt=0)
    num_iterations: int = Field(gt=0)
    num_iterations_firstrun: int = Field(gt=0)
    crash_max_size: int = Field(gt=0)

    class Config:
        env_prefix = "FUZZER_"


class AgentSettings(BaseSettings):
    mode: FuzzerMode
    default_target: str

    class Config:
        env_prefix = "AGENT_"


class MessageQueues(BaseSettings):
    scheduler: str
    crash_analyzer: str

    class Config:
        env_prefix = "MQ_QUEUE_"


class MessageQueueSettings(BaseSettings):
    username: str
    password: str
    region: str
    url: Optional[AnyUrl]
    queues: MessageQueues
    broker: str = Field(regex=r"^sqs$")

    class Config:
        env_prefix = "MQ_"


class KubernetesSettings(BaseSettings):
    namespace: str
    pod_name: str
    user_container: str

    class Config:
        env_prefix = "KUBERNETES_"


class RunnerSettings(BaseSettings):
    poll_interval_ms: int
    grace_period_sec: int

    class Config:
        env_prefix = "RUNNER_"


class PathsSettings(BaseSettings):
    runner_binary: str
    #monitor_binary: str
    #monitor_config: str

    metrics: str

    volume_disk: str
    volume_tmpfs: str

    agent_binaries: str
    user_home_link: str
    user_tmpfs_source: str
    user_tmpfs_link: str

    class Config:
        env_prefix = "PATH_"

    @validator("volume_disk", "volume_tmpfs")
    def path_exists(value):

        if not os.path.exists(value):
            raise ValueError("Directory does not exist")

        if not os.path.isdir(value):
            raise ValueError("Provided path is not a directory")

        return value


class AppSettings(BaseSettings):
    environment: str = Field(env="ENVIRONMENT", regex=r"^(dev|prod|test)$")
    message_queue: MessageQueueSettings
    kubernetes: KubernetesSettings
    object_storage: ObjectStorage
    runner: RunnerSettings
    paths: PathsSettings
    agent: AgentSettings
    fuzzer: FuzzerSettings


@lru_cache()
def load_app_settings():
    return AppSettings(
        message_queue=MessageQueueSettings(
            queues=MessageQueues(),
        ),
        kubernetes=KubernetesSettings(),
        object_storage=ObjectStorage(
            buckets=Buckets(),
        ),
        runner=RunnerSettings(),
        paths=PathsSettings(),
        agent=AgentSettings(),
        fuzzer=FuzzerSettings(),
    )
