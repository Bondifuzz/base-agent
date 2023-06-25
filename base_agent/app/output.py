from typing import Optional
from pydantic import BaseModel, validator, root_validator


class CrashBase(BaseModel):
    type: str
    """ Type of crash: crash, oom, timeout, leak, etc.. """

    input_id: Optional[str]
    """ Id (key) of uploaded to object storage input which caused program to abort """

    input: Optional[str]
    """ Crash input (base64-encoded). Used if crash file is not too large """

    output: str
    """ Crash output (long multiline text) """

    reproduced: bool
    """ True if crash was reproduced, else otherwise """


class Status(BaseModel):

    code: str
    """ Agent exit code """

    message: str
    """ Agent exit status in human-readable format """

    details: Optional[str]
    """ Error details. Usually, it's long and verbose message """


class Metrics(BaseModel):

    tmpfs: int
    """ Amount of disk space consumed during fuzzing session """

    memory: int
    """ Amount of ram consumed during fuzzing session """


class Statistics(BaseModel):

    work_time: int # TODO: or float?
    """ Fuzzer work time """


class AgentOutput(BaseModel):
    status: Status
    metrics: Metrics
    crashes_found: int
    statistics: Optional[Statistics]
