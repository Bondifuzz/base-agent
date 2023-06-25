from __future__ import annotations
from typing import Optional

from mqtransport import MQApp
from mqtransport.participants import Consumer, Producer
from pydantic import BaseModel


########################################
# Producers
########################################


class MP_NewCrash(Producer):
    name: str = "agent.crash.new"

    class Model(BaseModel):
        user_id: str
        project_id: str
        pool_id: str
        fuzzer_id: str
        fuzzer_rev: str
        fuzzer_engine: str
        fuzzer_lang: str
        crash: dict # CrashBase
        created: str
