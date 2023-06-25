from __future__ import annotations
from typing import Optional

from mqtransport import MQApp
from mqtransport.participants import Consumer, Producer
from pydantic import BaseModel


########################################
# Producers
########################################


class MP_FuzzerRunResult(Producer):

    name = "agent.fuzzer.result"

    class Model(BaseModel):
        user_id: str
        project_id: str
        pool_id: str
        fuzzer_id: str
        fuzzer_rev: str
        fuzzer_engine: str
        fuzzer_lang: str

        session_id: str
        agent_mode: str

        start_time: str
        finish_time: str
        agent_result: Optional[dict]
