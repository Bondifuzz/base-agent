from __future__ import annotations
from typing import TYPE_CHECKING

from abc import ABCMeta, abstractmethod

if TYPE_CHECKING:
    from .settings import AppSettings
    from .storage.s3 import ObjectStorage
    from .output import Status, CrashBase, Statistics
    from .transfer import FileTransfer
    from .kubernetes import UserContainerManager
    from typing import List, Optional


class AgentMode(metaclass=ABCMeta):
    
    transfer: FileTransfer
    status: Optional[Status]
    fuzz_statistics: Optional[Statistics]
    crashes: List[CrashBase]

    @abstractmethod
    async def run(self) -> None:
        pass
    
    @abstractmethod
    async def finish(self) -> None:
        pass


class Agent(metaclass=ABCMeta):
    @abstractmethod
    def select_mode(
        self,
        settings: AppSettings,
        container_mgr: UserContainerManager,
        object_storage: ObjectStorage,
        run_id: str,
    ) -> AgentMode:
        pass