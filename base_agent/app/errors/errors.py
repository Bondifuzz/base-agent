from typing import Optional

from .codes import *


class AgentError(Exception):
    def __init__(self, code: str, message: str, details: Optional[str] = None):
        super().__init__(code, message, details)

    @property
    def code(self) -> str:
        return self.args[0]

    @property
    def message(self) -> str:
        return self.args[1]

    @property
    def details(self) -> Optional[str]:
        return self.args[2]

    def __str__(self) -> str:
        return f"[{self.code}]: {self.message}"


class InternalError(AgentError):
    def __init__(self, details: Optional[str] = None):
        super().__init__(
            code=E_INTERNAL_ERROR,
            message=f"Internal error",
            details=details,
        )


class InvalidConfigError(AgentError):
    def __init__(self, details: str):
        super().__init__(
            code=E_CONFIG_INVALID,
            message="Fuzzer configuration is invalid",
            details=details,
        )


class FuzzerLaunchError(AgentError):
    def __init__(self, message: str, details: Optional[str] = None):
        super().__init__(
            code=E_FUZZER_LAUNCH_ERROR,
            message=message,
            details=details,
        )


class FuzzerAbortedError(AgentError):
    def __init__(self):
        super().__init__(
            code=E_FUZZER_ABORTED,
            message="Fuzzer aborted",
        )


########################################
# S3/Object storage
########################################


class FileDownloadError(InternalError):
    pass

class RemoteFileDeleteError(InternalError):
    pass

class RemoteFileLookupError(InternalError):
    pass

class FileUploadError(InternalError):
    pass


########################################
# Monitors
########################################


class ResourceLimitExceeded(AgentError):
    pass

class RamLimitExceeded(ResourceLimitExceeded):
    def __init__(self):
        super().__init__(
            code=E_RAM_LIMIT_EXCEEDED,
            message="Ram limit exceeded",
        )

class TmpfsLimitExceeded(ResourceLimitExceeded):
    def __init__(self):
        super().__init__(
            code=E_TMPFS_LIMIT_EXCEEDED,
            message="No space left on tmpfs",
        )

class TimeLimitExceeded(InternalError):
    pass
