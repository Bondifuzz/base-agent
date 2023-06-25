from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional
import functools
import random
import string
import os
from stat import S_IEXEC

from .settings import load_app_settings


class TimeMeasure:

    start_time: Optional[datetime]
    finish_time: Optional[datetime]
    elapsed: Optional[timedelta]

    def __init__(self):
        self.start_time = None
        self.finish_time = None
        self.elapsed = None

    @contextmanager
    def measuring(self):

        try:
            self.start_time = datetime.utcnow()
            yield

        finally:
            self.finish_time = datetime.utcnow()
            self.elapsed = self.finish_time - self.start_time


def rfc3339(date: datetime) -> str:
    return date.replace(microsecond=0).isoformat() + "Z"


def rfc3339_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def random_string(n: int):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def testing_only(func):

    """
    Provides decorator, which forbids
    calling dangerous functions in production
    """

    settings = load_app_settings()
    is_danger = settings.environment == "prod"

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        if is_danger:
            err = f"Function '{func.__name__}' is forbidden to call in production"
            help = "Please, check 'ENVIRONMENT' variable is not set to 'prod'"
            raise RuntimeError(f"{err}. {help}")

        return func(*args, **kwargs)

    return wrapper


def chmod_recursive(file_perms: int, dir_perms: int, path: str = "."):

    # Use helper function to apply chmod recursively
    def _chmod_recursive(path: str):
        for item in os.scandir(path):
            if item.is_file():
                os.chmod(item.path, file_perms)
            elif item.is_dir():
                os.chmod(item.path, dir_perms)
                _chmod_recursive(item.path)

    return _chmod_recursive(path)


def make_executable(binary: str):
    st = os.stat(binary)
    os.chmod(binary, st.st_mode | S_IEXEC)


def fs_consumed(fs_path: str) -> int:
    if not os.path.exists(fs_path):
        return 0

    stat = os.statvfs(fs_path)
    return (stat.f_blocks - stat.f_bavail) * stat.f_bsize
