from __future__ import annotations
from typing import TYPE_CHECKING

import os

if TYPE_CHECKING:
    from .settings import AppSettings


class BasePaths:
    _settings: AppSettings

    def __init__(self, settings: AppSettings) -> None:
        self._settings = settings

        os.makedirs(settings.paths.user_tmpfs_source, exist_ok=True)

        if not os.path.lexists(settings.paths.user_home_link):
            os.symlink(
                src=settings.paths.agent_binaries,
                dst=settings.paths.user_home_link,
            )
        if not os.path.lexists(settings.paths.user_tmpfs_link):
            os.symlink(
                src=settings.paths.user_tmpfs_source,
                dst=settings.paths.user_tmpfs_link,
            )

    @property
    def disk_volume(self):
        return self._settings.paths.volume_disk

    @property
    def tmpfs_volume(self):
        return self._settings.paths.volume_tmpfs

    @property
    def runner_binary(self):
        return self._settings.paths.runner_binary

    @property
    def runner_config(self):
        return os.path.join(self.disk_volume, "runner.json")

    @property
    def user_home(self):
        return self._settings.paths.user_home_link

    @property
    def user_tmpfs(self):
        return self._settings.paths.user_tmpfs_link

    @property
    def binaries(self):
        #return os.path.join(self.disk_volume, "binaries")
        return self._settings.paths.agent_binaries

    @property
    def config(self):
        return os.path.join(self.disk_volume, "config.json")

    @property
    def fuzzer_log(self):
        return os.path.join(self.tmpfs_volume, "fuzzer.log")

    @property
    def merge_log(self):
        return os.path.join(self.tmpfs_volume, "merge.log")

    @property
    def repro_log(self):
        return os.path.join(self.tmpfs_volume, "repro.log")

    @property
    def clean_log(self):
        return os.path.join(self.tmpfs_volume, "clean.log")
    