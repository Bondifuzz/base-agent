"""
## Kubernetes API module
"""

from __future__ import annotations

import os
import json
import logging
import aiohttp
from typing import TYPE_CHECKING, Optional

from kubernetes_asyncio import config
from kubernetes_asyncio.config.config_exception import ConfigException

from kubernetes_asyncio.client import ApiClient
from kubernetes_asyncio.stream import WsApiClient
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.client.api.core_v1_api import CoreV1Api as BaseCoreV1Api
from ..errors import FuzzerAbortedError, FuzzerLaunchError, InternalError, RamLimitExceeded, TimeLimitExceeded, TmpfsLimitExceeded

from ..settings import AppSettings
from .ws_client import ExecWSClient
from ..paths import BasePaths

if TYPE_CHECKING:

    # fmt: off
    # isort: off
    from kubernetes_asyncio.client import V1Pod, V1PodStatus, V1ContainerStatus, V1ContainerStateTerminated, V1ContainerState
    from aiohttp import ClientResponse
    from typing import List, Dict
    # isort: on
    # fmt: on


########################################
# Kubernetes API wrapper
########################################

class CoreV1Api(BaseCoreV1Api):
    async def connect_get_namespaced_pod_exec(self, name, namespace, **kwargs) -> ExecWSClient:
        assert isinstance(self.api_client, WsApiClient), "exec must be called on WsApiClient"

        stdout: bool = kwargs.get("stdout", True)
        stderr: bool = kwargs.get("stderr", True)
        # not standard argument - removing from kwargs
        combine_output: bool = kwargs.pop("combine_output", False)
        
        kwargs["_preload_content"] = False
        response = await super().connect_get_namespaced_pod_exec(name, namespace, **kwargs)
        assert isinstance(response, aiohttp.ClientWebSocketResponse), f"{type(response)} - {response}"

        ws_client = ExecWSClient(
            ws_response=response,
            stdout=stdout,
            stderr=stderr,
            combine_output=combine_output,
        )
        return ws_client

    async def connect_post_namespaced_pod_exec(self, name, namespace, **kwargs) -> ExecWSClient:
        assert isinstance(self.api_client, WsApiClient), "exec must be called on WsApiClient"

        stdout: bool = kwargs.get("stdout", True)
        stderr: bool = kwargs.get("stderr", True)
        # not standard argument - removing from kwargs
        combine_output: bool = kwargs.pop("combine_output", False)
        
        kwargs["_preload_content"] = False
        response = await super().connect_get_namespaced_pod_exec(name, namespace, **kwargs)
        assert isinstance(response, aiohttp.ClientWebSocketResponse), f"{type(response)} - {response}"

        ws_client = ExecWSClient(
            ws_response=response,
            stdout=stdout,
            stderr=stderr,
            combine_output=combine_output,
        )
        return ws_client


class UserContainerManager:

    _logger: logging.Logger
    _namespace: str
    _pod_name: str
    _user_container: str

    def __init__(self, settings: AppSettings):
        self._logger = logging.getLogger("user_container")
        self._settings = settings
        self._namespace = settings.kubernetes.namespace
        self._pod_name = settings.kubernetes.pod_name
        self._user_container = settings.kubernetes.user_container
        self._paths = BasePaths(settings)

    async def create(settings: AppSettings):
        self = UserContainerManager(settings)

        try:
            if "KUBERNETES_PORT" in os.environ:
                config.load_incluster_config()
            else:
                await config.load_kube_config()

        except ConfigException as e:
            self._logger.exception("Failed to load kube config")
            raise InternalError()

        return self

    async def exec_command(
        self,
        cmd: List[str],
        cwd: str,
        env: Dict[str, str] = dict(),
        stdin_file: Optional[str] = None,
        stdout_file: Optional[str] = None,
        stderr_file: Optional[str] = None,
        time_limit: Optional[int] = None,
    ) -> int:

        runner_cfg = dict(
            command=cmd,
            env=[dict(name=k, value=v) for k, v in env.items()],
            cwd=cwd,
            streams=dict(
                stdin=stdin_file,
                stdout=stdout_file,
                stderr=stderr_file,
            ),
            poll_interval_ms=self._settings.runner.poll_interval_ms,
            grace_period_sec=self._settings.runner.grace_period_sec,
            run_timeout_sec=time_limit,
        )

        with open(self._paths.runner_config, "w") as f:
            f.write(json.dumps(runner_cfg))

        async with WsApiClient() as ws_client:
            v1_ws_api = CoreV1Api(ws_client)

            resp = await v1_ws_api.connect_get_namespaced_pod_exec(
                name=self._pod_name,
                namespace=self._namespace,
                container=self._user_container,
                command=[self._paths.runner_binary, self._paths.runner_config],
                tty=False, stdin=False,
                stdout=True, stderr=True,
                combine_output=False,
            )

            runner_output = ""
            runner_logs = ""

            # timeout?
            while resp.is_open():
                await resp.update(timeout=5)
                runner_logs += resp.read_stderr(default="")
                runner_output += resp.read_stdout(default="")
            
            # None only when resp.is_open() == True
            runner_exitcode = resp.returncode
            assert runner_exitcode is not None

            if runner_exitcode == 101:
                raise TimeLimitExceeded()

            # terminated, SIGKILL, SIGTERM
            elif runner_exitcode in [102, 128 + 9, 128 + 15]:
                cont_info = await self.read_fuzzer_container()
                cont_state: V1ContainerState = cont_info.state
                
                if cont_state.terminated is None:
                    state_str = "Waiting" if cont_state.running is None else "Running" 
                    self._logger.error(f"Fuzzer container in unexpected state: {state_str}")
                    raise InternalError()
                
                cont_state_term: V1ContainerStateTerminated = cont_state.terminated
                monitor_exitcode: int = cont_state_term.exit_code
                if monitor_exitcode == 101:
                    self._logger.info("No space left on container tmpfs")
                    raise TmpfsLimitExceeded()
                elif monitor_exitcode == 102:
                    self._logger.info("Container terminated")
                    raise FuzzerAbortedError()
                else:
                    reason: str = cont_state_term.reason
                    if reason.strip().lower() == "oomkilled":
                        self._logger.info("Container OOMKilled")
                        raise RamLimitExceeded()
                    else:
                        monitor_logs = await self.read_monitor_output()
                        self._logger.error(
                            f"Container killed with exitcode={monitor_exitcode}, reason={reason}\n" +
                            f"runner_logs:\n{runner_logs}\n" +
                            f"monitor_logs:\n{monitor_logs}"
                        )
                        raise InternalError()

            elif runner_exitcode != 0:
                self._logger.error(
                    f"Runner exited with exitcode={runner_exitcode}\n" +
                    f"logs:\n{runner_logs}"
                )
                raise InternalError()

            try:
                process_exitcode = int(runner_output.strip())

            except:
                self._logger.error(
                    f"Wrong output from runner\n" +
                    f"output:\n{runner_output}\n" +
                    f"logs:\n{runner_logs}"
                )
                raise InternalError()

            return process_exitcode

    async def read_fuzzer_container(self) -> V1ContainerStatus:
        async with ApiClient() as client:
            v1_api = CoreV1Api(client)

            pod: V1Pod = await v1_api.read_namespaced_pod(
                namespace=self._namespace,
                name=self._pod_name,
            )
            pod_status: V1PodStatus = pod.status
            cont_statuses: List[V1ContainerStatus] = pod_status.container_statuses
            for cont_status in cont_statuses:
                if cont_status.name != self._user_container:
                    return cont_status

            # TODO: exception type? (should not happen)
            raise ApiException(status=404, reason="Fuzzer container not found") 

    async def read_monitor_output(self) -> str:
        async with ApiClient() as client:
            v1_api = CoreV1Api(client)

            resp: ClientResponse = await v1_api.read_namespaced_pod_log(
                namespace=self._namespace,
                name=self._pod_name,
                container=self._user_container,
                _preload_content=False,
            )

            if resp.status != 200:
                raise ApiException(resp.status, resp.reason)

            logs = await resp.read()
            return logs.decode("utf-8", "replace")
