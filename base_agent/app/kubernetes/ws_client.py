from __future__ import annotations
from typing import TYPE_CHECKING

import asyncio
from contextlib import suppress
import yaml
import time
import aiohttp
import async_timeout
from kubernetes_asyncio.client.exceptions import ApiException

if TYPE_CHECKING:
    from typing import Dict, Union, Optional


STDIN_CHANNEL = 0
STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2
ERROR_CHANNEL = 3
RESIZE_CHANNEL = 4


class ExecWSClient:
    def __init__(
        self,
        ws_response: aiohttp.ClientWebSocketResponse,
        stdout: bool = True,
        stderr: bool = True,
        combine_output: bool = False
    ):
        self._connected = False
        self._channels: Dict[int, str] = dict()

        if not stdout or not stderr and combine_output:
            # TODO: warning, combine output with not all channels enabled
            stdout = True
            stderr = True

        self.stdout = stdout
        self.stderr = stderr
        self.combine_output = combine_output

        self.sock = ws_response
        self._connected = True
        self._returncode = None

    def peek_channel(self, channel: int, default: Optional[str] = None):
        return self._channels.get(channel, default)

    def read_channel(self, channel: int, default: Optional[str] = None):
        """Read data from a channel."""
        return self._channels.pop(channel, default)

    async def write_channel(self, channel: int, data: Union[str, bytes]):
        """Write data to a channel."""
        if isinstance(data, bytes):
            await self.sock.send_bytes(bytes(chr(channel), "ascii") + data)
        else:
            await self.sock.send_str(chr(channel) + data)

    def peek_stdout(self, default: Optional[str] = None):
        """Same as peek_channel with channel=1."""
        return self.peek_channel(STDERR_CHANNEL, default=default)

    def read_stdout(self, default: Optional[str] = None):
        """Same as read_channel with channel=1."""
        return self.read_channel(STDOUT_CHANNEL, default=default)

    def peek_stderr(self, default: Optional[str] = None):
        """Same as peek_channel with channel=2."""
        return self.peek_channel(STDERR_CHANNEL, default=default)

    def read_stderr(self, default: Optional[str] = None):
        """Same as read_channel with channel=2."""
        return self.read_channel(STDERR_CHANNEL, default=default)

    def is_open(self):
        """True if the connection is still alive."""
        return self._connected

    async def write_stdin(self, data: Union[str, bytes]):
        """The same as write_channel with channel=0."""
        await self.write_channel(STDIN_CHANNEL, data)

    async def update(self, timeout: Optional[float] = None):
        """Update channel buffers with at most one complete frame of input."""
        if not self.is_open():
            return
        if self.sock.closed:
            self._connected = False
            return
        if timeout <= 0:
            return

        try:
            async with async_timeout.timeout(timeout):
                msg = await self.sock.receive()
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    self._connected = False
                    return
                elif msg.type == aiohttp.WSMsgType.BINARY or msg.type == aiohttp.WSMsgType.TEXT:
                    data: Union[bytes, str] = msg.data
                    if isinstance(data, bytes):
                        data = data.decode("utf-8", "replace")
                    if len(data) > 2:
                        channel = ord(data[0])
                        data = data[1:]

                        if (
                            channel not in [STDOUT_CHANNEL, STDERR_CHANNEL] or
                            channel == STDOUT_CHANNEL and self.stdout or
                            channel == STDERR_CHANNEL and self.stderr
                        ):
                            if channel == STDERR_CHANNEL and self.combine_output:
                                channel = STDOUT_CHANNEL # combine output to stdout

                            if channel not in self._channels:
                                self._channels[channel] = data
                            else:
                                self._channels[channel] += data
        
        except asyncio.TimeoutError:
            pass # TODO: with suppress?

    async def run_forever(self, timeout: Optional[float] = None):
        """Wait till connection is closed or timeout reached. Buffer any input
        received during this time."""
        if timeout:
            start = time.time()
            while self.is_open() and time.time() - start < timeout:
                await self.update(timeout=(timeout - time.time() + start))
        else:
            while self.is_open():
                await self.update(timeout=None)

    @property
    def returncode(self):
        """
        The return code, A None value indicates that the process hasn't
        terminated yet.
        """
        if self.is_open():
            return None
        else:
            if self._returncode is None:
                err = self.peek_channel(ERROR_CHANNEL)
                if err is None:
                    raise ApiException(500, "Broken connection")
                err = yaml.safe_load(err)
                if err['status'] == "Success":
                    self._returncode = 0
                else:
                    self._returncode = int(err['details']['causes'][0]['message'])
            return self._returncode

    async def close(self, code: int = aiohttp.WSCloseCode.OK, message: bytes = b""):
        """
        close websocket connection.
        """
        self._connected = False
        if self.sock:
            with suppress(Exception):
                await self.sock.close(code=code, message=message)
        self.sock = None
