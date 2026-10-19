"""Client for rclone's remote-control API on a unix socket.

Every unit's rclone process listens on its own socket with --rc-no-auth;
access control is the socket living in ferryman's 0700 runtime dir, with no
TCP listener at all.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx


class RcError(Exception):
    pass


class RcClient:
    def __init__(self, socket: Path, timeout: float = 30.0) -> None:
        self.socket = socket
        self.timeout = timeout

    async def call(self, endpoint: str, timeout: float | None = None, **params: Any) -> Any:
        transport = httpx.AsyncHTTPTransport(uds=str(self.socket))
        try:
            async with httpx.AsyncClient(
                transport=transport,
                base_url="http://rclone",
                timeout=timeout or self.timeout,
            ) as client:
                r = await client.post(f"/{endpoint}", json=params)
        except httpx.HTTPError as e:
            raise RcError(f"{endpoint}: {e or type(e).__name__}") from e
        try:
            body = r.json()
        except ValueError:
            body = {"error": r.text}
        if r.status_code != 200:
            raise RcError(f"{endpoint}: {body.get('error', r.status_code)}")
        return body

    async def alive(self) -> bool:
        try:
            await self.call("rc/noop", timeout=5)
            return True
        except RcError:
            return False

    async def job_status(self, jobid: int) -> dict[str, Any]:
        st: dict[str, Any] = await self.call("job/status", jobid=jobid)
        return st

    async def job_stop(self, jobid: int) -> None:
        await self.call("job/stop", jobid=jobid)

    async def job_wait(self, jobid: int, interval: float = 5.0) -> dict[str, Any]:
        while True:
            st = await self.job_status(jobid)
            if st.get("finished"):
                return st
            await asyncio.sleep(interval)

    async def stats(self, group: str | None = None, short: bool = False) -> dict[str, Any]:
        params: dict[str, Any] = {"short": short}
        if group:
            params["group"] = group
        st: dict[str, Any] = await self.call("core/stats", **params)
        return st

    async def stats_reset(self, group: str | None = None) -> None:
        params: dict[str, Any] = {"group": group} if group else {}
        await self.call("core/stats-reset", **params)

    async def bwlimit(self, rate: str) -> dict[str, Any]:
        r: dict[str, Any] = await self.call("core/bwlimit", rate=rate)
        return r

    async def backend_set(self, fs: str, opt: dict[str, str]) -> None:
        await self.call("backend/command", command="set", fs=fs, opt=opt)
