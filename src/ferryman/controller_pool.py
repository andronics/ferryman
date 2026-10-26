"""Agent side of the controller: pools whose keys and rotation state live on
the controller, shared by every agent that leases from them.

A lease carries the key's credentials. They are written to a 0600 file in the
runtime dir (tmpfs) because rclone's drive `backend set` takes a key *file*;
nothing else on the agent ever holds them. If the controller goes away the
agent keeps using its last lease and only loses rotation; it retries and
complains in the log until the controller answers again.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx

from .notify import Emit, Event, discard
from .pool import Lease, PoolError, PoolExhausted

log = logging.getLogger(__name__)

WATCH_RETRY_MIN = 5.0
WATCH_RETRY_MAX = 60.0


class ControllerError(PoolError):
    pass


class ControllerClient:
    def __init__(
        self,
        url: str,
        token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.timeout = timeout
        self._client = httpx.AsyncClient(
            base_url=url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            transport=transport,
        )

    @classmethod
    def from_token_file(
        cls, url: str, token_file: Path, transport: httpx.AsyncBaseTransport | None = None
    ) -> ControllerClient:
        return cls(url, token_file.read_text().strip(), transport=transport)

    async def request(self, method: str, path: str, timeout: float | None = None, **kw: Any) -> Any:
        try:
            r = await self._client.request(method, path, timeout=timeout or self.timeout, **kw)
        except httpx.HTTPError as e:
            raise ControllerError(f"controller unreachable: {e or type(e).__name__}") from e
        try:
            body = r.json()
        except ValueError:
            body = {"error": r.text[:200]}
        if r.status_code == 409 and "frees_at" in body:
            raise PoolExhausted(path.split("/")[3], float(body["frees_at"]))
        if r.status_code != 200:
            raise ControllerError(f"controller: {body.get('error', f'HTTP {r.status_code}')}")
        return body

    async def aclose(self) -> None:
        await self._client.aclose()


class ControllerPool:
    def __init__(self, name: str, client: ControllerClient, keys_dir: Path) -> None:
        self.name = name
        self.client = client
        self.keys_dir = keys_dir
        self._lease: Lease | None = None
        self._size = 0
        self.emit: Emit = discard
        # Leases are served from the cache while the controller is away.
        self.reachable = True

    @property
    def size(self) -> int:
        return self._size

    @property
    def rotates(self) -> bool:
        return self._size > 1

    @property
    def epoch(self) -> int:
        return self._lease.epoch if self._lease else -1

    @property
    def current(self) -> str | None:
        return self._lease.key if self._lease else None

    def _up(self) -> None:
        if not self.reachable:
            self.reachable = True
            self.emit(Event("controller.recovered", self.name, "reachable again", "info",
                            resolves=True))  # fmt: skip

    def _down(self, e: Exception) -> None:
        self.reachable = False
        self.emit(Event("controller.unreachable", self.name, str(e)))

    def _store(self, body: dict[str, Any]) -> Lease:
        key = str(body["key"])
        creds = body["credentials"]
        if creds.get("type") != "service_account" or creds.get("client_email") != key:
            raise ControllerError(f"pool {self.name}: controller sent a bad key for {key}")
        self.keys_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = self.keys_dir / f"{key}.json"
        tmp = path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(creds, f)
        os.replace(tmp, path)
        # Keys the pool has moved off stay valid credentials; don't leave them lying around.
        for old in self.keys_dir.glob("*.json"):
            if old != path:
                old.unlink(missing_ok=True)
        self._size = int(body["size"])
        lease = Lease(key, path, int(body["epoch"]))
        if self._lease is None or lease.epoch != self._lease.epoch:
            log.info("pool %s: leased %s (epoch %d)", self.name, key, lease.epoch)
        self._lease = lease
        return lease

    async def lease(self) -> Lease:
        try:
            lease = self._store(await self.client.request("GET", f"/v2/pools/{self.name}/lease"))
            self._up()
            return lease
        except PoolExhausted:
            self._up()
            raise
        except ControllerError as e:
            self._down(e)
            if self._lease is None:
                raise
            log.warning("pool %s: %s; keeping %s", self.name, e, self._lease.key)
            return self._lease

    async def exhausted(self, expected: str | None = None) -> Lease:
        body = await self.client.request(
            "POST", f"/v2/pools/{self.name}/exhausted", json={"expected": expected}
        )
        return self._store(body)

    def forget(self) -> None:
        """Remove leased credentials from disk (on agent shutdown)."""
        for path in self.keys_dir.glob("*.json"):
            path.unlink(missing_ok=True)
        self._lease = None

    async def info(self) -> dict[str, Any]:
        info: dict[str, Any] = await self.client.request("GET", f"/v2/pools/{self.name}")
        return info

    async def watch(self, on_change: Callable[[ControllerPool, Lease], Awaitable[None]]) -> None:
        """Follow the pool's epoch with long polls, calling `on_change` with
        each new lease: how a rotation triggered by another agent reaches
        this one's running units."""
        retry = WATCH_RETRY_MIN
        while True:
            epoch = self._lease.epoch if self._lease else -1
            try:
                body = await self.client.request(
                    "GET",
                    f"/v2/pools/{self.name}/watch",
                    params={"epoch": epoch},
                    # The server answers within its own watch timeout (30s by default).
                    timeout=self.client.timeout + 60,
                )
                retry = WATCH_RETRY_MIN
                self._up()
            except PoolExhausted:
                # Watch replies with a lease, so an all-exhausted pool is a
                # 409 here; it changes only when a key frees or rotates.
                self._up()
                await asyncio.sleep(retry)
                retry = min(retry * 2, WATCH_RETRY_MAX)
                continue
            except PoolError as e:
                self._down(e)
                log.warning("pool %s: watch failed (%s); retrying in %.0fs", self.name, e, retry)
                await asyncio.sleep(retry)
                retry = min(retry * 2, WATCH_RETRY_MAX)
                continue
            if int(body["epoch"]) != epoch:
                await on_change(self, self._store(body))
