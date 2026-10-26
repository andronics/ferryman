"""Service-account key pools and rotation on Drive's daily upload cap.

Google Drive caps uploads at ~750G per identity per rolling 24h. A pool is an
ordered list of service-account keys that are all members of the same shared
drives; uploads go through the current key until Drive answers
"User rate limit exceeded.", then the pool advances to the next key. A
single-key pool never rotates.

State is per pool, not per unit: the quota is per account across every drive
it touches, so once one consumer exhausts it, all of them move. A pool is
either local (state in a file on this host) or served by the coordinator
(state shared by every agent leasing from it; see coordinator_pool.py). Every
rotation bumps the pool's epoch, which is how watchers notice it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .config import Pool as PoolConfig

log = logging.getLogger(__name__)

# The exact message rclone's drive backend treats as the daily upload cap (and
# makes fatal under stop_on_upload_limit). Case matters: the per-request
# throttle is "User Rate Limit Exceeded" and is retried, not rotated.
QUOTA_MSG = "User rate limit exceeded."


def is_quota_error(message: str | None) -> bool:
    return bool(message) and QUOTA_MSG in (message or "")


class PoolError(Exception):
    pass


class NoOtherKey(PoolError):
    """A rotation by hand found no other usable key; the current one stays."""


class PoolExhausted(PoolError):
    def __init__(self, pool: str, frees_at: float) -> None:
        self.frees_at = frees_at
        super().__init__(
            f"pool {pool}: every key is exhausted; earliest frees at "
            f"{time.strftime('%F %T', time.localtime(frees_at))}"
        )


@dataclass(frozen=True)
class Lease:
    """The key a pool's consumers should use: its name, a local file rclone
    can read it from, and the pool epoch it was current in."""

    key: str
    path: Path
    epoch: int


class Pool(Protocol):
    name: str

    @property
    def size(self) -> int: ...

    @property
    def rotates(self) -> bool: ...

    @property
    def epoch(self) -> int:
        """The rotation count as this host last knew it."""
        ...

    async def lease(self) -> Lease: ...

    async def exhausted(self, expected: str | None = None) -> Lease:
        """Report a cap hit on `expected` (or the current key) and return the
        key to move to. Rotates at most once per key, however many callers
        report the same hit."""
        ...

    async def info(self) -> dict[str, Any]: ...


class LocalPool:
    """A pool whose state lives in a JSON file on this host:
    {"current": key, "exhausted": {key: epoch}, "rotated_at": epoch}"""

    def __init__(
        self,
        name: str,
        cfg: PoolConfig,
        state_file: Path,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.name = name
        self.cfg = cfg
        self.state_file = state_file
        self.clock = clock
        self.lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return len(self.cfg.keys)

    @property
    def rotates(self) -> bool:
        return self.size > 1

    @property
    def epoch(self) -> int:
        return int(self._read().get("epoch", 0))

    def _read(self) -> dict[str, Any]:
        try:
            data: dict[str, Any] = json.loads(self.state_file.read_text())
            return data
        except FileNotFoundError:
            return {}

    def _write(self, state: dict[str, Any]) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
        os.replace(tmp, self.state_file)

    def file_ok(self, key: str) -> bool:
        try:
            self.key_file(key)
            return True
        except PoolError:
            return False

    def available(self) -> list[str]:
        """Keys not exhausted within the quota window and with a good key file,
        in config order. Key files are read every time, so a fixed one is
        back in rotation without a restart."""
        now = self.clock()
        exhausted = self._read().get("exhausted", {})
        window = self.cfg.quota_window
        return [k for k in self.cfg.keys if exhausted.get(k, 0) + window <= now and self.file_ok(k)]

    def current(self) -> str:
        """The key uploads should use right now. Settles on the first available
        key when the state is empty or the recorded one has been exhausted or
        its file has gone bad."""
        state = self._read()
        avail = self.available()
        if not avail:
            raise self._unavailable(state)
        cur = state.get("current")
        if cur in avail:
            return str(cur)
        if cur and not self.file_ok(cur):
            log.warning("pool %s: key file for %s is bad; moving to %s", self.name, cur, avail[0])
        state["current"] = avail[0]
        self._write(state)
        return avail[0]

    async def advance(self, expected: str | None = None) -> str:
        """Move to the next available key.

        With `expected`, this is a cap hit on that key: it is marked exhausted
        for the quota window, and the call is a no-op (returning the actual
        current key) when someone already rotated past it, so N transfers that
        all hit the cap on the same key rotate once, not N times.

        Without, it is a rotation by hand: the current key keeps its quota and
        stays available, and NoOtherKey is raised if no other key is usable."""
        async with self.lock:
            if not expected and not self._read().get("current"):
                # Never used yet: rotating by hand means moving past the key
                # it would settle on, not onto it.
                self.current()
            state = self._read()
            cur = state.get("current")
            if expected and cur and cur != expected:
                log.info("pool %s: already rotated past %s (now %s)", self.name, expected, cur)
                return str(cur)

            now = self.clock()
            spent = cur or expected
            if expected and spent:
                state.setdefault("exhausted", {})[spent] = now
                self._write(state)

            # Next available key after the current one, wrapping, so the pool
            # is consumed in order rather than always restarting from the top.
            keys = self.cfg.keys
            i = keys.index(spent) if spent in keys else -1
            avail = set(self.available())
            nxt = next((k for k in keys[i + 1 :] + keys[: i + 1] if k in avail), None)
            if nxt is None:
                raise self._unavailable(state)
            if nxt == spent:
                raise NoOtherKey(f"pool {self.name}: no other usable key; staying on {spent}")

            state["current"] = nxt
            state["rotated_at"] = now
            state["epoch"] = int(state.get("epoch", 0)) + 1
            self._write(state)
            log.warning("pool %s: rotated %s -> %s", self.name, spent or "<none>", nxt)
            return nxt

    async def lease(self) -> Lease:
        key = self.current()
        return Lease(key, self.key_file(key), self.epoch)

    async def exhausted(self, expected: str | None = None) -> Lease:
        key = await self.advance(expected)
        return Lease(key, self.key_file(key), self.epoch)

    async def info(self) -> dict[str, Any]:
        return self.describe()

    def key_file(self, key: str) -> Path:
        """Path of a key, after checking it parses as a service-account key
        for that client_email (an encrypted or wrong file fails here, before
        rclone ever sees it)."""
        assert self.cfg.keys_dir is not None  # a local pool always has one
        path = self.cfg.keys_dir / f"{key}.json"
        try:
            data = json.loads(path.read_text())
        except FileNotFoundError:
            raise PoolError(f"pool {self.name}: key file missing: {path}") from None
        except (OSError, ValueError) as e:
            raise PoolError(f"pool {self.name}: unreadable key file {path}: {e}") from None
        if data.get("type") != "service_account" or data.get("client_email") != key:
            raise PoolError(f"pool {self.name}: {path} is not a service-account key for {key}")
        return path

    def describe(self) -> dict[str, Any]:
        state = self._read()
        now = self.clock()
        exhausted = state.get("exhausted", {})
        keys = []
        for k in self.cfg.keys:
            file_ok = self.file_ok(k)
            frees = exhausted.get(k, 0) + self.cfg.quota_window
            keys.append(
                {
                    "key": k,
                    "current": k == state.get("current"),
                    "file_ok": file_ok,
                    "exhausted_until": frees if frees > now else None,
                }
            )
        return {
            "pool": self.name,
            "quota_window": self.cfg.quota_window,
            "rotated_at": state.get("rotated_at"),
            "epoch": int(state.get("epoch", 0)),
            "keys": keys,
        }

    def _unavailable(self, state: dict[str, Any]) -> PoolError:
        """Why no key is available: exhausted keys with good files free in
        time; if there are none, the pool needs its key files fixed."""
        now = self.clock()
        ex = state.get("exhausted", {})
        window = self.cfg.quota_window
        frees = [t + window for k, t in ex.items()
                 if k in self.cfg.keys and t + window > now and self.file_ok(k)]  # fmt: skip
        if frees:
            return PoolExhausted(self.name, min(frees))
        return PoolError(f"pool {self.name}: no usable key: every key file is bad")
