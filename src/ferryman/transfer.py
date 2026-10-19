"""Transfers: move/copy/sync from a local path into a unit's remote.

A transfer runs as an rc job inside the unit's own rclone process, not as a
second rclone: one fs instance and one credential per unit, so a pool
rotation moves the mount and the transfer together, and the job's result
comes back over the socket instead of an exit code plus a log grep. A
process restart aborts the job; the next run resumes (a move's finished
files are already gone from the source).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from croniter import croniter

from .config import Transfer
from .pool import PoolError, is_quota_error
from .rc import RcError

if TYPE_CHECKING:
    from .agent import Agent

log = logging.getLogger(__name__)

_ENDPOINTS: dict[str, tuple[str, dict[str, Any]]] = {
    "move": ("sync/move", {"deleteEmptySrcDirs": True}),
    "copy": ("sync/copy", {}),
    "sync": ("sync/sync", {}),
}


class TransferError(Exception):
    pass


class TransferBusy(TransferError):
    pass


class TransferRunner:
    def __init__(self, name: str, agent: Agent) -> None:
        self.name = name
        self.agent = agent
        self.lock = asyncio.Lock()
        self.jobid: int | None = None
        self.started: float | None = None
        self.next_run: float | None = None
        self.last: dict[str, Any] | None = None

    @property
    def t(self) -> Transfer:
        return self.agent.cfg.transfers[self.name]

    @property
    def group(self) -> str:
        # Stats group of this transfer's jobs inside the unit's process.
        return f"transfer-{self.name}"

    @property
    def running(self) -> bool:
        return self.lock.locked()

    def status(self) -> dict[str, Any]:
        return {
            "transfer": self.name,
            "unit": self.t.unit,
            "mode": self.t.mode,
            "schedule": self.t.schedule,
            "running": self.running,
            "jobid": self.jobid,
            "started": self.started if self.running else None,
            "next_run": self.next_run,
            "last": self.last,
        }

    async def run(self, dry_run: bool = False) -> dict[str, Any]:
        if self.lock.locked():
            raise TransferBusy(f"transfer {self.name} is already running")
        async with self.lock:
            self.started = time.time()
            result: dict[str, Any]
            try:
                result = await self._run(dry_run)
            except asyncio.CancelledError:
                self.last = {"ok": False, "error": "aborted", "started": self.started,
                             "finished": time.time(), "dry_run": dry_run}  # fmt: skip
                log.warning("transfer %s: aborted", self.name)
                raise
            except (TransferError, PoolError, RcError) as e:
                result = {"ok": False, "error": str(e)}
            result.update(started=self.started, finished=time.time(), dry_run=dry_run)
            self.last = result
            if result["ok"]:
                log.info("transfer %s: %s", self.name, describe(result))
            else:
                log.error("transfer %s failed: %s", self.name, result["error"])
            return result

    async def _run(self, dry_run: bool) -> dict[str, Any]:
        t, agent = self.t, self.agent
        src = agent.layout.transfer_source(self.name)
        # Most units never have anything staged, so a missing source is the
        # normal "nothing to do" case, not a failure.
        if not src.is_dir():
            return {"ok": True, "skipped": f"{src} does not exist"}
        sup = agent.units[t.unit]
        if not sup.ready:
            raise TransferError(f"unit {t.unit} is not ready ({sup.state.value})")

        # Stats groups outlive their jobs; without a reset a run would report
        # the previous run's counters and errors.
        with contextlib.suppress(RcError):
            await sup.rc.stats_reset(self.group)

        pool = agent.pools.get(sup.unit.pool or "")
        attempts = len(pool.cfg.keys) if pool and pool.rotates else 1
        endpoint, extra = _ENDPOINTS[t.mode]
        # Eight transfers: at Drive's per-request latency one stream idles
        # about half the time, eight of them fill the uplink.
        job_config = {"Transfers": t.transfers, "Checkers": t.checkers,
                      "BufferSize": "16M", "DryRun": dry_run}  # fmt: skip

        for _ in range(attempts):
            key = sup.key
            reply = await sup.rc.call(
                endpoint,
                srcFs=str(src),
                dstFs=f"{t.unit}:{t.dest}",
                _async=True,
                _group=self.group,
                _config=job_config,
                **extra,
            )
            jobid = int(reply["jobid"])
            self.jobid = jobid
            log.info("transfer %s: job %d started%s", self.name, jobid, f" on {key}" if key else "")
            try:
                st = await sup.rc.job_wait(jobid, self.agent.job_poll_interval)
            except asyncio.CancelledError:
                # Stopping ferryman must stop the job too, or it would carry
                # on inside the unit's process with nothing watching it.
                with contextlib.suppress(RcError):
                    await asyncio.shield(sup.rc.job_stop(jobid))
                raise
            finally:
                self.jobid = None

            if st.get("success"):
                return {"ok": True, **await self._summary()}
            err = str(st.get("error") or "")
            if pool is None or not pool.rotates or not is_quota_error(err):
                raise TransferError(f"job {jobid}: {err or 'failed'}")
            log.warning("transfer %s: upload cap hit on %s", self.name, key)
            await agent.rotate(pool.name, expected=key)
            # Clears the group's lastError so the health check does not
            # rotate again for the same hit.
            with contextlib.suppress(RcError):
                await sup.rc.stats_reset(self.group)
        raise TransferError("upload cap hit on every key in the pool")

    async def _summary(self) -> dict[str, Any]:
        try:
            st = await self.agent.units[self.t.unit].rc.stats(self.group, short=True)
        except RcError:
            return {}
        return {
            "files": st.get("transfers", 0),
            "bytes": st.get("bytes", 0),
            "errors": st.get("errors", 0),
        }

    async def schedule(self) -> None:
        """Run on the cron schedule until cancelled. Runs that would overlap a
        still-running one are skipped."""
        if not self.t.schedule:
            return
        it = croniter(self.t.schedule, datetime.now().astimezone())
        while True:
            self.next_run = it.get_next(float)
            await asyncio.sleep(max(0.0, self.next_run - time.time()))
            if self.running:
                log.info("transfer %s: previous run still going, skipping", self.name)
                continue
            with contextlib.suppress(TransferBusy):
                await self.run()


def describe(result: dict[str, Any]) -> str:
    if result.get("skipped"):
        return f"nothing to do ({result['skipped']})"
    return (
        f"{result.get('files', 0)} file(s), {iec(result.get('bytes', 0))}, "
        f"{result.get('errors', 0)} error(s)"
    )


def iec(n: float) -> str:
    for unit, size in (("G", 1 << 30), ("M", 1 << 20), ("K", 1 << 10)):
        if n >= size:
            return f"{n / size:.1f}{unit}"
    return f"{n:.0f}B"
