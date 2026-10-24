"""The agent: supervises every unit, schedules transfers, runs the health
check, rotates pools, and serves the control API."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
from typing import Any

import httpx

from . import mountutil, patterns, sdnotify
from .config import Config, Layout
from .coordinator_pool import CoordinatorClient, CoordinatorPool
from .pool import Lease, LocalPool, Pool, PoolError, is_quota_error
from .rc import RcError
from .transfer import TransferRunner
from .unit import UnitSupervisor

log = logging.getLogger(__name__)


class Agent:
    def __init__(
        self, cfg: Config, coordinator_transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.cfg = cfg
        self.layout = Layout(cfg)
        self.job_poll_interval = 5.0
        self.coordinator = (
            CoordinatorClient.from_token_file(
                cfg.coordinator.url, cfg.coordinator.token_file, coordinator_transport
            )
            if cfg.coordinator
            else None
        )
        self.pools: dict[str, Pool] = {}
        for name, p in cfg.pools.items():
            if p.coordinator:
                assert self.coordinator is not None
                self.pools[name] = CoordinatorPool(
                    name, self.coordinator, self.layout.leased_keys(name)
                )
            else:
                self.pools[name] = LocalPool(name, p, self.layout.pool_state(name))
        self.units = {
            name: UnitSupervisor(name, cfg, self.layout, self.pools.get(u.pool or ""))
            for name, u in cfg.units.items()
        }
        self.transfers = {name: TransferRunner(name, self) for name in cfg.transfers}
        # Transfers on coordinator pools must hold a claim before they run;
        # name -> why a transfer may not run (absent: it may).
        self.claim_blocks: dict[str, str] = {}
        self._load_claims()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stopping = asyncio.Event()

    # -------------------------------------------------------------- lifecycle

    def spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        task: asyncio.Task[Any] = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def start(self) -> None:
        self.cfg.paths.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        for sup in self.units.values():
            sup.start()
        for pool in self.pools.values():
            if isinstance(pool, CoordinatorPool):
                self.spawn(pool.watch(self.apply), f"watch:{pool.name}")
        if self._claims():
            self.spawn(self._claims_loop(), "claims")
        for tr in self.transfers.values():
            if tr.t.schedule:
                self.spawn(tr.schedule(), f"schedule:{tr.name}")
        self.spawn(self._health_loop(), "health")
        if (interval := sdnotify.watchdog_interval()) is not None:
            self.spawn(self._watchdog(interval), "watchdog")

    async def stop(self) -> None:
        # Transfers first (stopping their jobs), then the processes under them.
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(*(sup.stop() for sup in self.units.values()))
        for pool in self.pools.values():
            if isinstance(pool, CoordinatorPool):
                pool.forget()
        if self.coordinator:
            await self.coordinator.aclose()

    async def run(self) -> None:
        from .api import serve

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stopping.set)
        server = await serve(self)
        await self.start()
        sdnotify.notify("READY=1")
        log.info("ferryman agent ready: %d unit(s), %d transfer(s)",
                 len(self.units), len(self.transfers))  # fmt: skip
        await self._stopping.wait()
        log.info("shutting down")
        sdnotify.notify("STOPPING=1")
        await self.stop()
        await server.stop()

    async def _watchdog(self, interval: float) -> None:
        while True:
            sdnotify.notify("WATCHDOG=1")
            await asyncio.sleep(interval)

    # --------------------------------------------------------------- rotation

    async def rotate(self, pool_name: str, expected: str | None = None) -> str:
        """Advance a pool and move every running unit on it to the new key in
        place via `backend set`: no remount, the VFS and dir caches survive,
        a transfer job running in the process carries on under the new key,
        and an upload in flight on the old key fails once and is retried."""
        pool = self.pools[pool_name]
        lease = await pool.exhausted(expected)
        await self.apply(pool, lease)
        return lease.key

    async def apply(self, pool: Pool, lease: Lease) -> None:
        """Move every ready unit on `pool` to `lease`'s key. Units that are not
        ready take the current lease when they next start."""
        for sup in self.units.values():
            if sup.pool is not pool or not sup.ready or sup.key == lease.key:
                continue
            try:
                await sup.rc.backend_set(f"{sup.name}:", {"service_account_file": str(lease.path)})
                sup.key, sup.key_path = lease.key, lease.path
                sup.write_conf()  # the runtime conf keeps saying what the process runs on
                log.info("unit %s: switched to %s", sup.name, lease.key)
            except RcError as e:
                log.error(
                    "unit %s: could not switch to %s (%s); restarting", sup.name, lease.key, e
                )
                self.spawn(sup.restart(), f"restart:{sup.name}")

    # ----------------------------------------------------------------- claims

    def _claims(self) -> list[dict[str, str]]:
        """Claims for every transfer whose unit's pool is on the coordinator.
        A drive is identified by its id (or, without one, its pool)."""
        out = []
        for name, t in self.cfg.transfers.items():
            u = self.cfg.units[t.unit]
            if u.pool and isinstance(self.pools.get(u.pool), CoordinatorPool):
                target = u.drive_id or f"pool:{u.pool}"
                dest = self.layout.transfer_dest(name)
                out.append({"transfer": name, "target": target, "dest": dest, "mode": t.mode})
        return out

    def _load_claims(self) -> None:
        """Until the coordinator answers, a transfer may run only if it was
        granted before (persisted), unchanged. A coordinator outage then
        stops nothing that was already running fine."""
        try:
            granted = json.loads(self.layout.claims_file.read_text())
        except (FileNotFoundError, ValueError):
            granted = []
        for c in self._claims():
            if c not in granted:
                self.claim_blocks[c["transfer"]] = "awaiting a claim from the coordinator"

    async def _claims_loop(self) -> None:
        assert self.coordinator is not None
        wanted = self._claims()
        while True:
            try:
                r = await self.coordinator.request("PUT", "/v1/claims", json=wanted)
            except PoolError as e:
                log.warning("claims: %s; retrying in 30s", e)
                await asyncio.sleep(30)
                continue
            accepted = set(r["accepted"])
            self.claim_blocks = {
                c["transfer"]: f"destination overlaps {c['owner']}'s transfer "
                f"{c['owner_transfer']} on the same drive"
                for c in r["conflicts"]
            }
            for name, why in self.claim_blocks.items():
                log.error("transfer %s will not run: %s", name, why)
            granted = [c for c in wanted if c["transfer"] in accepted]
            path = self.layout.claims_file
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(granted, indent=2) + "\n")
            os.replace(tmp, path)
            # Re-assert now and then: the coordinator may have lost its state.
            await asyncio.sleep(600)

    # ----------------------------------------------------------------- health

    async def _health_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.health.interval)
            await self.check_health()

    async def check_health(self) -> dict[str, str]:
        """Restart ready units that no longer work, and rotate on upload-cap
        hits nothing else handled. Being in the mount table is not enough: a
        wedged FUSE endpoint (network drop, revoked key) stays mounted while
        I/O hangs, so a bounded stat is required too. Sync and rotation both
        go through the rc socket, so a unit that no longer answers rc is
        unhealthy as well."""
        report: dict[str, str] = {}
        for sup in list(self.units.values()):
            if not sup.ready:
                report[sup.name] = sup.state.value
                continue
            reason = None
            if sup.unit.mount:
                path = self.layout.mount_path(sup.name)
                if not mountutil.is_mounted(path):
                    reason = "not mounted"
                elif not await mountutil.responsive(path, self.cfg.health.stat_timeout):
                    reason = "mountpoint not responding"
            if reason is None and not await sup.rc.alive():
                reason = "rc not answering"
            if reason:
                log.warning("unit %s unhealthy (%s): restarting", sup.name, reason)
                sup.last_error = reason
                self.spawn(sup.restart(), f"restart:{sup.name}")
                report[sup.name] = f"restarted: {reason}"
                continue
            report[sup.name] = "healthy"
            await self._unhandled_cap_hit(sup)
        return report

    async def _unhandled_cap_hit(self, sup: UnitSupervisor) -> None:
        # A cap hit nothing handled (a VFS writeback upload, say) sits in the
        # process's stats as lastError. A running transfer handles its own
        # hits, so skip units with one: rotating here as well could advance
        # the pool twice for a single hit.
        if sup.pool is None or not sup.pool.rotates:
            return
        if any(tr.running and tr.t.unit == sup.name for tr in self.transfers.values()):
            return
        try:
            st = await sup.rc.stats(short=True)
        except RcError:
            return
        if not is_quota_error(st.get("lastError")):
            return
        log.warning("unit %s: upload cap hit - rotating pool %s", sup.name, sup.pool.name)
        try:
            await self.rotate(sup.pool.name, expected=sup.key)
        except PoolError as e:
            log.error("%s", e)
        with contextlib.suppress(RcError):
            await sup.rc.stats_reset()

    # ------------------------------------------------------------------ views

    def status(self) -> dict[str, Any]:
        return {
            "units": [sup.status() for sup in self.units.values()],
            "transfers": [tr.status() for tr in self.transfers.values()],
        }

    def select_units(self, pattern: str | None) -> list[UnitSupervisor]:
        return [self.units[n] for n in patterns.match(pattern, self.units)]

    async def stats(self, pattern: str | None = None) -> list[dict[str, Any]]:
        """What each ready unit's process is doing right now (the sum over its
        transfer jobs and the VFS): SPEED is the current rate over files in
        flight; bytes and files count since start or the last stats reset."""
        out: list[dict[str, Any]] = []
        for sup in self.select_units(pattern):
            if not sup.ready:
                continue
            try:
                st = await sup.rc.stats()
            except RcError as e:
                out.append({"unit": sup.name, "error": str(e)})
                continue
            out.append({
                "unit": sup.name,
                "speed": sum(t.get("speedAvg", 0) for t in st.get("transferring") or []),
                "bytes": st.get("bytes", 0),
                "files": st.get("transfers", 0),
                "errors": st.get("errors", 0),
                "last_error": st.get("lastError"),
            })  # fmt: skip
        return out

    async def jobs(self, pattern: str | None = None) -> list[dict[str, Any]]:
        """rc jobs running inside each ready unit's process: transfers
        (group transfer-<name>) and dir-cache refreshes."""
        out: list[dict[str, Any]] = []
        for sup in self.select_units(pattern):
            if not sup.ready:
                continue
            try:
                listing = await sup.rc.call("job/list")
                for jobid in listing.get("runningIds") or []:
                    st = await sup.rc.job_status(jobid)
                    # rclone runs rc calls as jobs too; job/list itself is
                    # finished by the time we ask, so only real work remains.
                    if st.get("finished"):
                        continue
                    out.append({
                        "unit": sup.name,
                        "jobid": jobid,
                        "group": st.get("group"),
                        "duration": st.get("duration"),
                    })  # fmt: skip
            except RcError as e:
                out.append({"unit": sup.name, "error": str(e)})
        return out

    async def throttle(self, pattern: str, rate: str) -> list[dict[str, Any]]:
        """core/bwlimit on each matching ready unit ("10M", "5M:1M" up:down,
        "off"). Lasts until the process restarts; nothing persists it."""
        out: list[dict[str, Any]] = []
        for sup in self.select_units(pattern):
            if not sup.ready:
                continue
            try:
                r = await sup.rc.bwlimit(rate)
                out.append({"unit": sup.name, "rate": r.get("rate")})
            except RcError as e:
                out.append({"unit": sup.name, "error": str(e)})
        return out
