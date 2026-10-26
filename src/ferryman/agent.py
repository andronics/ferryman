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

from . import __version__, mountutil, patterns, sdnotify
from .config import Config, Layout
from .controller_pool import ControllerClient, ControllerPool
from .notify import Event, Notifier
from .pool import Lease, LocalPool, Pool, PoolError, PoolExhausted, is_quota_error
from .rc import RcError
from .transfer import TransferBusy, TransferRunner
from .tuning import TuneError, Tuner
from .unit import UnitSupervisor

log = logging.getLogger(__name__)


class Agent:
    def __init__(
        self, cfg: Config, controller_transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.cfg = cfg
        self.layout = Layout(cfg)
        self.notifier = Notifier(cfg.notify, self.layout.hostname)
        self.job_poll_interval = 5.0
        self.controller = (
            ControllerClient.from_token_file(
                cfg.controller.url, cfg.controller.token_file, controller_transport
            )
            if cfg.controller
            else None
        )
        self.pools: dict[str, Pool] = {}
        for name, p in cfg.pools.items():
            if p.controller:
                assert self.controller is not None
                self.pools[name] = ControllerPool(
                    name, self.controller, self.layout.leased_keys(name)
                )
            else:
                self.pools[name] = LocalPool(name, p, self.layout.pool_state(name))
        self.units = {
            name: UnitSupervisor(name, cfg, self.layout, self.pools.get(u.pool or ""))
            for name, u in cfg.units.items()
        }
        for sup in self.units.values():
            sup.emit = self.notifier.emit
        for pool in self.pools.values():
            if isinstance(pool, ControllerPool):
                pool.emit = self.notifier.emit
        self.transfers = {name: TransferRunner(name, self) for name in cfg.transfers}
        self.tuner = Tuner(self)
        # Transfers on controller pools must hold a claim before they run;
        # name -> why a transfer may not run (absent: it may).
        self.claim_blocks: dict[str, str] = {}
        self._load_claims()
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stopping = asyncio.Event()

    # -------------------------------------------------------------- lifecycle

    def spawn(self, coro: Any, name: str) -> asyncio.Task[Any]:
        return self._track(asyncio.create_task(coro, name=name))

    def _track(self, task: asyncio.Task[Any]) -> asyncio.Task[Any]:
        # Tracked tasks are cancelled before the units stop, so nothing (a
        # restart, say) starts a unit again during shutdown.
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def restart(self, sup: UnitSupervisor) -> asyncio.Task[None]:
        task = sup.restart_soon()
        self._track(task)
        return task

    async def start(self) -> None:
        self.cfg.paths.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        for sup in self.units.values():
            sup.start()
        for pool in self.pools.values():
            if isinstance(pool, ControllerPool):
                self.spawn(pool.watch(self.apply), f"watch:{pool.name}")
        if self._claims():
            self.spawn(self._claims_loop(), "claims")
        if self.cfg.controller:
            self.spawn(self._report_loop(), "report")
            if self.cfg.controller.commands:
                self.spawn(self._command_loop(), "commands")
        for tr in self.transfers.values():
            if tr.t.schedule:
                self.spawn(tr.schedule(), f"schedule:{tr.name}")
        if self.cfg.tuning.schedule:
            self.spawn(self.tuner.schedule(), "tune-schedule")
        self.spawn(self._health_loop(), "health")
        if self.notifier.enabled:
            self.spawn(self.notifier.run(), "notify")
        if (interval := sdnotify.watchdog_interval()) is not None:
            self.spawn(self._watchdog(interval), "watchdog")

    async def stop(self) -> None:
        # Transfers first (stopping their jobs), then the processes under them.
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(*(sup.stop() for sup in self.units.values()))
        for pool in self.pools.values():
            if isinstance(pool, ControllerPool):
                pool.forget()
        if self.controller:
            await self.controller.aclose()

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
        before = pool.epoch
        try:
            lease = await pool.exhausted(expected)
        except PoolExhausted as e:
            self.notifier.emit(Event("pool.exhausted", pool_name, str(e), "error"))
            raise
        if lease.epoch != before:
            why = f"cap hit on {expected}" if expected else "rotated by hand"
            self.notifier.emit(Event("pool.rotated", pool_name, f"now on {lease.key} ({why})",
                                     "info"))  # fmt: skip
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
                self.restart(sup)

    # ----------------------------------------------------------------- claims

    def _claims(self) -> list[dict[str, str]]:
        """Claims for every transfer whose unit's pool is on the controller.
        A drive is identified by its id (or, without one, its pool)."""
        out = []
        for name, t in self.cfg.transfers.items():
            u = self.cfg.units[t.unit]
            if u.pool and isinstance(self.pools.get(u.pool), ControllerPool):
                target = u.drive_id or f"pool:{u.pool}"
                dest = self.layout.claim_dest(name)
                out.append({"transfer": name, "target": target, "dest": dest, "mode": t.mode})
        return out

    def _load_claims(self) -> None:
        """Until the controller answers, a transfer may run only if it was
        granted before (persisted), unchanged. A controller outage then
        stops nothing that was already running fine."""
        try:
            granted = json.loads(self.layout.claims_file.read_text())
        except (FileNotFoundError, ValueError):
            granted = []
        for c in self._claims():
            if c not in granted:
                self.claim_blocks[c["transfer"]] = "awaiting a claim from the controller"

    async def _claims_loop(self) -> None:
        assert self.controller is not None
        wanted = self._claims()
        while True:
            try:
                r = await self.controller.request("PUT", "/v1/claims", json=wanted)
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
                self.notifier.emit(Event("transfer.blocked", name, why, "error"))
            granted = [c for c in wanted if c["transfer"] in accepted]
            path = self.layout.claims_file
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(granted, indent=2) + "\n")
            os.replace(tmp, path)
            # Re-assert now and then: the controller may have lost its state.
            await asyncio.sleep(600)

    # ------------------------------------------------- controller UI support

    async def report(self) -> dict[str, Any]:
        """What the controller's UI shows for this agent."""
        local = [p.describe() for p in self.pools.values() if isinstance(p, LocalPool)]
        return {
            "version": __version__,
            "status": self.status(),
            "stats": await self.stats(),
            "pools": local,
            "events": self.notifier.recent(20),
        }

    async def _report_loop(self) -> None:
        assert self.controller is not None and self.cfg.controller is not None
        while True:
            try:
                await self.controller.request("PUT", "/v1/report", json=await self.report())
            except PoolError as e:
                log.debug("report: %s", e)  # the watch already complains loudly
            await asyncio.sleep(self.cfg.controller.report_interval)

    async def _command_loop(self) -> None:
        """Commands from the controller's UI, collected with long polls."""
        assert self.controller is not None
        retry = 5.0
        while True:
            try:
                cmds = await self.controller.request(
                    "GET", "/v1/commands", timeout=self.controller.timeout + 60
                )
                retry = 5.0
            except PoolError as e:
                log.debug("commands: %s; retrying in %.0fs", e, retry)
                await asyncio.sleep(retry)
                retry = min(retry * 2, 60.0)
                continue
            for c in cmds:
                self.spawn(self.execute(c), f"command:{c['id']}")

    async def execute(self, cmd: dict[str, Any]) -> dict[str, Any]:
        action, target = cmd["action"], cmd["target"]
        log.info("command %s from the controller: %s %s", cmd["id"], action, target)
        result: dict[str, Any]
        try:
            if action == "restart":
                sup = self.units[target]
                await asyncio.shield(self.restart(sup))
                result = {"ok": True, "state": sup.state.value}
            elif action == "tune":
                self.tuner.check()
                apply = cmd.get("args", {}).get("apply")
                self.spawn(self._tune_quietly(apply), "tune")
                result = {"ok": True, "started": True}
            elif action == "run":
                tr = self.transfers[target]
                if tr.running:
                    raise TransferBusy(f"transfer {target} is already running")
                dry_run = bool(cmd.get("args", {}).get("dry_run"))
                self.spawn(tr.run(dry_run), f"run:{target}")
                result = {"ok": True, "started": True}
            else:
                result = {"ok": False, "error": f"unknown action {action!r}"}
        except KeyError as e:
            result = {"ok": False, "error": f"not found: {e.args[0]}"}
        except (TransferBusy, TuneError) as e:
            result = {"ok": False, "error": str(e)}
        if self.controller is not None:
            try:
                await self.controller.request("POST", f"/v1/commands/{cmd['id']}", json=result)
            except PoolError as e:
                log.warning("command %s: could not report its result: %s", cmd["id"], e)
        return result

    async def _tune_quietly(self, apply: bool | None) -> None:
        # Nobody awaits a tune started from the UI: its outcome is the
        # tuner's status and events, which the next report carries.
        try:
            await self.tuner.run(apply=apply)
        except TuneError as e:
            log.warning("tune from the controller: %s", e)

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
            if sup.restarting:
                report[sup.name] = "restarting"
                continue
            if not sup.ready:
                report[sup.name] = sup.state.value
                continue
            reason = None
            if sup.unit.mounts:
                path = sup.primary_path
                if not mountutil.is_mounted(path):
                    reason = "not mounted"
                elif not await mountutil.responsive(path, self.cfg.health.stat_timeout):
                    reason = "mountpoint not responding"
            if reason is None and not await sup.rc.alive():
                reason = "rc not answering"
            # The probes await: something else may have restarted it meanwhile.
            if reason and not sup.restarting and sup.ready:
                log.warning("unit %s unhealthy (%s): restarting", sup.name, reason)
                sup.failed(f"unhealthy: {reason}")
                self.restart(sup)
                report[sup.name] = f"restarted: {reason}"
                continue
            if reason:
                report[sup.name] = "restarting"
                continue
            notes = [*await sup.heal_mounts(), *await sup.heal_serves()]
            report[sup.name] = "; ".join(["healthy", *notes])
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
            "hostname": self.layout.hostname,
            "units": [sup.status() for sup in self.units.values()],
            "transfers": [tr.status() for tr in self.transfers.values()],
            "tuning": self.tuner.status(),
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
