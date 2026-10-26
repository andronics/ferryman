"""The agent: supervises every unit, schedules transfers, runs the health
check, rotates pools, and serves the control API."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from typing import Any

import httpx

from . import __version__, mountutil, patterns, sdnotify
from .bootstrap import Rejected, Start, build, fetch, save_cached
from .config import AgentBootstrap, Config, Layout, unit_spec
from .controller_pool import ControllerClient, ControllerPool
from .events import Outbox
from .notify import Event, Notifier
from .pool import Lease, LocalPool, Pool, PoolError, PoolExhausted, is_quota_error
from .rc import RcError
from .transfer import TransferBusy, TransferRunner
from .tuning import TuneError, Tuner
from .unit import UnitSupervisor

log = logging.getLogger(__name__)


class Agent:
    def __init__(
        self,
        cfg: Config,
        controller_transport: httpx.AsyncBaseTransport | None = None,
        bootstrap: AgentBootstrap | None = None,
        start: Start | None = None,
    ) -> None:
        self.cfg = cfg
        # The agent's own file and the slice it runs (None: a config given
        # directly, as tests do; nothing then follows the controller's).
        self.bootstrap = bootstrap
        self.slice = start.slice if start else None
        # What the controller sees of it: the version in force, and whether
        # it is fully applied, a cached one the controller hasn't confirmed
        # (cached), partly applied while running transfers finish (pending,
        # with what waits), or behind a newer one this host refused
        # (rejected, with why).
        self.config_state: dict[str, Any] = {
            "version": self.slice.get("version") if self.slice else None,
            "state": "cached" if start and start.source == "cache" else "applied",
            "error": start.rejected if start else None,
        }
        if start and start.rejected:
            self.config_state["state"] = "rejected"
        self.layout = Layout(cfg)
        self.notifier = Notifier(self.layout.hostname)
        # Agents notify no one: with a controller, every event goes into the
        # outbox, delivered to the controller in order, outages included.
        self.outbox: Outbox | None = None
        if cfg.controller:
            self.outbox = Outbox(self.layout.outbox)
            self.notifier.sink = self.outbox.append
        self.job_poll_interval = 5.0
        self.controller = (
            ControllerClient.from_token_file(
                cfg.controller.url, cfg.controller.token_file, controller_transport
            )
            if cfg.controller
            else None
        )
        self.pools: dict[str, Pool] = {name: self._pool(name) for name in cfg.pools}
        self.units = {name: self._supervisor(name) for name in cfg.units}
        self.transfers = {name: TransferRunner(name, self) for name in cfg.transfers}
        self.tuner = Tuner(self)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stopping = asyncio.Event()
        self._started = False
        # Per-name tasks a new config may cancel or replace.
        self._watches: dict[str, asyncio.Task[Any]] = {}
        self._schedules: dict[str, asyncio.Task[Any]] = {}
        self._tune_schedule: asyncio.Task[Any] | None = None
        self._tune_schedule_for: str | None = None
        # Units being stopped or swapped for a new config's; awaited at
        # shutdown, never cancelled (a unit must unmount cleanly).
        self._settling: set[asyncio.Task[Any]] = set()

    def _pool(self, name: str) -> Pool:
        p = self.cfg.pools[name]
        if p.controller:
            assert self.controller is not None
            pool = ControllerPool(name, self.controller, self.layout.leased_keys(name))
            pool.emit = self.notifier.emit
            return pool
        return LocalPool(name, p, self.layout.pool_state(name))

    def _supervisor(self, name: str) -> UnitSupervisor:
        u = self.cfg.units[name]
        sup = UnitSupervisor(name, self.cfg, self.layout, self.pools.get(u.pool or ""))
        sup.emit = self.notifier.emit
        return sup

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

    def _watch(self, name: str) -> None:
        pool = self.pools[name]
        if isinstance(pool, ControllerPool):
            self._watches[name] = self.spawn(pool.watch(self.apply), f"watch:{name}")

    def _schedule(self, name: str) -> None:
        tr = self.transfers[name]
        if tr.t.schedule:
            self._schedules[name] = self.spawn(tr.schedule(), f"schedule:{name}")

    async def start(self) -> None:
        self._started = True
        self.cfg.paths.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        for sup in self.units.values():
            sup.start()
        for name in self.pools:
            self._watch(name)
        if self.cfg.controller:
            self.spawn(self._report_loop(), "report")
            if self.bootstrap is not None and self.slice is not None:
                self.spawn(self._config_loop(), "config")
            if self.cfg.controller.commands:
                self.spawn(self._command_loop(), "commands")
        for name in self.transfers:
            self._schedule(name)
        if self.cfg.tuning.schedule:
            self._tune_schedule = self.spawn(self.tuner.schedule(), "tune-schedule")
            self._tune_schedule_for = self.cfg.tuning.schedule
        self.spawn(self._health_loop(), "health")
        if self.outbox is not None:
            self.spawn(self._events_loop(), "events")
        if (interval := sdnotify.watchdog_interval()) is not None:
            self.spawn(self._watchdog(interval), "watchdog")

    async def stop(self) -> None:
        self._stopping.set()
        # Transfers first (stopping their jobs), then the processes under them.
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await asyncio.gather(*self._settling, return_exceptions=True)
        await asyncio.gather(*(sup.stop() for sup in self.units.values()))
        # Leased keys stay: the next start may need them if the controller
        # is away then.
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

    # ------------------------------------------------- controller UI support

    async def report(self) -> dict[str, Any]:
        """What the controller's UI shows for this agent."""
        local = [p.describe() for p in self.pools.values() if isinstance(p, LocalPool)]
        return {
            "version": __version__,
            "status": self.status(),
            "stats": await self.stats(),
            "pools": local,
            "config": self.config_state,
            "outbox": len(self.outbox.entries) if self.outbox else 0,
        }

    async def _events_loop(self) -> None:
        """Deliver the outbox to the controller, oldest first; what isn't
        acknowledged is sent again (the controller ignores repeats)."""
        assert self.controller is not None and self.outbox is not None
        outbox = self.outbox
        outbox.wake = asyncio.Event()
        retry = 5.0
        while True:
            outbox.wake.clear()
            batch = outbox.pending()
            if not batch:
                await outbox.wake.wait()
                continue
            try:
                r = await self.controller.request(
                    "POST", "/v2/events", json={"outbox": outbox.id, "events": batch}
                )
                outbox.ack(int(r["acked"]))
                retry = 5.0
            except PoolError as e:
                log.debug("events: %s; %d waiting, retrying in %.0fs", e, len(outbox.entries),
                          retry)  # fmt: skip
                await asyncio.sleep(retry)
                retry = min(retry * 2, 60.0)

    async def _config_loop(self) -> None:
        """Follow the controller's slice for this agent with long polls."""
        assert self.controller is not None and self.bootstrap is not None
        retry = 5.0
        while True:
            # Wait on the newest version seen, so one pending or rejected
            # doesn't come straight back; ask outright while running a
            # cached one, so a controller that's back confirms it at once.
            seen = self.config_state.get("offered") or self.config_state["version"]
            wait = None if self.config_state["state"] == "cached" else seen
            try:
                slc = await fetch(self.controller, wait, timeout=self.controller.timeout + 60)
                retry = 5.0
            except PoolError as e:
                log.debug("config: %s; retrying in %.0fs", e, retry)
                await asyncio.sleep(retry)
                retry = min(retry * 2, 60.0)
                continue
            self.on_slice(slc)

    def on_slice(self, slc: dict[str, Any]) -> None:
        """A slice from the controller: the one in force (now confirmed), or
        a newer one, checked here, then applied in place."""
        if slc.get("version") == self.config_state["version"]:
            if self.config_state["state"] == "cached":
                self.config_state.update(state="applied", error=None)
            return
        if slc.get("version") == self.config_state.get("offered"):
            return  # already pending or rejected
        if "units" not in slc:
            return  # a long poll that timed out on a version we've seen
        assert self.bootstrap is not None
        try:
            cfg = build(self.bootstrap, slc)
        except Rejected as e:
            log.error("config version %s rejected, %s stays in force: %s",
                      slc.get("version"), self.config_state["version"], e)  # fmt: skip
            self.config_state.update(state="rejected", error=str(e), offered=slc.get("version"))
            return
        save_cached(self.bootstrap, slc)
        self.apply_config(cfg, slc)

    # ---------------------------------------------------------- live apply

    def apply_config(self, cfg: Config, slc: dict[str, Any]) -> None:
        """Put a new config in force without a restart: only what changed is
        touched, and nothing a running transfer uses changes under it."""
        log.info("applying config version %s", slc.get("version"))
        self.cfg, self.layout, self.slice = cfg, Layout(cfg), slc
        self.config_state = {"version": slc.get("version"), "state": "applied", "error": None}
        self.reconcile()

    def _busy(self, unit: str) -> list[str]:
        """Running transfers that use a unit."""
        return [n for n, tr in self.transfers.items() if tr.running and tr.t.unit == unit]

    def reconcile(self) -> None:
        """Bring what runs in line with self.cfg, as far as it can now; what
        must wait for a running transfer is listed as pending and done when
        that transfer ends (on_transfer_done calls this again)."""
        want, waits = self.cfg, []
        for name in want.pools:
            if name not in self.pools:
                self.pools[name] = self._pool(name)
                if self._started:
                    self._watch(name)
        # Units: new ones start, changed ones are swapped, removed ones stop.
        for name, sup in list(self.units.items()):
            if name in want.units:
                same_pool = sup.pool is self.pools.get(want.units[name].pool or "")
                if same_pool and unit_spec(sup.cfg, name) == unit_spec(want, name):
                    sup.cfg, sup.layout = want, self.layout  # equal: just the newer copy
                    continue
            if busy := self._busy(name):
                what = "restart" if name in want.units else "stop"
                waits.append(f"{what} unit {name} after transfer {', '.join(busy)}")
                continue
            new = self._supervisor(name) if name in want.units else None
            if new is None:
                del self.units[name]
            else:
                self.units[name] = new
            log.info("unit %s: %s for the new config", name, "restarting" if new else "stopping")
            self._settle(sup, new)
        for name in want.units:
            if name not in self.units:
                self.units[name] = self._supervisor(name)
                log.info("unit %s: starting for the new config", name)
                if self._started:
                    self.units[name].start()
        # Transfers: a changed or removed one finishes its run first.
        for name, tr in list(self.transfers.items()):
            new_t = want.transfers.get(name)
            if new_t == tr.t:
                continue
            if tr.running:
                what = "update" if new_t else "remove"
                waits.append(f"{what} transfer {name} after its run")
                continue
            if task := self._schedules.pop(name, None):
                task.cancel()
            del self.transfers[name]
            if new_t is not None:
                self.transfers[name] = TransferRunner(name, self)
                if self._started:
                    self._schedule(name)
        for name in want.transfers:
            if name not in self.transfers:
                self.transfers[name] = TransferRunner(name, self)
                if self._started:
                    self._schedule(name)
        # Pools no unit uses any more.
        for name in list(self.pools):
            if name not in want.pools and not any(
                s.pool is self.pools[name] for s in self.units.values()
            ):
                if task := self._watches.pop(name, None):
                    task.cancel()
                pool = self.pools.pop(name)
                if isinstance(pool, ControllerPool):
                    pool.discard()
        if self._started and want.tuning.schedule != self._tune_schedule_for:
            if self._tune_schedule is not None:
                self._tune_schedule.cancel()
                self._tune_schedule = None
            if want.tuning.schedule:
                self._tune_schedule = self.spawn(self.tuner.schedule(), "tune-schedule")
            self._tune_schedule_for = want.tuning.schedule
        if waits:
            self.config_state.update(state="pending", pending=waits)
            for w in waits:
                log.info("config version %s: %s", self.config_state["version"], w)
        else:
            self.config_state.update(state="applied")
            self.config_state.pop("pending", None)

    def _settle(self, old: UnitSupervisor, new: UnitSupervisor | None) -> None:
        """Stop a unit's process, then start its replacement (they share its
        rc socket and mount paths, so never both at once)."""

        async def go() -> None:
            await old.stop()
            if new is not None and self._started and not self._stopping.is_set():
                new.start()

        task = asyncio.create_task(go(), name=f"settle:{old.name}")
        self._settling.add(task)
        task.add_done_callback(self._settling.discard)

    def on_transfer_done(self, name: str) -> None:
        """A run ended: what a new config left waiting on it can go ahead."""
        if self.config_state.get("pending") and not self._stopping.is_set():
            self.reconcile()

    async def _report_loop(self) -> None:
        assert self.controller is not None and self.cfg.controller is not None
        while True:
            try:
                await self.controller.request("PUT", "/v2/report", json=await self.report())
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
                    "GET", "/v2/commands", timeout=self.controller.timeout + 60
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
                await self.controller.request("POST", f"/v2/commands/{cmd['id']}", json=result)
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
