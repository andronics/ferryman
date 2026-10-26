"""Per-unit supervisor: one rclone process per unit, restarted with backoff.

A mount unit runs `rclone mount` in the foreground; a transfer-only unit
(`mount: false`) runs `rclone rcd`. Either way the process listens on its own
rc socket, which is how ferryman warms, syncs, rotates and inspects it. One
process per unit isolates failures: a wedged FUSE endpoint takes down one
unit, not all of them.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import mountutil
from .config import Config, Layout, Unit, read_settings, resolved
from .notify import Emit, Event, discard
from .rc import RcClient, RcError

if TYPE_CHECKING:
    from .pool import Pool

log = logging.getLogger(__name__)

READY_TIMEOUT = 120.0
BACKOFF_MIN = 5.0
BACKOFF_MAX = 300.0
# A process that stayed up this long resets the backoff.
STABLE_AFTER = 600.0
STOP_TIMEOUT = 15.0
WARM_POLL = 5.0
WARM_RETRY_DELAY = 30.0


class State(StrEnum):
    STOPPED = "stopped"
    STARTING = "starting"
    READY = "ready"
    BACKOFF = "backoff"
    STOPPING = "stopping"


class UnitSupervisor:
    def __init__(self, name: str, cfg: Config, layout: Layout, pool: Pool | None) -> None:
        self.name = name
        self.cfg = cfg
        self.layout = layout
        self.pool = pool
        self.rc = RcClient(layout.rc_socket(name))
        self.state = State.STOPPED
        self.since = time.time()
        self.pid: int | None = None
        self.key: str | None = None
        self.key_path: Path | None = None
        self.restarts = 0
        self.last_error: str | None = None
        # The last dir-cache warm-up: state running/done/failed/cancelled.
        self.warm: dict[str, Any] | None = None
        self.warm_poll_interval = WARM_POLL
        self.warm_retry_delay = WARM_RETRY_DELAY
        # Upload chunk size from an applied tuning profile (bytes), if any.
        self.tuned_chunk: int | None = None
        self._warm_task: asyncio.Task[None] | None = None
        self._restart_task: asyncio.Task[None] | None = None
        # A failure has been reported and the unit has not been ready since.
        self._failing = False
        self.emit: Emit = discard
        self._proc: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    @property
    def unit(self) -> Unit:
        return self.cfg.units[self.name]

    @property
    def ready(self) -> bool:
        return self.state is State.READY

    # ------------------------------------------------------------------ config

    def render_conf(self, stanza: str | None = None) -> str:
        """The unit's rclone.conf stanza, authenticated by the pool's current key.

        The drive-side upload tuning lives here rather than on transfer jobs:
        a job must resolve to the mount's own fs instance (so one `backend set`
        moves both), and only the instance's config can carry backend options.
        The chunk size is the unit's own option, else an applied tuning
        profile's, else 128M: Drive charges a fixed round trip per chunk, and
        at the 8M default a stream idled about half the time. A bare number
        is KiB to rclone, hence the B. stop_on_upload_limit makes
        the daily cap a fatal error a job or the mount's stats report, instead
        of hours of silent retries."""
        u = self.unit
        opts: dict[str, str] = {"type": u.backend}
        if u.backend == "drive":
            opts["scope"] = "drive"
            opts["service_account_file"] = str(self.key_path)
            if u.drive_id:
                opts["team_drive"] = u.drive_id
            opts["chunk_size"] = f"{self.tuned_chunk}B" if self.tuned_chunk else "128M"
            opts["stop_on_upload_limit"] = "true"
        opts.update(u.options)
        return f"[{stanza or self.name}]\n" + "".join(f"{k} = {v}\n" for k, v in opts.items())

    def write_conf(self) -> None:
        path = self.layout.rclone_conf(self.name)
        tmp = path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(self.render_conf())
        os.replace(tmp, path)

    def argv(self) -> list[str]:
        L, n = self.layout, self.name
        common = [
            "--config", str(L.rclone_conf(n)),
            "--cache-dir", str(L.cache_dir(n)),
            "--log-level", self.cfg.rclone.log_level,
            "--log-file", str(L.log_file(n)),
            "--rc-addr", f"unix://{L.rc_socket(n)}",
            # Required for vfs/refresh; access control is the socket living
            # in the 0700 runtime dir, with no TCP listener at all.
            "--rc-no-auth",
        ]  # fmt: skip
        if not self.unit.mount:
            return [self.cfg.rclone.binary, "rcd", *common]

        uid, gid, cache_max, _ = resolved(self.cfg, n)
        read = {k: v["value"] for k, v in read_settings(self.cfg, n).items()}
        # Read flags come from defaults and the unit (see UnitDefaults for why
        # they are small).
        #
        # The dir cache is in memory only and --poll-interval keeps it
        # correct, so --dir-cache-time is long on purpose: expiry would just
        # make directories cold again after a warm-up has loaded them.
        #
        # --vfs-cache-max-size is per mount, so the caps don't bound total
        # disk use; --vfs-cache-min-free-space guards the shared cache disk.
        argv = [
            self.cfg.rclone.binary, "mount", f"{n}:", str(L.mount_path(n)),
            *common,
            "--rc",
            "--allow-non-empty",
            "--allow-other",
            "--buffer-size", read["buffer_size"],
            "--default-permissions",
            "--dir-cache-time", "1000h",
            "--poll-interval", "15s",
            "--umask", "002",
            "--vfs-cache-mode", read["vfs_cache_mode"],
            "--vfs-read-chunk-size", read["vfs_read_chunk_size"],
            "--vfs-read-ahead", read["vfs_read_ahead"],
            "--vfs-cache-max-age", "336h",
            "--vfs-cache-max-size", cache_max,
            "--vfs-cache-poll-interval", "5m",
            "--vfs-cache-min-free-space", self.cfg.defaults.cache_min_free_space,
        ]  # fmt: skip
        if uid is not None:
            argv += ["--uid", str(uid)]
        if gid is not None:
            argv += ["--gid", str(gid)]
        return argv + self.unit.mount_args

    # --------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._supervise(), name=f"unit:{self.name}")

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        if task is None or task.done():
            self._set(State.STOPPED)
            return
        was_ready = self.ready
        self._set(State.STOPPING)
        proc = self._proc
        if proc and proc.returncode is None:
            if self.unit.mount and was_ready:
                # rclone mount exits on its own once its mountpoint is unmounted.
                await mountutil.unmount(self.layout.mount_path(self.name))
            else:
                # Still starting (nothing to unmount yet) or an rcd, which
                # never exits by itself: ask it to stop now rather than wait.
                with contextlib.suppress(ProcessLookupError):
                    proc.terminate()
            await self._terminate(proc)
        try:
            await asyncio.wait_for(asyncio.shield(task), STOP_TIMEOUT)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._set(State.STOPPED)

    async def restart(self) -> None:
        await self.stop()
        self.start()

    def restart_soon(self) -> asyncio.Task[None]:
        """Restart in the background. A restart already under way is reused,
        so two triggers for one failure (the health loop and `ferryman
        health`, say) restart once."""
        if not self.restarting:
            self._restart_task = asyncio.create_task(self.restart(), name=f"restart:{self.name}")
        assert self._restart_task is not None
        return self._restart_task

    @property
    def restarting(self) -> bool:
        return self._restart_task is not None and not self._restart_task.done()

    def failed(self, reason: str) -> None:
        """Record a failure (and tell someone); cleared when the unit is next ready."""
        self.last_error = reason
        self._failing = True
        self.emit(Event("unit.failed", self.name, reason, "error"))

    def status(self) -> dict[str, Any]:
        return {
            "unit": self.name,
            "state": self.state.value,
            "since": self.since,
            "pid": self.pid,
            "key": self.key,
            "pool": self.unit.pool,
            "mount": str(self.layout.mount_path(self.name)) if self.unit.mount else None,
            "restarts": self.restarts,
            "last_error": self.last_error,
            "warm": self.warm,
            "read": read_settings(self.cfg, self.name) if self.unit.mount else None,
        }

    def _set(self, state: State) -> None:
        if state is not self.state:
            log.debug("unit %s: %s -> %s", self.name, self.state.value, state.value)
            self.state = state
            self.since = time.time()

    async def _supervise(self) -> None:
        backoff = BACKOFF_MIN
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self._run_once()
            except asyncio.CancelledError:
                await self._cleanup()
                raise
            except Exception as e:
                if self._stop.is_set():
                    self.last_error = str(e)
                else:
                    log.error("unit %s: %s", self.name, e)
                    self.failed(str(e))
            await self._cleanup()
            if self._stop.is_set():
                break
            if time.monotonic() - started > STABLE_AFTER:
                backoff = BACKOFF_MIN
            self._set(State.BACKOFF)
            self.restarts += 1
            log.info("unit %s: restarting in %.0fs", self.name, backoff)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
        self._set(State.STOPPED)

    async def _run_once(self) -> None:
        self._set(State.STARTING)
        L, n = self.layout, self.name
        mount_path = L.mount_path(n)

        L.runtime_dir(n).mkdir(parents=True, exist_ok=True, mode=0o700)
        L.cache_dir(n).mkdir(parents=True, exist_ok=True)
        L.log_file(n).parent.mkdir(parents=True, exist_ok=True)
        if self.unit.mount:
            await mountutil.clear_stale(mount_path, self.cfg.health.stat_timeout)
            uid, gid, _, _ = resolved(self.cfg, n)
            mountutil.make_dirs(mount_path, uid, gid, self.cfg.defaults.dir_mode)
        if self.pool is not None:
            lease = await self.pool.lease()
            self.key, self.key_path = lease.key, lease.path
        if self._stop.is_set():
            return
        self.write_conf()
        # A socket left by a crashed run would make the rc listener fail to bind.
        L.rc_socket(n).unlink(missing_ok=True)

        # The child must not inherit our systemd notify socket: rclone mount
        # sends READY=1 itself, which would report ferryman ready early.
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("NOTIFY_SOCKET", "WATCHDOG_USEC", "WATCHDOG_PID")
        }
        with L.log_file(n).open("ab") as out:
            self._proc = proc = await asyncio.create_subprocess_exec(
                *self.argv(),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=out,
                stderr=out,
                env=env,
                start_new_session=True,
            )
        self.pid = proc.pid
        log.info("unit %s: started rclone (pid %d)", n, proc.pid)

        deadline = time.monotonic() + READY_TIMEOUT
        while True:
            if self._stop.is_set():
                return  # stop() is terminating the process
            if proc.returncode is not None:
                raise RuntimeError(
                    f"rclone exited with {proc.returncode} during startup (see {L.log_file(n)})"
                )
            if (not self.unit.mount or mountutil.is_mounted(mount_path)) and await self.rc.alive():
                break
            if time.monotonic() > deadline:
                raise RuntimeError(f"not ready after {READY_TIMEOUT:.0f}s")
            await asyncio.sleep(0.5)

        self._set(State.READY)
        self.last_error = None
        log.info("unit %s: ready", n)
        if self._failing:
            self._failing = False
            self.emit(Event("unit.recovered", n, "ready again", "info", resolves=True))
        if self.unit.mount and resolved(self.cfg, n)[3]:
            self._warm_task = asyncio.create_task(self._warm(), name=f"warm:{n}")

        code = await proc.wait()
        if not self._stop.is_set():
            raise RuntimeError(f"rclone exited with {code}")

    async def _warm(self) -> None:
        """rclone's dir cache lives in memory, so every (re)start leaves it
        empty and the first walk of the tree (a library scan) pays a Drive
        round trip per directory. A recursive vfs/refresh fetches the whole
        drive in bulk pages instead. It runs as an rc job that is followed to
        the end: rclone forgets finished jobs after a minute, so this is the
        only place its outcome can be seen. One retry on failure."""
        for attempt in range(2):
            if attempt:
                await asyncio.sleep(self.warm_retry_delay)
            started = time.time()
            self.warm = {"state": "running", "started": started, "finished": None, "error": None}
            try:
                # vfs/refresh takes its flags as strings: a JSON true fails
                # the job with 'value must be string "recursive"=true'.
                job = await self.rc.call("vfs/refresh", recursive="true", _async=True)
                log.info("unit %s: dir cache refresh started (job %s)", self.name, job["jobid"])
                st = await self.rc.job_wait(job["jobid"], self.warm_poll_interval)
                error = st.get("error") or _refresh_errors(st.get("output"))
            except RcError as e:
                error = str(e)
            except asyncio.CancelledError:
                self.warm.update(state="cancelled", finished=time.time())
                raise
            if error and self._stop.is_set():
                self.warm.update(state="cancelled", finished=time.time())
                return  # the process is going away under it
            took = time.time() - started
            self.warm.update(finished=time.time(), state="failed" if error else "done", error=error)
            if not error:
                log.info("unit %s: dir cache refresh finished in %.0fs", self.name, took)
                self.emit(Event("warm.done", self.name, f"took {took:.0f}s", "info",
                                resolves=True))  # fmt: skip
                return
            if attempt:
                self.emit(Event("warm.failed", self.name, error))
            log.warning(
                "unit %s: dir cache refresh failed after %.0fs%s: %s",
                self.name, took, "" if attempt else ", retrying", error,
            )  # fmt: skip

    async def _terminate(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(proc.wait()), STOP_TIMEOUT)
            return
        except TimeoutError:
            pass
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(asyncio.shield(proc.wait()), STOP_TIMEOUT)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()

    async def _cleanup(self) -> None:
        task, self._warm_task = self._warm_task, None
        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        proc, self._proc = self._proc, None
        if proc and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            await self._terminate(proc)
        self.pid = None
        self.layout.rc_socket(self.name).unlink(missing_ok=True)
        if self.unit.mount:
            # Whatever is left at the mountpoint belonged to the process that
            # just died, so it is a dead endpoint: detach it now rather than
            # leave "Transport endpoint is not connected" for the next start.
            path = self.layout.mount_path(self.name)
            if mountutil.is_mounted(path):
                await mountutil.detach(path)


def _refresh_errors(output: Any) -> str | None:
    """vfs/refresh reports per-directory failures in its output while the
    job itself still succeeds: {"result": {"<dir>": "OK" | "<error>"}}."""
    result = output.get("result") if isinstance(output, dict) else None
    if not isinstance(result, dict):
        return None
    bad = [f"{d or '/'}: {r}" for d, r in result.items() if r != "OK"]
    return "; ".join(bad) or None
