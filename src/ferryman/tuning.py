"""Upload tuning: measure what this host's path to Drive sustains, and pick
the chunk size and transfer count from that instead of fixed numbers.

The method is the manual diagnosis it replaces. Drive takes uploads in
resumable chunks, one HTTPS request each, and a stream waits for each
chunk's acknowledgement before it sends the next. So one stream's rate grows
with the chunk size until that fixed wait is amortised (step A). More
streams then add up until something saturates: the NIC, the uplink beyond
it, Google, or the CPU (step B). The cheapest setting within CLOSE of the
best rate wins: less memory, and smaller retries.

Probes run in a throwaway `rclone rcd` on a unit's current key, never in a
unit's own process, so live mounts and transfers neither skew the numbers
nor run with probe settings. Each probe uploads random data into
.ferryman-tune/<hostname>/ on that unit's drive and is stopped after a fixed
window; the folder goes to the drive's trash at the end (a purge with
use_trash = false fails with a 404 for service accounts on shared drives).
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from croniter import croniter

from .config import Tuning, parse_size
from .notify import Event
from .pool import PoolError, is_quota_error
from .rc import RcClient, RcError
from .transfer import iec

if TYPE_CHECKING:
    from .agent import Agent
    from .unit import UnitSupervisor

log = logging.getLogger(__name__)

BACKEND = "drive"
GAIN = 0.10  # a step must add this much rate to count as progress
CLOSE = 0.05  # settings within this of the best rate are as good as it
NOISY = 0.20  # repeats of one probe further apart than this: a noisy tune
SOURCE_BYTES = 1 << 30
READY_TIMEOUT = 30.0
STOP_TIMEOUT = 30.0
PURGE_TIMEOUT = 120.0
# Without a profile (or with one not applied). Eight transfers: at Drive's
# per-request latency one stream idled about half the time on the 13 Oct
# workstation; eight filled its uplink.
DEFAULT_TRANSFERS = 8
DEFAULT_CHUNK = "128M"
# A transfer run says something about the link only if it moved this much,
# in files of at least DRIFT_FILE_CHUNKS chunks: small files are bound by
# API round trips, not bandwidth.
DRIFT_MIN_BYTES = 1 << 30
DRIFT_FILE_CHUNKS = 4
IDLE_POLL = 300.0  # how often a queued re-tune checks for an idle link
# A probe process past 2 x max_memory plus this aborts the tune: chunk
# buffers are the only thing that should grow with the settings.
RSS_SLACK = 512 << 20


class TuneError(Exception):
    pass


class TuneBusy(TuneError):
    pass


class _OutOfBudget(Exception):
    pass


@dataclass
class Probe:
    """One setting, measured `repeats` times; the figures are its best run's."""

    chunk: int
    transfers: int
    rate: float = 0.0  # bytes/s
    per_stream: float = 0.0  # bytes/s: rate / transfers
    runs: list[float] = field(default_factory=list)
    bytes: int = 0  # uploaded, all runs
    cpu: float | None = None  # cores the probe process used
    rss: int | None = None
    nic_tx: float | None = None  # whole-host TX bytes/s
    rate_limited: bool = False

    @property
    def memory(self) -> int:
        return self.chunk * self.transfers

    @property
    def noisy(self) -> bool:
        return len(self.runs) > 1 and min(self.runs) < max(self.runs) * (1 - NOISY)

    def row(self) -> dict[str, Any]:
        return {**asdict(self), "noisy": self.noisy}


Measure = Callable[[int, int], Awaitable[Probe]]


@dataclass
class Outcome:
    probes: list[Probe]
    chosen: Probe
    # Why step B ended: plateau, memory, rate-limited, budget, or max (it
    # still grew at the last transfer count).
    stopped: str


async def search(measure: Measure, chunks: list[int], steps: list[int], max_memory: int) -> Outcome:
    probes: list[Probe] = []
    stopped = "max"
    try:
        # Step A: one stream, growing chunks, until a step stops paying.
        knee: Probe | None = None
        for c in chunks:
            if c > max_memory:
                break
            p = await measure(c, 1)
            probes.append(p)
            if p.rate_limited:
                stopped = "rate-limited"
                break
            if knee is not None and p.rate < knee.rate * (1 + GAIN):
                break
            knee = p
        # Step B: that chunk, more streams, until the total stops growing.
        if knee is not None and stopped != "rate-limited":
            best = knee
            for t in steps[1:]:
                if knee.chunk * t > max_memory:
                    stopped = "memory"
                    break
                p = await measure(knee.chunk, t)
                probes.append(p)
                if p.rate_limited:
                    stopped = "rate-limited"
                    break
                if p.rate < best.rate * (1 + GAIN):
                    stopped = "plateau"
                    break
                best = p
    except _OutOfBudget:
        stopped = "budget"
    usable = [p for p in probes if not p.rate_limited and p.rate > 0]
    if not usable:
        raise TuneError("no probe measured a rate" if probes else "no chunk size fits max_memory")
    top = max(p.rate for p in usable)
    chosen = min((p for p in usable if p.rate >= top * (1 - CLOSE)),
                 key=lambda p: (p.memory, p.transfers))  # fmt: skip
    return Outcome(probes, chosen, stopped)


def classify(o: Outcome, link: float | None, cores: int) -> tuple[str, str]:
    """What limited the rate, in the terms of the manual diagnosis."""
    best = max(o.probes, key=lambda p: p.rate)
    if o.stopped == "rate-limited":
        return "api", "Google answered with rate limits as streams were added"
    if best.cpu is not None and best.cpu >= 0.9 * cores:
        return "cpu", f"the probe process used {best.cpu:.1f} of {cores} cores"
    if link and best.nic_tx and best.nic_tx >= 0.85 * link:
        return "nic", f"host TX at {iec(best.nic_tx)}/s of a {iec(link)}/s link"
    if o.stopped == "plateau":
        return "path", ("more streams stopped adding throughput below the NIC's speed: "
                        "the uplink beyond this host, or Google")  # fmt: skip
    if o.stopped == "memory":
        return "memory", "more streams would exceed tuning.max_memory"
    if o.stopped == "budget":
        return "budget", "the byte budget ran out before the rate stopped growing"
    return "transfers", "the rate still grew at the largest transfer count tried"


@dataclass
class Profile:
    backend: str
    host: str
    at: float
    unit: str
    key: str | None
    rclone: str
    chunk_size: int
    transfers: int
    rate: float
    limit: str
    detail: str
    stopped: str
    # Single-stream rate at the chosen chunk over that at the smallest:
    # what amortising Drive's per-chunk wait bought.
    chunk_gain: float
    noisy: bool
    bytes_used: int
    probes: list[dict[str, Any]]

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(self), indent=2) + "\n")
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: Path) -> Profile | None:
        try:
            return cls(**json.loads(path.read_text()))
        except FileNotFoundError:
            return None
        except (ValueError, TypeError) as e:
            log.warning("ignoring unreadable tuning profile %s: %s", path, e)
            return None


# ------------------------------------------------------------ host signals


def default_iface() -> str | None:
    """The interface of the default route with the lowest metric."""
    try:
        rows = Path("/proc/net/route").read_text().splitlines()[1:]
    except OSError:
        return None
    routes = [r.split() for r in rows]
    defaults = sorted((int(r[6]), r[0]) for r in routes if len(r) > 6 and r[1] == "00000000")
    return defaults[0][1] if defaults else None


def nic_tx_bytes(iface: str | None) -> int | None:
    if not iface:
        return None
    try:
        return int(Path(f"/sys/class/net/{iface}/statistics/tx_bytes").read_text())
    except (OSError, ValueError):
        return None


def link_speed(iface: str | None) -> float | None:
    """The NIC's link speed in bytes/s (None if unknown, as for wifi)."""
    if not iface:
        return None
    try:
        mbit = int(Path(f"/sys/class/net/{iface}/speed").read_text())
    except (OSError, ValueError):
        return None
    return mbit * 1e6 / 8 if mbit > 0 else None


def proc_cpu(pid: int) -> float | None:
    """CPU seconds (user + system) the process has used."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None


def proc_rss(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError):
        pass
    return None


# ------------------------------------------------------------------ tuner


@dataclass
class _Sample:
    at: float
    bytes: int
    cpu: float | None
    tx: int | None


@dataclass
class _Run:
    rate: float
    per_stream: float
    bytes: int
    cpu: float | None
    rss: int | None
    nic_tx: float | None
    rate_limited: bool
    quota: bool


class Tuner:
    def __init__(self, agent: Agent) -> None:
        self.agent = agent
        self.lock = asyncio.Lock()
        # The latest tune, and the profile in effect (an earlier one, when
        # the latest was only suggested).
        self.profile = Profile.load(agent.layout.tune_profile(BACKEND))
        self.active = Profile.load(agent.layout.tune_active(BACKEND))
        self.last: dict[str, Any] | None = None
        self.progress: str | None = None
        self.sample_interval = 1.0
        self.idle_poll = IDLE_POLL
        self.next_run: float | None = None
        # Qualifying transfer runs' rates over the profile's, newest last.
        self.recent: collections.deque[float] = collections.deque(maxlen=self.cfg.drift_runs)
        self.drifting = False
        self._retune: asyncio.Task[None] | None = None
        if self.active:
            for sup in self._tunable():
                sup.tuned_chunk = self.active.chunk_size

    @property
    def cfg(self) -> Tuning:
        return self.agent.cfg.tuning

    @property
    def running(self) -> bool:
        return self.lock.locked()

    def status(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "progress": self.progress,
            "profile": asdict(self.profile) if self.profile else None,
            "active": asdict(self.active) if self.active else None,
            "last": self.last,
            "next_run": self.next_run,
            "drifting": self.drifting,
            "recent": list(self.recent),
            "retune_queued": self._retune is not None and not self._retune.done(),
            "units": {s.name: self.unit_settings(s.name) for s in self.agent.units.values()
                      if s.unit.backend == BACKEND},
            "transfers": {n: self.transfer_settings(n) for n in self.agent.transfers},
        }  # fmt: skip

    # ------------------------------------------------------------- settings

    def _tunable(self) -> list[UnitSupervisor]:
        """Drive units without a chunk_size of their own."""
        return [s for s in self.agent.units.values()
                if s.unit.backend == BACKEND and "chunk_size" not in s.unit.options]  # fmt: skip

    def unit_settings(self, unit: str) -> dict[str, Any]:
        """The unit's upload chunk size and where it comes from."""
        u = self.agent.cfg.units[unit]
        if "chunk_size" in u.options:
            return {"chunk_size": u.options["chunk_size"], "source": "config"}
        if (p := self.active) and u.backend == BACKEND:
            return {"chunk_size": p.chunk_size, "source": "profile"}
        return {"chunk_size": parse_size(DEFAULT_CHUNK), "source": "default"}

    def transfer_settings(self, name: str) -> dict[str, Any]:
        """Transfers and checkers for a transfer's jobs: its own config, else
        an applied profile's transfer count (for drive units), else 8."""
        t = self.agent.cfg.transfers[name]
        p = self.active if self.agent.cfg.units[t.unit].backend == BACKEND else None
        fallback, source = (p.transfers, "profile") if p else (DEFAULT_TRANSFERS, "default")
        return {
            "transfers": t.transfers or fallback,
            "checkers": t.checkers or fallback,
            "source": "config" if t.transfers else source,
        }

    async def apply(self) -> list[dict[str, Any]]:
        """Put the latest profile into effect: transfers take its count from
        their next run; drive units without their own chunk_size switch now,
        in place via `backend set` (as rotation does), and keep it across
        restarts through their rendered conf."""
        p = self.profile
        if p is None:
            raise TuneError("no tuning profile to apply (run `ferryman tune`)")
        p.save(self.agent.layout.tune_active(BACKEND))
        self.active = p
        out: list[dict[str, Any]] = []
        for sup in self._tunable():
            sup.tuned_chunk = p.chunk_size
            if not sup.ready:
                out.append({"unit": sup.name, "chunk_size": p.chunk_size, "live": False})
                continue
            try:
                # `set` takes plain bytes: "16M" fails to parse.
                r = await sup.rc.call("backend/command", command="set", fs=f"{sup.name}:",
                                      opt={"chunk_size": str(p.chunk_size)})  # fmt: skip
                sup.write_conf()
                was = (r.get("result") or {}).get("chunk_size", {}).get("previous")
                out.append({"unit": sup.name, "chunk_size": p.chunk_size, "live": True,
                            "was": was})  # fmt: skip
            except RcError as e:
                log.error("unit %s: could not set chunk size live (%s); restarting", sup.name, e)
                self.agent.restart(sup)
                out.append({"unit": sup.name, "chunk_size": p.chunk_size, "live": False,
                            "error": str(e)})  # fmt: skip
        log.info("tune: applied %s", describe(p))
        return out

    def plan(self) -> dict[str, Any]:
        """The probe grid and the most a tune could upload."""
        cfg = self.cfg
        mem = parse_size(cfg.max_memory)
        chunks = [c for c in map(parse_size, cfg.chunk_sizes) if c <= mem]
        most = (len(chunks) + len(cfg.transfers) - 1) * cfg.repeats
        return {
            "chunk_sizes": chunks,
            "transfers": cfg.transfers,
            "repeats": cfg.repeats,
            "max_probe_runs": most,
            "max_bytes": min(parse_size(cfg.budget), most * parse_size(cfg.probe_bytes)),
            "max_seconds": most * (cfg.warmup + cfg.window),
        }

    def candidate(self, unit: str | None) -> UnitSupervisor:
        """The ready drive unit whose drive and current key the probes use."""
        sups = [s for s in self.agent.units.values() if s.unit.backend == BACKEND]
        if unit is not None:
            sup = self.agent.units[unit]
            if sup.unit.backend != BACKEND:
                raise TuneError(f"unit {unit} is not a {BACKEND} unit")
            sups = [sup]
        if not sups:
            raise TuneError(f"no {BACKEND} units to tune for")
        for sup in sups:
            if sup.ready and sup.key_path is not None:
                return sup
        raise TuneError(f"no ready {BACKEND} unit to borrow a drive and key from")

    def busy(self) -> str | None:
        """Why a tune would measure a shared link right now, if it would."""
        for tr in self.agent.transfers.values():
            if tr.running and self.agent.cfg.units[tr.t.unit].backend == BACKEND:
                return f"transfer {tr.name} is running"
        return None

    def check(self, unit: str | None = None) -> UnitSupervisor:
        """The unit a tune would probe through, or why it cannot run now."""
        if self.lock.locked():
            raise TuneBusy("a tune is already running")
        sup = self.candidate(unit)
        if why := self.busy():
            raise TuneBusy(f"{why}; a tune now would share the link with it")
        return sup

    async def run(
        self, unit: str | None = None, dry_run: bool = False, apply: bool | None = None
    ) -> dict[str, Any]:
        """Tune, store the profile and, if `apply` (default: tuning.apply is
        auto and the tune was not noisy), put it into effect."""
        sup = self.check(unit)
        plan = {"unit": sup.name, "key": sup.key, **self.plan()}
        if dry_run:
            return {"ok": True, "dry_run": True, "plan": plan}
        emit = self.agent.notifier.emit
        async with self.lock:
            started = time.time()
            try:
                await self._lock_uplink(plan["max_seconds"])
            except TuneError as e:
                self.last = {"ok": False, "error": str(e), "started": started,
                             "finished": time.time()}  # fmt: skip
                log.warning("tune not started: %s", e)
                if isinstance(e, TuneBusy):
                    raise  # a queued re-tune keeps waiting
                return self.last
            log.info("tune: probing via unit %s on %s", sup.name, sup.key)
            try:
                profile = await self._tune(sup)
            except asyncio.CancelledError:
                self.last = {"ok": False, "error": "aborted", "started": started,
                             "finished": time.time()}  # fmt: skip
                raise
            except (TuneError, RcError, OSError) as e:
                self.last = {"ok": False, "error": str(e), "started": started,
                             "finished": time.time()}  # fmt: skip
                log.error("tune failed: %s", e)
                emit(Event("tune.failed", BACKEND, str(e), "error"))
                return self.last
            finally:
                self.progress = None
                await self._unlock_uplink()
            profile.save(self.agent.layout.tune_profile(BACKEND))
            self.profile = profile
            self.recent.clear()
            self.last = {"ok": True, "started": started, "finished": time.time()}
            summary = describe(profile)
            log.info("tune: %s", summary)
            emit(Event("tune.done", BACKEND, summary, "info", resolves=True))
        if apply is None:
            apply = self.cfg.apply == "auto" and not profile.noisy
            if self.cfg.apply == "auto" and profile.noisy:
                log.warning("tune: not applying a noisy profile; re-run, or apply by hand")
        applied = await self.apply() if apply else []
        return {"ok": True, "profile": asdict(profile), "applied": applied}

    # ------------------------------------------------- drift and schedule

    def observe(self, transfer: str, result: dict[str, Any]) -> None:
        """Compare a finished transfer run's rate with the profile in effect
        (else the latest); enough slow runs in a row mean the link changed."""
        ref = self.active or self.profile
        t = self.agent.cfg.transfers[transfer]
        if ref is None or self.agent.cfg.units[t.unit].backend != BACKEND:
            return
        size, files = int(result.get("bytes", 0)), int(result.get("files", 0))
        secs = float(result.get("transfer_time") or 0)
        chunk = parse_size(self.unit_settings(t.unit)["chunk_size"])
        if size < DRIFT_MIN_BYTES or not files or secs <= 0:
            return
        if size / files < DRIFT_FILE_CHUNKS * chunk:
            return
        ratio = size / secs / ref.rate
        self.recent.append(ratio)
        emit = self.agent.notifier.emit
        if ratio >= self.cfg.drift:
            if self.drifting:
                self.drifting = False
                emit(Event("tune.recovered", BACKEND, f"transfer {transfer} ran at "
                           f"{iec(size / secs)}/s, {ratio:.0%} of the profile", "info",
                           resolves=True))  # fmt: skip
            return
        if self.drifting or len(self.recent) < self.cfg.drift_runs:
            return
        if all(r < self.cfg.drift for r in self.recent):
            self.drifting = True
            msg = (f"the last {len(self.recent)} transfer runs reached "
                   f"{', '.join(f'{r:.0%}' for r in self.recent)} of the profile's "
                   f"{iec(ref.rate)}/s")  # fmt: skip
            log.warning("tune: drift: %s", msg)
            emit(Event("tune.drift", BACKEND, msg, "warning"))
            if self.cfg.apply == "auto":
                self.queue_retune()

    def queue_retune(self) -> None:
        """Re-tune once nothing else is using the link."""
        if self._retune is None or self._retune.done():
            self._retune = self.agent.spawn(self._retune_when_idle(), "retune")

    async def _retune_when_idle(self) -> None:
        while True:
            await asyncio.sleep(self.idle_poll)
            try:
                await self.run()
                return
            except TuneBusy as e:
                log.debug("re-tune waiting: %s", e)
            except TuneError as e:
                log.warning("re-tune dropped: %s", e)
                return

    async def schedule(self) -> None:
        if not self.cfg.schedule:
            return
        it = croniter(self.cfg.schedule, datetime.now().astimezone())
        while True:
            self.next_run = it.get_next(float)
            await asyncio.sleep(max(0.0, self.next_run - time.time()))
            try:
                await self.run()
            except TuneError as e:
                log.info("scheduled tune skipped: %s", e)

    # ---------------------------------------------------------- uplink lock

    @property
    def _uplink_path(self) -> str | None:
        if self.cfg.uplink and self.agent.controller is not None:
            return f"/v1/tune-lock/{self.cfg.uplink}"
        return None

    async def _lock_uplink(self, seconds: float) -> None:
        """Hold the controller's lock on this host's uplink for the tune."""
        if (path := self._uplink_path) is None:
            return
        assert self.agent.controller is not None
        try:
            r = await self.agent.controller.request("POST", path, json={"ttl": seconds + 300})
        except PoolError as e:
            raise TuneError(f"cannot take the uplink {self.cfg.uplink} lock: {e}") from e
        if not r["granted"]:
            raise TuneBusy(f"{r['holder']} is tuning on uplink {self.cfg.uplink}")

    async def _unlock_uplink(self) -> None:
        if (path := self._uplink_path) is None:
            return
        assert self.agent.controller is not None
        try:
            await self.agent.controller.request("DELETE", path)
        except PoolError as e:
            log.warning("tune: could not release the uplink lock (it expires): %s", e)

    # ------------------------------------------------------------ internals

    @property
    def _scratch(self) -> str:
        return f".ferryman-tune/{self.agent.layout.hostname}"

    async def _tune(self, sup: UnitSupervisor) -> Profile:
        L, cfg = self.agent.layout, self.cfg
        L.tune_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        L.tune_log.parent.mkdir(parents=True, exist_ok=True)
        conf, sock = L.tune_dir / "rclone.conf", L.tune_dir / "rc.sock"
        fd = os.open(conf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(sup.render_conf("tune"))
        sock.unlink(missing_ok=True)
        await asyncio.to_thread(self._prepare_source)

        env = {k: v for k, v in os.environ.items()
               if k not in ("NOTIFY_SOCKET", "WATCHDOG_USEC", "WATCHDOG_PID")}  # fmt: skip
        with L.tune_log.open("ab") as out:
            proc = await asyncio.create_subprocess_exec(
                self.agent.cfg.rclone.binary, "rcd",
                "--config", str(conf),
                "--rc-addr", f"unix://{sock}",
                "--rc-no-auth",
                "--log-level", self.agent.cfg.rclone.log_level,
                "--log-file", str(L.tune_log),
                stdin=asyncio.subprocess.DEVNULL, stdout=out, stderr=out,
                env=env, start_new_session=True,
            )  # fmt: skip
        rc = RcClient(sock)
        used = [0]
        try:
            await self._wait_ready(rc, proc)
            version = str((await rc.call("core/version")).get("version", "?"))
            chunks = [parse_size(c) for c in cfg.chunk_sizes]
            outcome = await search(self._measurer(rc, proc.pid, sup, used), chunks,
                                   cfg.transfers, parse_size(cfg.max_memory))  # fmt: skip
        finally:
            try:
                await rc.call("operations/purge", fs="tune:", remote=self._scratch,
                              timeout=PURGE_TIMEOUT)  # fmt: skip
            except RcError as e:
                # Nothing to purge when no probe got as far as a folder.
                log.debug("tune: purge %s: %s", self._scratch, e)
            if proc.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), STOP_TIMEOUT)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
            sock.unlink(missing_ok=True)
            conf.unlink(missing_ok=True)

        limit, detail = classify(outcome, link_speed(default_iface()), os.cpu_count() or 1)
        c = outcome.chosen
        single = [p for p in outcome.probes if p.transfers == 1 and p.rate > 0]
        gain = (max((p.rate for p in single if p.chunk == c.chunk), default=0.0)
                / single[0].rate) if single else 0.0  # fmt: skip
        return Profile(
            backend=BACKEND, host=L.hostname, at=time.time(), unit=sup.name, key=sup.key,
            rclone=version, chunk_size=c.chunk, transfers=c.transfers, rate=c.rate,
            limit=limit, detail=detail, stopped=outcome.stopped, chunk_gain=round(gain, 2),
            noisy=any(p.noisy for p in outcome.probes), bytes_used=used[0],
            probes=[p.row() for p in outcome.probes],
        )  # fmt: skip

    def _prepare_source(self) -> None:
        """One random file, hardlinked four times per stream into a folder per
        transfer count: enough files that no stream runs dry in a window."""
        src = self.agent.layout.tune_source
        src.mkdir(parents=True, exist_ok=True, mode=0o700)
        data = src / "random.bin"
        if not data.exists() or data.stat().st_size != SOURCE_BYTES:
            tmp = data.with_suffix(".tmp")
            with tmp.open("wb") as f:
                for _ in range(SOURCE_BYTES >> 20):
                    f.write(os.urandom(1 << 20))
            os.replace(tmp, data)
        for t in self.cfg.transfers:
            d = src / f"x{t}"
            d.mkdir(exist_ok=True)
            for i in range(4 * t):
                link = d / f"f{i:02d}.bin"
                if not link.exists():
                    os.link(data, link)

    async def _wait_ready(self, rc: RcClient, proc: asyncio.subprocess.Process) -> None:
        deadline = time.monotonic() + READY_TIMEOUT
        while not await rc.alive():
            if proc.returncode is not None:
                raise TuneError(f"probe rclone exited with {proc.returncode} "
                                f"(see {self.agent.layout.tune_log})")  # fmt: skip
            if time.monotonic() > deadline:
                raise TuneError(f"probe rclone not ready after {READY_TIMEOUT:.0f}s")
            await asyncio.sleep(0.2)

    def _measurer(self, rc: RcClient, pid: int, sup: UnitSupervisor, used: list[int]) -> Measure:
        cfg = self.cfg
        budget, cap = parse_size(cfg.budget), parse_size(cfg.probe_bytes)
        count = 0

        async def measure(chunk: int, transfers: int) -> Probe:
            nonlocal count
            p = Probe(chunk, transfers)
            for _ in range(cfg.repeats):
                if budget - used[0] < cap:
                    if p.runs:
                        break
                    raise _OutOfBudget
                count += 1
                self.progress = f"probe {count}: {iec(chunk)} chunks x {transfers}"
                run = await self._probe_once(rc, pid, chunk, transfers,
                                             f"{self._scratch}/{count:02d}", cap)  # fmt: skip
                used[0] += run.bytes
                p.bytes += run.bytes
                p.runs.append(run.rate)
                if run.quota:
                    raise TuneError(f"upload cap hit on {sup.key} during the tune")
                p.rate_limited |= run.rate_limited
                if run.rate >= p.rate:
                    p.rate, p.per_stream = run.rate, run.per_stream
                    p.cpu, p.rss, p.nic_tx = run.cpu, run.rss, run.nic_tx
            log.info("tune: %s chunks x %d: %s/s (%s/s per stream)",
                     iec(chunk), transfers, iec(p.rate), iec(p.per_stream))  # fmt: skip
            return p

        return measure

    async def _probe_once(
        self, rc: RcClient, pid: int, chunk: int, transfers: int, dest: str, cap: int
    ) -> _Run:
        cfg = self.cfg
        group = f"tune-{dest.rsplit('/', 1)[-1]}"
        src = self.agent.layout.tune_source / f"x{transfers}"
        reply = await rc.call(
            "sync/copy",
            srcFs=str(src),
            # A connection-string override: each chunk size is its own fs
            # instance, so no probe inherits another's. The B matters: a bare
            # number is read as KiB, which turns 8M into 8G chunks.
            dstFs=f"tune,chunk_size={chunk}B:{dest}",
            _async=True,
            _group=group,
            _config={"Transfers": transfers, "Checkers": transfers},
        )
        jobid = int(reply["jobid"])
        rss_limit = 2 * parse_size(cfg.max_memory) + RSS_SLACK
        iface = default_iface()
        start = time.monotonic()
        first = _Sample(start, 0, proc_cpu(pid), nic_tx_bytes(iface))
        warmed = False
        st: dict[str, Any] = {}
        try:
            while True:
                await asyncio.sleep(self.sample_interval)
                st = await rc.stats(group)
                now, done = time.monotonic(), int(st.get("bytes", 0))
                if (rss := proc_rss(pid)) is not None and rss > rss_limit:
                    raise TuneError(f"probe rclone grew to {iec(rss)}, over the "
                                    f"{iec(rss_limit)} limit; aborted")  # fmt: skip
                if is_quota_error(st.get("lastError")):
                    break
                if not warmed and now - start >= cfg.warmup:
                    first, warmed = _Sample(now, done, proc_cpu(pid), nic_tx_bytes(iface)), True
                elif warmed and now - first.at >= cfg.window:
                    break
                if done >= cap or (await rc.job_status(jobid)).get("finished"):
                    break
        finally:
            await self._stop_job(rc, jobid)
        last = _Sample(time.monotonic(), int(st.get("bytes", 0)), proc_cpu(pid),
                       nic_tx_bytes(iface))  # fmt: skip
        span = max(last.at - first.at, 1e-9)
        err = str(st.get("lastError") or "")
        quota = is_quota_error(err)
        rate = (last.bytes - first.bytes) / span
        return _Run(
            rate=rate,
            per_stream=rate / transfers,
            bytes=last.bytes,
            cpu=(last.cpu - first.cpu) / span
            if last.cpu is not None and first.cpu is not None else None,
            rss=proc_rss(pid),
            nic_tx=(last.tx - first.tx) / span
            if last.tx is not None and first.tx is not None else None,
            rate_limited=not quota and ("rateLimitExceeded" in err or "Error 429" in err),
            quota=quota,
        )  # fmt: skip

    async def _stop_job(self, rc: RcClient, jobid: int) -> None:
        with contextlib.suppress(RcError):
            await rc.job_stop(jobid)
        deadline = time.monotonic() + STOP_TIMEOUT
        while time.monotonic() < deadline:
            try:
                if (await rc.job_status(jobid)).get("finished"):
                    return
            except RcError:
                return
            await asyncio.sleep(0.2)


def describe(p: Profile) -> str:
    return (f"{iec(p.chunk_size)} chunks x {p.transfers} transfers, {iec(p.rate)}/s; "
            f"limit: {p.limit}{' (noisy)' if p.noisy else ''}")  # fmt: skip
