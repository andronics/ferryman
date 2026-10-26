"""Prometheus metrics, rendered in the text exposition format at scrape time
from the agent's (or controller's) live state. No client library: every
value already lives somewhere, so a scrape just reads it."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from . import __version__
from .controller_pool import ControllerPool
from .pool import LocalPool
from .rc import RcError
from .tuning import BACKEND
from .unit import State

if TYPE_CHECKING:
    from .agent import Agent
    from .controller import Controller

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
RC_TIMEOUT = 5.0

# name -> (type, help)
METRICS: dict[str, tuple[str, str]] = {
    "ferryman_info": ("gauge", "Agent version and hostname (always 1)."),
    "ferryman_unit_ready": ("gauge", "1 if the unit's rclone process is ready."),
    "ferryman_unit_state": ("gauge", "The unit's supervisor state (1 for the current one)."),
    "ferryman_unit_state_since_timestamp_seconds": ("gauge", "When the unit entered its state."),
    "ferryman_unit_restarts_total": ("counter", "Restarts of the unit's process."),
    "ferryman_unit_mount_up": ("gauge", "1 if the mount path is mounted (kind rclone/rc/bind)."),
    "ferryman_unit_serve_up": ("gauge", "1 if the server runs (labels type, addr)."),
    "ferryman_unit_warm_success": ("gauge", "1 if the last dir-cache warm-up worked."),
    "ferryman_unit_warm_duration_seconds": ("gauge", "How long the last warm-up took."),
    "ferryman_rclone_speed_bytes": ("gauge", "Current transfer rate of the process, bytes/s."),
    "ferryman_rclone_transferring": ("gauge", "Files in flight in the process."),
    "ferryman_rclone_bytes": ("gauge", "Bytes moved by the process since start or stats reset."),
    "ferryman_rclone_errors": ("gauge", "Errors in the process since start or stats reset."),
    "ferryman_tune_running": ("gauge", "1 while a tune runs."),
    "ferryman_tune_rate_bytes": ("gauge", "Rate the profile's setting reached, bytes/s."),
    "ferryman_tune_chunk_size_bytes": ("gauge", "The profile's upload chunk size."),
    "ferryman_tune_transfers": ("gauge", "The profile's transfer count."),
    "ferryman_tune_noisy": ("gauge", "1 if the profile's repeated probes disagreed."),
    "ferryman_tune_limit": ("gauge", "What limited the profile's rate (1 for it)."),
    "ferryman_tune_timestamp_seconds": ("gauge", "When the profile was measured."),
    "ferryman_transfer_running": ("gauge", "1 while the transfer runs."),
    "ferryman_transfer_last_success": ("gauge", "1 if the transfer's last run succeeded."),
    "ferryman_transfer_last_run_timestamp_seconds": ("gauge", "When the last run finished."),
    "ferryman_transfer_last_success_timestamp_seconds": (
        "gauge",
        "When a run last succeeded (since the agent started).",
    ),
    "ferryman_transfer_next_run_timestamp_seconds": ("gauge", "When the next scheduled run is."),
    "ferryman_transfer_runs_total": ("counter", "Runs by result (ok, skipped, failed)."),
    "ferryman_transfer_bytes_total": ("counter", "Bytes moved by successful runs."),
    "ferryman_transfer_files_total": ("counter", "Files moved by successful runs."),
    "ferryman_pool_keys": ("gauge", "Keys in the pool."),
    "ferryman_pool_keys_available": ("gauge", "Keys usable now: not exhausted, key file good."),
    "ferryman_pool_keys_bad": ("gauge", "Keys whose file is missing or invalid."),
    "ferryman_pool_epoch": ("gauge", "Rotations of the pool so far."),
    "ferryman_pool_rotated_timestamp_seconds": ("gauge", "When the pool last rotated."),
    "ferryman_pool_current_key": ("gauge", "The key in use (always 1)."),
    "ferryman_pool_frees_timestamp_seconds": (
        "gauge",
        "When the first key of an all-exhausted pool frees.",
    ),
    "ferryman_pool_key_probe_timestamp_seconds": (
        "gauge",
        "When an exhausted key was last probed, by result (ok, capped, error).",
    ),
    "ferryman_controller_reachable": ("gauge", "1 if the controller answered the last request."),
    "ferryman_controller_info": ("gauge", "Controller version (always 1)."),
    "ferryman_agent_last_report_timestamp_seconds": ("gauge", "When the agent last reported."),
    "ferryman_agent_units": ("gauge", "Units the agent runs."),
    "ferryman_agent_units_ready": ("gauge", "Of the agent's units, those ready."),
}


def _escape(v: str) -> str:
    return v.replace("\\", r"\\").replace('"', r"\"").replace("\n", r"\n")


class Exposition:
    def __init__(self) -> None:
        self._samples: dict[str, list[str]] = {}

    def add(self, name: str, value: float | None, **labels: str) -> None:
        if value is None:
            return
        lbl = ",".join(f'{k}="{_escape(str(v))}"' for k, v in labels.items())
        sample = f"{name}{{{lbl}}}" if lbl else name
        self._samples.setdefault(name, []).append(f"{sample} {float(value)!r}")

    def render(self) -> str:
        out = []
        for name, samples in self._samples.items():
            kind, help = METRICS[name]
            out += [f"# HELP {name} {help}", f"# TYPE {name} {kind}", *samples]
        return "\n".join(out) + "\n"


def _pool(m: Exposition, info: dict[str, Any]) -> None:
    p, keys = info["pool"], info["keys"]
    m.add("ferryman_pool_keys", len(keys), pool=p)
    usable = sum(not k["exhausted_until"] and k["file_ok"] for k in keys)
    m.add("ferryman_pool_keys_available", usable, pool=p)
    m.add("ferryman_pool_keys_bad", sum(not k["file_ok"] for k in keys), pool=p)
    m.add("ferryman_pool_epoch", info["epoch"], pool=p)
    m.add("ferryman_pool_rotated_timestamp_seconds", info.get("rotated_at"), pool=p)
    for k in keys:
        if k["current"]:
            m.add("ferryman_pool_current_key", 1, pool=p, key=k["key"])
    for k in keys:
        if pr := k.get("probe"):
            m.add("ferryman_pool_key_probe_timestamp_seconds", pr["at"], pool=p, key=k["key"],
                  result=pr["result"])  # fmt: skip
    frees = [k["exhausted_until"] for k in keys if k["exhausted_until"]]
    if frees and len(frees) == len(keys):
        m.add("ferryman_pool_frees_timestamp_seconds", min(frees), pool=p)


async def agent_metrics(agent: Agent) -> str:
    m = Exposition()
    m.add("ferryman_info", 1, version=__version__, hostname=agent.layout.hostname)

    async def rc_stats(name: str) -> tuple[str, dict[str, Any] | None]:
        try:
            return name, await asyncio.wait_for(agent.units[name].rc.stats(), RC_TIMEOUT)
        except (RcError, TimeoutError):
            return name, None

    ready = [n for n, s in agent.units.items() if s.ready]
    stats = dict(await asyncio.gather(*(rc_stats(n) for n in ready)))

    for name, sup in agent.units.items():
        m.add("ferryman_unit_ready", int(sup.ready), unit=name)
        for state in State:
            m.add("ferryman_unit_state", int(sup.state is state), unit=name, state=state.value)
        m.add("ferryman_unit_state_since_timestamp_seconds", sup.since, unit=name)
        m.add("ferryman_unit_restarts_total", sup.restarts, unit=name)
        for mt in sup.status()["mounts"]:
            up = sup.ready and mt["state"] == "mounted"
            m.add("ferryman_unit_mount_up", int(up), unit=name, path=mt["path"], kind=mt["kind"])
        for sv in sup.status()["serves"]:
            up = sup.ready and sv["state"] == "running"
            m.add("ferryman_unit_serve_up", int(up), unit=name, type=sv["type"], addr=sv["addr"])
        w = sup.warm
        if w and w["state"] in ("done", "failed"):
            m.add("ferryman_unit_warm_success", int(w["state"] == "done"), unit=name)
            m.add("ferryman_unit_warm_duration_seconds", w["finished"] - w["started"], unit=name)
        st = stats.get(name)
        if st is None:
            continue
        flying = st.get("transferring") or []
        m.add("ferryman_rclone_speed_bytes", sum(t.get("speedAvg", 0) for t in flying), unit=name)
        m.add("ferryman_rclone_transferring", len(flying), unit=name)
        m.add("ferryman_rclone_bytes", st.get("bytes", 0), unit=name)
        m.add("ferryman_rclone_errors", st.get("errors", 0), unit=name)

    for name, tr in agent.transfers.items():
        m.add("ferryman_transfer_running", int(tr.running), transfer=name)
        if tr.last and not tr.last.get("dry_run"):
            m.add("ferryman_transfer_last_success", int(bool(tr.last["ok"])), transfer=name)
            m.add("ferryman_transfer_last_run_timestamp_seconds", tr.last["finished"],
                  transfer=name)  # fmt: skip
        m.add("ferryman_transfer_last_success_timestamp_seconds", tr.last_ok_at, transfer=name)
        m.add("ferryman_transfer_next_run_timestamp_seconds", tr.next_run, transfer=name)
        for result, n in tr.runs.items():
            m.add("ferryman_transfer_runs_total", n, transfer=name, result=result)
        m.add("ferryman_transfer_bytes_total", tr.bytes_total, transfer=name)
        m.add("ferryman_transfer_files_total", tr.files_total, transfer=name)

    tuner = agent.tuner
    m.add("ferryman_tune_running", int(tuner.running), backend=BACKEND)
    # profile=latest: the last tune; profile=active: the one in effect.
    for which, p in (("latest", tuner.profile), ("active", tuner.active)):
        if p is None:
            continue
        labels = {"backend": p.backend, "profile": which}
        m.add("ferryman_tune_rate_bytes", p.rate, **labels)
        m.add("ferryman_tune_chunk_size_bytes", p.chunk_size, **labels)
        m.add("ferryman_tune_transfers", p.transfers, **labels)
        m.add("ferryman_tune_noisy", int(p.noisy), **labels)
        m.add("ferryman_tune_limit", 1, limit=p.limit, **labels)
        m.add("ferryman_tune_timestamp_seconds", p.at, **labels)

    for name, pool in agent.pools.items():
        if isinstance(pool, LocalPool):
            _pool(m, pool.describe())
        elif isinstance(pool, ControllerPool):
            # Only what the agent knows locally: a scrape never waits on the
            # controller, which has its own /metrics.
            m.add("ferryman_pool_epoch", pool.epoch if pool.epoch >= 0 else None, pool=name)
            if pool.current:
                m.add("ferryman_pool_current_key", 1, pool=name, key=pool.current)
            m.add("ferryman_controller_reachable", int(pool.reachable), pool=name)
    return m.render()


def controller_metrics(co: Controller) -> str:
    m = Exposition()
    m.add("ferryman_controller_info", 1, version=__version__)
    for pool in co.pools.values():
        _pool(m, pool.describe())
    for name in co.cfg.agents:
        rep = co.reports.get(name)
        m.add(
            "ferryman_agent_last_report_timestamp_seconds", rep["at"] if rep else None, agent=name
        )
        if not rep:
            continue
        units = rep["report"].get("status", {}).get("units", [])
        m.add("ferryman_agent_units", len(units), agent=name)
        m.add("ferryman_agent_units_ready", sum(u["state"] == "ready" for u in units), agent=name)
    return m.render()
