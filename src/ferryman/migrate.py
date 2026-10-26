"""`ferryman migrate`: 0.2 configs to 0.3.

In 0.2 each host's agent config held its own pools, units and transfers,
and an optional coordinator shared key pools. In 0.3 the controller holds
all of it and each agent keeps only a bootstrap file. This reads every
host's 0.2 agent config (and the coordinator's, if there was one) and
writes the controller's config and each host's agent.yaml:

- each host's units become a unit (what: drive, pool, remote options, read
  policy) and that host's binding (how: mounts, servers, uid/gid, cache);
  two hosts defining one unit name differently is refused, with the
  differences listed
- a read setting the hosts agree on goes on the unit; one they don't
  becomes a per-host override
- local pools become the controller's (their key files must be copied to
  the controller: the notes say which), coordinator pools carry over
- coordinator tokens are kept; a host that had none gets a new one
- mail and webhook settings become channels, and their level one default
  rule that behaves as 0.2 did; tuning and health come from the first host,
  with the other hosts' differences kept as per-agent tuning or noted

A 0.1 config (`roots`, a unit's `mountpoint`) is read too: its paths are
resolved the way 0.1 resolved them first.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from .config import HOST_FIELDS, POLICY_FIELDS, ControllerConfig, Tuning, UnitDefaults

WHAT = ("backend", "pool", "drive_id", "options")
HOW = ("mounts", "serve", "uid", "gid", "cache_max_size", "mount_args")
BUILT_IN = UnitDefaults().model_dump(mode="json")


class MigrateError(ValueError):
    pass


@dataclass
class Result:
    controller: dict[str, Any]
    agents: dict[str, dict[str, Any]]  # host -> its agent.yaml
    tokens: dict[str, str]  # host -> a new token (hosts that had none)
    notes: list[str] = field(default_factory=list)


def from_01(host: str, cfg: dict[str, Any], notes: list[str]) -> dict[str, Any]:
    """A 0.1 config as 0.2 would say it: `roots` and each unit's
    `mountpoint` become absolute mount paths (none with `mount: false`), and
    a transfer's source, missing or relative, its absolute path."""
    roots = cfg.get("roots")
    if roots is None:
        return cfg
    local, remote = PurePosixPath(roots["local"]), PurePosixPath(roots["remote"])
    cfg = {k: v for k, v in cfg.items() if k != "roots"}
    units = {}
    for name, u in (cfg.get("units") or {}).items():
        u = dict(u)
        mountpoint = u.pop("mountpoint")
        if u.pop("mount", True):
            u["mounts"] = [{"path": str(remote / mountpoint)}]
        u["_local"] = str(local / mountpoint)
        units[name] = u
    transfers = {}
    for name, t in (cfg.get("transfers") or {}).items():
        t = dict(t)
        src = t.get("source")
        if src is None:
            t["source"] = units[t["unit"]]["_local"]
        elif not str(src).startswith("/"):
            t["source"] = str(local / str(src))
        transfers[name] = t
    for u in units.values():
        del u["_local"]
    notes.append(f"{host}: a 0.1 config: mount paths and transfer sources resolved from its "
                 f"roots. 0.1 created a mountpoint's missing parents under {remote}; from 0.2 "
                 "only the mount directory is created, so its parent must exist")  # fmt: skip
    return {**cfg, "units": units, "transfers": transfers}


def migrate(hosts: dict[str, dict[str, Any]], coordinator: dict[str, Any] | None = None,
            controller_url: str | None = None) -> Result:  # fmt: skip
    if not hosts:
        raise MigrateError("no hosts given")
    co = coordinator or {}
    errors: list[str] = []
    notes: list[str] = []
    hosts = {h: from_01(h, cfg, notes) for h, cfg in hosts.items()}
    out: dict[str, Any] = {}
    for k in ("listen", "ui", "rclone", "watch_timeout"):
        if k in co:
            out[k] = co[k]
    state = co.get("state")
    if state in (None, "/var/lib/ferryman-coordinator"):
        out["state"] = "/var/lib/ferryman-controller"
        notes.append("controller state: mv /var/lib/ferryman-coordinator "
                     "/var/lib/ferryman-controller (pool rotation state and probes)")  # fmt: skip
    else:
        out["state"] = state

    # ------------------------------------------------------------- pools
    pools: dict[str, Any] = dict(co.get("pools") or {})
    for host, cfg in hosts.items():
        for name, p in (cfg.get("pools") or {}).items():
            if p.get("coordinator") or p.get("controller"):
                if name not in pools:
                    errors.append(f"{host}: pool {name} is the coordinator's, but its "
                                  "config (--coordinator) doesn't define it")  # fmt: skip
                continue
            mine = {k: p[k] for k in ("keys_dir", "keys", "quota_window") if k in p}
            if name not in pools:
                pools[name] = mine
                notes.append(f"pool {name}: copy its key files from {host}:{p.get('keys_dir')} "
                             f"to the controller's {p.get('keys_dir')}")  # fmt: skip
            elif pools[name].get("keys") != mine.get("keys"):
                errors.append(f"pool {name}: {host}'s keys differ from another host's "
                              "(rename one of the pools)")  # fmt: skip
    out["pools"] = pools

    # ------------------------------------------- units: what, and each how
    units: dict[str, dict[str, Any]] = {}
    owner: dict[str, str] = {}
    agents: dict[str, dict[str, Any]] = {}
    policy: dict[str, dict[str, dict[str, Any]]] = {}  # unit -> host -> effective policy
    for host, cfg in hosts.items():
        host_defaults = cfg.get("defaults") or {}
        bindings: dict[str, Any] = {}
        for name, u in (cfg.get("units") or {}).items():
            what = {k: u[k] for k in WHAT if u.get(k) not in (None, {})}
            if what.get("backend") == "drive":
                del what["backend"]  # the default
            if name in units and units[name] != what:
                diff = sorted(k for k in WHAT if units[name].get(k) != what.get(k))
                errors.append(f"unit {name}: {owner[name]} and {host} define it differently "
                              f"({', '.join(diff)}): rename one")  # fmt: skip
                continue
            units.setdefault(name, what)
            owner.setdefault(name, host)
            bindings[name] = {k: u[k] for k in HOW if u.get(k) not in (None, [])}
            # The policy this host ran the unit with: its own, else the
            # host's defaults where they weren't the built-in ones.
            policy.setdefault(name, {})[host] = {
                f: u[f] if u.get(f) is not None
                else host_defaults[f] if host_defaults.get(f, BUILT_IN[f]) != BUILT_IN[f]
                else None
                for f in POLICY_FIELDS
            }  # fmt: skip
        spec: dict[str, Any] = {"units": bindings}
        if hd := {f: host_defaults[f] for f in HOST_FIELDS
                  if host_defaults.get(f, BUILT_IN[f]) != BUILT_IN[f]}:  # fmt: skip
            spec["defaults"] = hd
        if transfers := cfg.get("transfers"):
            spec["transfers"] = transfers
        agents[host] = spec
    for name, by_host in policy.items():
        for f in POLICY_FIELDS:
            values = {h: v[f] for h, v in by_host.items()}
            if len({repr(v) for v in values.values()}) == 1:
                if (v := next(iter(values.values()))) is not None:
                    units[name][f] = v
            else:
                for h, v in values.items():
                    if v is not None:
                        agents[h]["units"][name][f] = v
                notes.append(f"unit {name}: hosts read it differently ({f}): kept per host")
    out["units"] = units

    # ------------------------------------------------------------ tokens
    first = next(iter(hosts.values()))
    known = co.get("agents") or {}
    tokens: dict[str, str] = {}
    boots: dict[str, dict[str, Any]] = {}
    for host, cfg in hosts.items():
        ref = cfg.get("coordinator") or {}
        if host in known:
            agents[host]["token_sha256"] = known[host]["token_sha256"]
        else:
            token = secrets.token_urlsafe(32)
            tokens[host] = token
            agents[host]["token_sha256"] = hashlib.sha256(token.encode()).hexdigest()
        url = ref.get("url") or controller_url
        if not url:
            errors.append(f"{host}: had no coordinator, so give the controller's URL "
                          "(--controller-url)")  # fmt: skip
        boot: dict[str, Any] = {
            "controller": {"url": url, "token_file": ref.get("token_file", "/etc/ferryman/token")},
        }
        for k in ("report_interval", "commands"):
            if k in ref:
                boot["controller"][k] = ref[k]
        for k in ("hostname", "paths", "api", "rclone"):
            if cfg.get(k) is not None:
                boot[k] = cfg[k]
        boots[host] = boot
    out["agents"] = {h: {"token_sha256": a.pop("token_sha256"), **a} for h, a in agents.items()}
    for h in known:
        if h not in hosts:
            notes.append(f"agent {h} is in the coordinator's config but no 0.2 config was "
                         "given for it: left out")  # fmt: skip

    # ----------------------------------------------- tuning, health, notify
    if tuning := first.get("tuning"):
        out["tuning"] = tuning
    base = Tuning.model_validate(first.get("tuning") or {}).model_dump(mode="json")
    for host, cfg in hosts.items():
        theirs = Tuning.model_validate(cfg.get("tuning") or {}).model_dump(mode="json")
        if changed := {k: v for k, v in theirs.items() if base.get(k) != v}:
            out["agents"][host]["tuning"] = changed
    if health := first.get("health"):
        out["health"] = health
    for host, cfg in hosts.items():
        if (cfg.get("health") or {}) != (first.get("health") or {}):
            notes.append(f"{host}: its health settings differ from the first host's, "
                         "whose are used for every agent")  # fmt: skip
    out["notify"] = _notify(hosts, notes)

    if errors:
        raise MigrateError("\n".join(errors))
    try:
        ControllerConfig.model_validate(out)
    except ValueError as e:
        raise MigrateError(f"the migrated config doesn't load:\n{e}") from None
    return Result(out, boots, tokens, notes)


def _notify(hosts: dict[str, dict[str, Any]], notes: list[str]) -> dict[str, Any]:
    channels: dict[str, Any] = {}
    levels = []
    first = next(iter(hosts.values())).get("notify") or {}
    for host, cfg in hosts.items():
        n = cfg.get("notify") or {}
        found = []
        if m := n.get("mail"):
            found.append(("mail", {"sendmail": {k: m[k] for k in ("to", "from", "sendmail")
                                                if k in m}}))  # fmt: skip
        if w := n.get("webhook"):
            found.append(("webhook", {"webhook": w}))
        for kind, ch in found:
            name = next((k for k, v in channels.items() if v == ch), None)
            if name is None:
                name = kind if kind not in channels else f"{kind}-{host}"
                channels[name] = ch
        if found:
            levels.append(n.get("level", "warning"))
        timing = ("batch", "repeat")
        if {k: n.get(k) for k in timing} != {k: first.get(k) for k in timing}:
            notes.append(f"{host}: notify batch/repeat differ from the first host's: "
                         "the first host's are used")  # fmt: skip
    out: dict[str, Any] = {k: first[k] for k in ("batch", "repeat") if k in first}
    if channels:
        out["channels"] = channels
        if any(ch.get("sendmail") for ch in channels.values()):
            notes.append("mail went through each host's sendmail; now the controller sends it: "
                         "give it a sendmail, or change the channel to smtp")  # fmt: skip
        severities = ("info", "warning", "error")
        least = min(levels, key=severities.index)
        out["rules"] = [{"name": "default", "match": {"severity": least},
                         "send": list(channels)}]  # fmt: skip
        if len(set(levels)) > 1:
            notes.append(f"notify levels differed between hosts: {least} is used for all")
    return out
