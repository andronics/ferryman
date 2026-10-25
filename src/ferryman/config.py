"""Config model: units (one rclone process each), pools of service-account keys,
and transfers (move/copy/sync jobs run inside a unit's process).

Loaded once from YAML at startup. Every path ferryman touches derives from
`paths` and `roots` through `Layout`, so nothing else builds paths by hand.
"""

from __future__ import annotations

import os
import re
import socket
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Literal

import yaml
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import placeholders as ph

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Paths(_Model):
    runtime: Path = Path("/run/ferryman")
    cache: Path = Path("/var/cache/ferryman")
    state: Path = Path("/var/lib/ferryman")
    logs: Path = Path("/var/log/ferryman")


class Roots(_Model):
    # Staged files waiting to be moved up live under `local`; mounts appear
    # under `remote`. A unit's mountpoint is relative to both.
    local: Path
    remote: Path


class Listen(_Model):
    host: str = "127.0.0.1"
    port: int = 8470
    # Traefik's address(es), so X-Forwarded-For is trusted for logging.
    forwarded_allow_ips: str = "127.0.0.1"


class Api(_Model):
    socket: Path | None = None  # default: <paths.runtime>/api.sock
    socket_mode: int = 0o660
    # Optional TCP listener with /healthz, /readyz and /metrics only (no
    # control: that stays on the socket), for Prometheus and k8s probes.
    listen: Listen | None = None


class MailNotify(_Model):
    to: list[str] = Field(min_length=1)
    sender: str | None = Field(default=None, alias="from")  # default ferryman@<hostname>
    sendmail: str = "/usr/sbin/sendmail"


class WebhookNotify(_Model):
    url: str  # receives a JSON POST: {host, title, text, events: [...]}
    headers: dict[str, str] = Field(default_factory=dict)


class Notify(_Model):
    level: Literal["info", "warning", "error"] = "warning"
    batch: float = 60.0  # collect events this long, then send them as one message
    repeat: float = 21600.0  # re-send a problem that is still open after this long
    mail: MailNotify | None = None
    webhook: WebhookNotify | None = None


class Rclone(_Model):
    binary: str = "rclone"
    log_level: str = "INFO"


class Health(_Model):
    interval: float = 120.0
    stat_timeout: float = 10.0


class UnitDefaults(_Model):
    uid: int | None = None
    gid: int | None = None
    cache_max_size: str = "10G"
    cache_min_free_space: str = "100G"
    warm: bool = True
    # Mode for directories ferryman creates under roots.remote (mountpoints
    # and their missing parents), owned by the unit's uid/gid.
    dir_mode: int = 0o775


class Pool(_Model):
    # A local pool names its keys by client_email; each is <keys_dir>/<key>.json.
    # A coordinator pool (`coordinator: true`) has neither: the coordinator
    # owns the keys and the rotation state, and agents lease from it.
    coordinator: bool = False
    keys_dir: Path | None = None
    keys: list[str] = Field(default_factory=list)
    quota_window: int = 86400

    @model_validator(mode="after")
    def _shape(self) -> Pool:
        if self.coordinator:
            if self.keys or self.keys_dir:
                raise ValueError("a coordinator pool has no keys or keys_dir of its own")
            return self
        if not self.keys or self.keys_dir is None:
            raise ValueError("a local pool needs keys_dir and at least one key")
        if len(set(self.keys)) != len(self.keys):
            raise ValueError("duplicate keys")
        return self


class CoordinatorRef(_Model):
    url: str  # e.g. https://ferryman.andronics.io (TLS terminated by Traefik)
    token_file: Path
    agent: str | None = None  # informational; the token identifies the agent
    # How often the agent reports its status (for the coordinator's UI and
    # metrics), and whether it takes commands (restart, run) from that UI.
    report_interval: float = 15.0
    commands: bool = True


class Unit(_Model):
    mountpoint: str
    backend: str = "drive"
    pool: str | None = None
    drive_id: str | None = None
    # Extra rclone remote options, rendered verbatim into the unit's config.
    options: dict[str, str] = Field(default_factory=dict)
    # false: no FUSE mount, the process is an `rclone rcd` that only runs transfers.
    mount: bool = True
    uid: int | None = None
    gid: int | None = None
    cache_max_size: str | None = None
    warm: bool | None = None
    mount_args: list[str] = Field(default_factory=list)

    @field_validator("mountpoint")
    @classmethod
    def _relative(cls, v: str) -> str:
        p = PurePosixPath(v)
        if not v or p.is_absolute() or ".." in p.parts:
            raise ValueError("mountpoint must be a relative path without '..'")
        return str(p)

    @model_validator(mode="after")
    def _drive_needs_pool(self) -> Unit:
        if self.backend == "drive" and not self.pool:
            raise ValueError("drive units need a pool")
        return self


class Transfer(_Model):
    unit: str
    mode: Literal["move", "copy", "sync"] = "move"
    # default: <roots.local>/<unit mountpoint>; relative paths resolve under
    # roots.local. Takes the fixed placeholders ({hostname}, {transfer},
    # {unit}, {env:NAME}); see placeholders.py.
    source: Path | None = None
    # Path inside the unit's remote. Takes the fixed placeholders and the
    # per-run time ones ({now:FORMAT}, {date}, {weekday}).
    dest: str = ""
    schedule: str | None = None  # cron expression; None = run on demand only
    transfers: int = 8
    checkers: int = 8
    # rclone filter patterns of source files to leave alone (e.g. "*.tmp").
    exclude: list[str] = Field(default_factory=list)
    # Skip files modified more recently than this ("15m", "2h"): ones still
    # being written.
    min_age: str | None = None
    # sync only. A sync deletes whatever is at the destination but not in the
    # source, so an empty source would wipe it: refused unless allow_empty.
    # max_delete aborts the job after that many deletions.
    allow_empty: bool = False
    max_delete: int | None = Field(default=None, ge=0)

    @field_validator("schedule")
    @classmethod
    def _cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"invalid cron expression: {v!r}")
        return v

    @field_validator("dest")
    @classmethod
    def _dest_placeholders(cls, v: str) -> str:
        ph.parse(v, ph.FIXED | ph.TIME)
        return v

    @field_validator("source")
    @classmethod
    def _source_placeholders(cls, v: Path | None) -> Path | None:
        if v is not None:
            ph.parse(str(v), ph.FIXED)
        return v

    @model_validator(mode="after")
    def _sync_only(self) -> Transfer:
        if self.mode != "sync" and (self.allow_empty or self.max_delete is not None):
            raise ValueError("allow_empty and max_delete apply to mode: sync only")
        if self.mode == "sync" and not ph.repeats_weekly(self.dest):
            raise ValueError(
                f"dest {self.dest!r}: a sync into a folder that changes every run uploads a "
                "full copy each time and never removes the old ones; use {weekday} for "
                "rolling copies, or mode: copy for dated archives"
            )
        return self


class Config(_Model):
    # What `{hostname}` expands to. Default: $FERRYMAN_HOSTNAME, else the
    # host's name (in a pod, set the env from the node name).
    hostname: str | None = None
    coordinator: CoordinatorRef | None = None
    paths: Paths = Paths()
    roots: Roots
    api: Api = Api()
    rclone: Rclone = Rclone()
    health: Health = Health()
    notify: Notify = Notify()
    defaults: UnitDefaults = UnitDefaults()
    pools: dict[str, Pool] = Field(default_factory=dict)
    units: dict[str, Unit] = Field(default_factory=dict)
    transfers: dict[str, Transfer] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _references(self) -> Config:
        for kind, names in (
            ("pool", self.pools),
            ("unit", self.units),
            ("transfer", self.transfers),
        ):
            for name in names:
                if not _NAME.match(name):
                    raise ValueError(f"invalid {kind} name: {name!r}")
        if self.coordinator is None:
            for name, pool in self.pools.items():
                if pool.coordinator:
                    raise ValueError(f"pool {name} is served by the coordinator, but no "
                                     "coordinator is configured")  # fmt: skip
        seen: dict[str, str] = {}
        for name, unit in self.units.items():
            if unit.pool and unit.pool not in self.pools:
                raise ValueError(f"unit {name}: unknown pool {unit.pool!r}")
            if unit.mount:
                if unit.mountpoint in seen:
                    raise ValueError(
                        f"units {seen[unit.mountpoint]} and {name} share mountpoint "
                        f"{unit.mountpoint!r}"
                    )
                seen[unit.mountpoint] = name
        for name, t in self.transfers.items():
            if t.unit not in self.units:
                raise ValueError(f"transfer {name}: unknown unit {t.unit!r}")
        return self

    @classmethod
    def load(cls, path: Path) -> Config:
        with path.open() as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)


class Layout:
    """Every filesystem path derived from the config."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.hostname = cfg.hostname or os.environ.get("FERRYMAN_HOSTNAME") or socket.gethostname()

    @property
    def api_socket(self) -> Path:
        return self.cfg.api.socket or self.cfg.paths.runtime / "api.sock"

    def mount_path(self, unit: str) -> Path:
        return self.cfg.roots.remote / self.cfg.units[unit].mountpoint

    def local_path(self, unit: str) -> Path:
        return self.cfg.roots.local / self.cfg.units[unit].mountpoint

    def runtime_dir(self, unit: str) -> Path:
        return self.cfg.paths.runtime / "units" / unit

    def rc_socket(self, unit: str) -> Path:
        return self.runtime_dir(unit) / "rc.sock"

    def rclone_conf(self, unit: str) -> Path:
        return self.runtime_dir(unit) / "rclone.conf"

    def cache_dir(self, unit: str) -> Path:
        return self.cfg.paths.cache / unit

    def log_file(self, unit: str) -> Path:
        return self.cfg.paths.logs / f"{unit}.log"

    def pool_state(self, pool: str) -> Path:
        return self.cfg.paths.state / "pools" / f"{pool}.json"

    def _placeholders(self, name: str) -> dict[str, str]:
        return {"hostname": self.hostname, "transfer": name, "unit": self.cfg.transfers[name].unit}

    def transfer_source(self, name: str) -> Path:
        t = self.cfg.transfers[name]
        if t.source is None:
            return self.local_path(t.unit)
        src = Path(ph.expand(str(t.source), self._placeholders(name)))
        return src if src.is_absolute() else self.cfg.roots.local / src

    def transfer_dest(self, name: str, now: datetime | None = None) -> str:
        """The dest for a run starting at `now` (default: now)."""
        return ph.expand(self.cfg.transfers[name].dest, self._placeholders(name), now)

    def claim_dest(self, name: str) -> str:
        """What a coordinator claim covers: the part of the dest that is the
        same on every run (all of it, unless it has time placeholders)."""
        return ph.fixed_prefix(self.cfg.transfers[name].dest, self._placeholders(name))

    def check_placeholders(self) -> list[str]:
        """Problems expanding each transfer's source and dest here (an unset
        {env:NAME}), for `verify` and agent startup."""
        problems = []
        for name in self.cfg.transfers:
            try:
                self.transfer_source(name)
                self.transfer_dest(name)
            except ph.PlaceholderError as e:
                problems.append(f"transfer {name}: {e}")
        return problems

    def leased_keys(self, pool: str) -> Path:
        # Keys leased from the coordinator, on the runtime (tmpfs) dir only.
        return self.cfg.paths.runtime / "keys" / pool

    @property
    def claims_file(self) -> Path:
        return self.cfg.paths.state / "claims.json"


def resolved(cfg: Config, unit: str) -> tuple[int | None, int | None, str, bool]:
    """(uid, gid, cache_max_size, warm) after applying `defaults`."""
    u, d = cfg.units[unit], cfg.defaults
    return (
        u.uid if u.uid is not None else d.uid,
        u.gid if u.gid is not None else d.gid,
        u.cache_max_size or d.cache_max_size,
        u.warm if u.warm is not None else d.warm,
    )


# ---------------------------------------------------------------- coordinator


class AgentEntry(_Model):
    # sha256 of the agent's bearer token (`ferryman token` makes both).
    token_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pools: list[str] = Field(default_factory=list)


class Ui(_Model):
    # The web UI at /ui/, for Traefik to put behind forward-auth. Off by default.
    enabled: bool = False
    # Header the auth proxy sets to the signed-in user. When set, requests
    # without it are refused, and actions are logged with the user. It is
    # only as trustworthy as the network path: nothing but the proxy must be
    # able to reach the coordinator's port.
    user_header: str | None = "X-authentik-username"


class CoordinatorConfig(_Model):
    listen: Listen = Listen()
    ui: Ui = Ui()
    state: Path = Path("/var/lib/ferryman-coordinator")
    watch_timeout: float = 30.0
    pools: dict[str, Pool] = Field(default_factory=dict)
    agents: dict[str, AgentEntry] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _references(self) -> CoordinatorConfig:
        for name, pool in self.pools.items():
            if pool.coordinator:
                raise ValueError(f"pool {name}: the coordinator serves local pools only")
        hashes: set[str] = set()
        for name, agent in self.agents.items():
            if not _NAME.match(name):
                raise ValueError(f"invalid agent name: {name!r}")
            for pool_name in agent.pools:
                if pool_name not in self.pools:
                    raise ValueError(f"agent {name}: unknown pool {pool_name!r}")
            if agent.token_sha256 in hashes:
                raise ValueError(f"agent {name}: token shared with another agent")
            hashes.add(agent.token_sha256)
        return self

    @classmethod
    def load(cls, path: Path) -> CoordinatorConfig:
        with path.open() as f:
            data = yaml.safe_load(f) or {}
        return cls.model_validate(data)
