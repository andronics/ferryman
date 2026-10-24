"""Config model: units (one rclone process each), pools of service-account keys,
and transfers (move/copy/sync jobs run inside a unit's process).

Loaded once from YAML at startup. Every path ferryman touches derives from
`paths` and `roots` through `Layout`, so nothing else builds paths by hand.
"""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from typing import Literal

import yaml
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


class Api(_Model):
    socket: Path | None = None  # default: <paths.runtime>/api.sock
    socket_mode: int = 0o660


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
    # default: <roots.local>/<unit mountpoint>; relative paths resolve under roots.local
    source: Path | None = None
    dest: str = ""  # path inside the unit's remote
    schedule: str | None = None  # cron expression; None = run on demand only
    transfers: int = 8
    checkers: int = 8

    @field_validator("schedule")
    @classmethod
    def _cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"invalid cron expression: {v!r}")
        return v


class Config(_Model):
    coordinator: CoordinatorRef | None = None
    paths: Paths = Paths()
    roots: Roots
    api: Api = Api()
    rclone: Rclone = Rclone()
    health: Health = Health()
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

    def transfer_source(self, name: str) -> Path:
        t = self.cfg.transfers[name]
        if t.source is None:
            return self.local_path(t.unit)
        return t.source if t.source.is_absolute() else self.cfg.roots.local / t.source

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


class Listen(_Model):
    host: str = "127.0.0.1"
    port: int = 8470
    # Traefik's address(es), so X-Forwarded-For is trusted for logging.
    forwarded_allow_ips: str = "127.0.0.1"


class AgentEntry(_Model):
    # sha256 of the agent's bearer token (`ferryman token` makes both).
    token_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pools: list[str] = Field(default_factory=list)


class CoordinatorConfig(_Model):
    listen: Listen = Listen()
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
