"""Config model: units (one rclone process each, with any number of mounts),
pools of service-account keys, and transfers (move/copy/sync jobs run inside
a unit's process).

Loaded once from YAML at startup. Mount paths and transfer sources are
absolute and given in full; everything ferryman keeps for itself derives
from `paths` through `Layout`.
"""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import yaml
from croniter import croniter
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import placeholders as ph

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# A mount hides whatever is at its path. Never under these trees, and never
# exactly at one of the top-level directories (below them is fine).
_SYSTEM_TREES = tuple(map(Path, ("/proc", "/sys", "/dev", "/etc", "/usr", "/boot", "/bin",
                                 "/sbin", "/lib", "/lib32", "/lib64")))  # fmt: skip
_TOP_LEVEL = frozenset({"/", "/var", "/run", "/home", "/root", "/srv", "/mnt", "/media",
                        "/opt", "/tmp", "/var/lib", "/var/cache", "/var/log"})  # fmt: skip


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Paths(_Model):
    runtime: Path = Path("/run/ferryman")
    cache: Path = Path("/var/cache/ferryman")
    state: Path = Path("/var/lib/ferryman")
    logs: Path = Path("/var/log/ferryman")


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


# rclone reads a bare number as KiB ("8388608" is 8G), so read sizes need a
# suffix; only 0 is unambiguous without one.
_RCLONE_SIZE = re.compile(r"^(0|\d+(\.\d+)?[KMGTP]i?B?)$", re.IGNORECASE)
CacheMode = Literal["off", "minimal", "writes", "full"]
# Mount read settings, in defaults and per unit; see read_settings().
READ_SETTINGS = ("buffer_size", "vfs_cache_mode", "vfs_read_chunk_size", "vfs_read_ahead")


def _cache_mode(v: object) -> object:
    # YAML 1.1 reads a bare `off` as false; that can only mean "off" here.
    return "off" if v is False else v


def _rclone_size(v: str | None) -> str | None:
    if v is not None and not _RCLONE_SIZE.match(v):
        raise ValueError(f"size {v!r} needs a unit suffix (16M, 1G); a bare number is KiB "
                         "to rclone")  # fmt: skip
    return v


class UnitDefaults(_Model):
    uid: int | None = None
    gid: int | None = None
    cache_max_size: str = "10G"
    cache_min_free_space: str = "100G"
    warm: bool = True
    # How a mount reads. Small on purpose: library scanners open every file
    # just to probe its header, and each open fetches the read-ahead window
    # into the VFS cache; at 256M buffer + 256M read-ahead that churned ~600G
    # through a 64G cache in under three days. A unit of many small random
    # reads may want less still (buffer_size: 0, a smaller chunk).
    buffer_size: str = "16M"  # in-memory read-ahead per open file
    vfs_cache_mode: CacheMode = "full"
    vfs_read_chunk_size: str = "128M"  # size of each ranged read from the remote
    vfs_read_ahead: str = "0"  # extra on-disk read-ahead; full cache mode only
    # Mode of a mount directory ferryman creates (only the directory itself:
    # its parent must exist), owned by the unit's uid/gid.
    dir_mode: int = 0o775

    @field_validator("buffer_size", "vfs_read_chunk_size", "vfs_read_ahead")
    @classmethod
    def _sizes(cls, v: str) -> str:
        _rclone_size(v)
        return v

    @field_validator("vfs_cache_mode", mode="before")
    @classmethod
    def _mode(cls, v: object) -> object:
        return _cache_mode(v)


_SIZE = re.compile(r"^(\d+)([KMGT]?)$")


def parse_size(v: str | int) -> int:
    """rclone-style size ("128M", "2G") in bytes; suffixes are binary."""
    if isinstance(v, int):
        return v
    m = _SIZE.match(v.strip().upper().removesuffix("I").removesuffix("B"))
    if not m:
        raise ValueError(f"invalid size {v!r} (e.g. 128M, 2G)")
    return int(m[1]) << (10 * " KMGT".index(m[2] or " "))


class Tuning(_Model):
    """`ferryman tune`: probes Drive uploads at increasing chunk sizes, then
    increasing transfers, and keeps the cheapest setting near the best rate.
    Probe bytes count against the key's daily upload cap like any upload."""

    budget: str = "32G"  # most a whole tune may upload
    probe_bytes: str = "2G"  # most one probe may upload
    max_memory: str = "2G"  # transfers x chunk size never exceeds this
    warmup: float = Field(default=3.0, ge=0)  # seconds discarded at each probe's start
    window: float = Field(default=15.0, gt=0)  # seconds measured after it
    # Each probe runs this often and keeps its best rate: the path to Google
    # varies by tens of percent between identical back-to-back runs.
    repeats: int = Field(default=2, ge=1)
    chunk_sizes: list[str] = Field(default_factory=lambda: ["8M", "16M", "32M", "64M", "128M",
                                                            "256M"])  # fmt: skip
    transfers: list[int] = Field(default_factory=lambda: [1, 2, 4, 8, 12, 16])
    # suggest: record the profile only. auto: apply it unless it is noisy,
    # and re-tune on drift. (`ferryman tune --apply` applies one either way.)
    apply: Literal["suggest", "auto"] = "suggest"
    schedule: str | None = None  # cron for a re-tune; skipped while the link is busy
    # Hosts behind the same uplink share a name here; with a coordinator, only
    # one of them tunes at a time, or each would measure the others' probes.
    uplink: str | None = None
    # Drift: drift_runs qualifying transfer runs in a row below drift x the
    # profile's rate raise tune.drift (and, with apply: auto, a re-tune).
    drift: float = Field(default=0.6, gt=0, lt=1)
    drift_runs: int = Field(default=3, ge=1)

    @field_validator("schedule")
    @classmethod
    def _cron(cls, v: str | None) -> str | None:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"invalid cron expression: {v!r}")
        return v

    @field_validator("budget", "probe_bytes", "max_memory")
    @classmethod
    def _size(cls, v: str) -> str:
        parse_size(v)
        return v

    @field_validator("chunk_sizes")
    @classmethod
    def _chunks(cls, v: list[str]) -> list[str]:
        sizes = [parse_size(s) for s in v]
        if not sizes or sizes != sorted(set(sizes)):
            raise ValueError("chunk_sizes must be ascending and distinct")
        return v

    @field_validator("transfers")
    @classmethod
    def _steps(cls, v: list[int]) -> list[int]:
        if not v or v[0] != 1 or v != sorted(set(v)):
            raise ValueError("transfers must be ascending, distinct and start at 1")
        return v


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


class Mount(_Model):
    """One path a unit's remote is mounted at. All of a unit's mounts share
    its process, connection and VFS cache, so only FUSE options can differ
    between them: a VFS setting that differed would give the path its own
    cache (see the read settings, which are per unit)."""

    # Absolute; takes {hostname}, {unit} and {env:NAME}.
    path: str
    allow_other: bool = True  # other users (a media server) may use the mount
    default_permissions: bool = True  # the kernel enforces the mode bits
    # A kernel bind of the unit's writable mount, remounted ro: same files
    # and cache, writes refused (root only). If every mount is read_only,
    # the first is rclone's own, with --read-only, and the rest bind to it.
    read_only: bool = False

    @field_validator("path")
    @classmethod
    def _absolute(cls, v: str) -> str:
        ph.parse(v, ph.MOUNT)
        if not v.startswith("/") or ".." in PurePosixPath(v).parts:
            raise ValueError(f"mount path {v!r} must be absolute, without '..'")
        return v


ServeType = Literal["dlna", "ftp", "http", "nfs", "restic", "s3", "sftp", "webdav"]
_HTPASSWD_TYPES = frozenset({"http", "webdav", "restic"})
_TLS_TYPES = frozenset({"ftp", "http", "restic", "s3", "webdav"})
_NO_AUTH_TYPES = frozenset({"dlna", "nfs"})  # rclone has no auth for these
# Options a server may not set: the VFS ones would give it its own VFS (a
# second cache on the unit's cache dir, and rc vfs/* calls that fail as
# ambiguous); the rest are this model's fields.
_SERVE_RESERVED = frozenset({
    "fs", "type", "addr", "user", "pass", "htpasswd", "cert", "key", "passive_port", "auth_key",
    "buffer_size", "read_only", "uid", "gid", "umask", "dir_perms", "file_perms", "link_perms",
    "dir_cache_time", "poll_interval", "no_modtime", "no_seek", "no_checksum", "links",
    "case_insensitive", "vfs_case_insensitive", "attr_timeout",
})  # fmt: skip
_ADDR = re.compile(r"^(?P<host>\[[0-9a-fA-F:.]*\]|[^:\[\]]*):(?P<port>\d{1,5})$")
_PORT_RANGE = re.compile(r"^(\d{1,5})-(\d{1,5})$")


def _loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False  # a name: it could resolve anywhere


_WILDCARD = frozenset({"", "0.0.0.0", "[::]"})


class Serve(_Model):
    """A server started inside the unit's process (`serve/start`). It shares
    the process's connection and VFS: the unit's read settings, uid/gid and
    cache apply to it, and key rotation moves it with everything else."""

    type: ServeType
    addr: str  # host:port; required, since every type's default port would clash
    user: str | None = None
    # File holding the password (s3: the secret key, `user` the access key).
    # Read when the server starts, so it never sits in the config.
    pass_file: Path | None = None
    htpasswd: Path | None = None  # http, webdav, restic
    cert: Path | None = None  # TLS; ftp is then FTPS with implicit TLS (ftps://)
    key: Path | None = None
    # sftp's SSH host key (rclone makes and keeps one in its cache dir if unset).
    host_key: Path | None = None
    passive_port: str | None = None  # ftp: data port range "30000-30099", required
    # nfs and dlna have no auth: beyond loopback only with this set.
    allow_unauthenticated: bool = False
    # Passed to serve/start as-is (rclone's flag names, '-' as '_').
    options: dict[str, str | int | bool | list[str]] = Field(default_factory=dict)

    @property
    def authenticated(self) -> bool:
        if self.htpasswd is not None:
            return True
        if self.type == "sftp" and "authorized_keys" in self.options:
            return True
        return self.user is not None and self.pass_file is not None

    @property
    def host_port(self) -> tuple[str, int]:
        m = _ADDR.match(self.addr)
        assert m
        return m["host"], int(m["port"])

    @property
    def passive_range(self) -> tuple[int, int] | None:
        if self.passive_port is None:
            return None
        m = _PORT_RANGE.match(self.passive_port)
        assert m
        return int(m[1]), int(m[2])

    @model_validator(mode="after")
    def _shape(self) -> Serve:
        m = _ADDR.match(self.addr)
        if not m or not 0 < int(m["port"]) < 65536:
            raise ValueError(f"{self.type} serve: addr {self.addr!r} must be host:port "
                             "(127.0.0.1:8081, :8081 for every address)")  # fmt: skip
        if (self.user is None) != (self.pass_file is None):
            raise ValueError(f"{self.type} serve: user and pass_file go together")
        if self.htpasswd is not None and self.type not in _HTPASSWD_TYPES:
            raise ValueError(f"{self.type} serve: htpasswd is for http, webdav and restic")
        if (self.cert is None) != (self.key is None):
            raise ValueError(f"{self.type} serve: cert and key go together")
        if self.cert is not None and self.type not in _TLS_TYPES:
            raise ValueError(f"{self.type} serve: cert/key are for {', '.join(sorted(_TLS_TYPES))}")
        if self.host_key is not None and self.type != "sftp":
            raise ValueError(f"{self.type} serve: host_key is for sftp only")
        if self.type in _NO_AUTH_TYPES and (self.user or self.htpasswd):
            raise ValueError(f"{self.type} serve: rclone has no authentication for {self.type}")
        if self.type == "ftp":
            r = _PORT_RANGE.match(self.passive_port or "")
            if not r or not 0 < int(r[1]) <= int(r[2]) < 65536:
                raise ValueError("ftp serve: passive_port must be a range like 30000-30099 "
                                 "(each FTP server needs its own)")  # fmt: skip
        elif self.passive_port is not None:
            raise ValueError(f"{self.type} serve: passive_port is for ftp only")
        reserved = sorted(k for k in self.options if k in _SERVE_RESERVED or k.startswith("vfs_"))
        if reserved:
            raise ValueError(
                f"{self.type} serve: options {', '.join(reserved)} can't be set here: VFS "
                "settings are the unit's (one per server would split its cache), and the "
                "others are serve fields"
            )
        host = m["host"]
        if not _loopback(host) and not self.authenticated and not self.allow_unauthenticated:
            raise ValueError(
                f"{self.type} serve on {self.addr} is reachable beyond this host without "
                "authentication: add user + pass_file (or htpasswd), bind it to 127.0.0.1, "
                "or set allow_unauthenticated: true"
            )
        return self


class Unit(_Model):
    backend: str = "drive"
    pool: str | None = None
    drive_id: str | None = None
    # Extra rclone remote options, rendered verbatim into the unit's config.
    options: dict[str, str] = Field(default_factory=dict)
    # The first is the process's own `rclone mount`; the rest are added to it
    # over rc. None: no FUSE, the process is an `rclone rcd`.
    mounts: list[Mount] = Field(default_factory=list)
    # Servers inside the unit's process; see Serve.
    serve: list[Serve] = Field(default_factory=list)
    uid: int | None = None
    gid: int | None = None
    cache_max_size: str | None = None
    warm: bool | None = None
    # Read settings for this mount; unset takes `defaults`.
    buffer_size: str | None = None
    vfs_cache_mode: CacheMode | None = None
    vfs_read_chunk_size: str | None = None
    vfs_read_ahead: str | None = None
    # Appended last, so a flag here overrides one ferryman sets (rclone takes
    # the last value of a repeated flag). Prefer the settings above: they are
    # validated and show in status.
    mount_args: list[str] = Field(default_factory=list)

    @field_validator("buffer_size", "vfs_read_chunk_size", "vfs_read_ahead")
    @classmethod
    def _sizes(cls, v: str | None) -> str | None:
        return _rclone_size(v)

    @field_validator("vfs_cache_mode", mode="before")
    @classmethod
    def _mode(cls, v: object) -> object:
        return _cache_mode(v)

    @property
    def primary(self) -> int:
        """Index of the mount the process itself makes: the first writable
        one (the read_only ones bind to it), else the first."""
        return next((i for i, m in enumerate(self.mounts) if not m.read_only), 0)

    @model_validator(mode="after")
    def _drive_needs_pool(self) -> Unit:
        if self.backend == "drive" and not self.pool:
            raise ValueError("drive units need a pool")
        return self

    @model_validator(mode="after")
    def _binds_inherit(self) -> Unit:
        # A bind mount shares the FUSE mount it binds: its allow_other and
        # default_permissions are that mount's, whatever it says.
        if not self.mounts:
            return self
        p = self.mounts[self.primary]
        for i, m in enumerate(self.mounts):
            if i == self.primary or not m.read_only:
                continue
            for opt in ("allow_other", "default_permissions"):
                if getattr(m, opt) != getattr(p, opt):
                    raise ValueError(
                        f"read_only mount {m.path} binds {p.path}, so its {opt} is "
                        f"{p.path}'s ({getattr(p, opt)}): leave it out or set it there"
                    )
        return self


class Transfer(_Model):
    unit: str
    mode: Literal["move", "copy", "sync"] = "move"
    # Absolute. Takes the fixed placeholders ({hostname}, {transfer}, {unit},
    # {env:NAME}); see placeholders.py.
    source: str
    # Path inside the unit's remote. Takes the fixed placeholders and the
    # per-run time ones ({now:FORMAT}, {date}, {weekday}).
    dest: str = ""
    schedule: str | None = None  # cron expression; None = run on demand only
    # Unset: the applied tuning profile's transfer count, else 8.
    transfers: int | None = Field(default=None, ge=1)
    checkers: int | None = Field(default=None, ge=1)
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
    def _source_placeholders(cls, v: str) -> str:
        ph.parse(v, ph.FIXED)
        if not v.startswith("/") or ".." in PurePosixPath(v).parts:
            raise ValueError(f"source {v!r} must be absolute, without '..'")
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
    api: Api = Api()
    rclone: Rclone = Rclone()
    health: Health = Health()
    notify: Notify = Notify()
    defaults: UnitDefaults = UnitDefaults()
    tuning: Tuning = Tuning()
    pools: dict[str, Pool] = Field(default_factory=dict)
    units: dict[str, Unit] = Field(default_factory=dict)
    transfers: dict[str, Transfer] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _removed(cls, data: Any) -> Any:
        """Name the 0.2 replacement for a key that was removed, rather than
        leave a bare 'extra inputs are not permitted'."""
        if not isinstance(data, dict):
            return data
        if "roots" in data:
            raise ValueError(
                "roots was removed in 0.2: give each unit `mounts: [{path: "
                "/absolute/path}]` and each transfer an absolute `source`"
            )
        for name, u in (data.get("units") or {}).items():
            if isinstance(u, dict) and (gone := sorted({"mountpoint", "mount"} & u.keys())):
                raise ValueError(
                    f"unit {name}: {' and '.join(gone)} removed in 0.2: use "
                    "`mounts: [{path: /absolute/path}]` (no mounts: no FUSE)"
                )
        for name, t in (data.get("transfers") or {}).items():
            if isinstance(t, dict) and "source" not in t:
                raise ValueError(f"transfer {name}: source is required since 0.2 "
                                 "(an absolute path)")  # fmt: skip
        return data

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
        for name, unit in self.units.items():
            if unit.pool and unit.pool not in self.pools:
                raise ValueError(f"unit {name}: unknown pool {unit.pool!r}")
            set_here = [k for k in READ_SETTINGS if getattr(unit, k) is not None]
            if not (unit.mounts or unit.serve) and set_here:
                raise ValueError(f"unit {name}: {', '.join(set_here)} only apply to mounts "
                                 "and servers")  # fmt: skip
            r = read_settings(self, name)
            if any(s.type == "nfs" for s in unit.serve) and \
                    r["vfs_cache_mode"]["value"] not in ("writes", "full"):  # fmt: skip
                raise ValueError(f"unit {name}: an nfs server needs vfs_cache_mode writes or "
                                 "full (rclone serves it read-only otherwise)")  # fmt: skip
            if (unit.mounts or unit.serve) and r["vfs_read_ahead"]["value"] != "0" and \
                    r["vfs_cache_mode"]["value"] != "full":  # fmt: skip
                raise ValueError(f"unit {name}: vfs_read_ahead only applies with "
                                 "vfs_cache_mode: full")  # fmt: skip
        self._check_ports()
        for name, t in self.transfers.items():
            if t.unit not in self.units:
                raise ValueError(f"transfer {name}: unknown unit {t.unit!r}")
        return self

    def _check_ports(self) -> None:
        """Two servers on one port would make rclone panic (a nil pointer,
        not an "address in use"), so overlaps are refused here: the same
        port on the same or a wildcard host, or inside an FTP passive range."""
        taken: list[tuple[str, int, int, str]] = []  # host, first, last port, who
        if (lst := self.api.listen) is not None:
            taken.append((lst.host, lst.port, lst.port, "api.listen"))
        for name, unit in self.units.items():
            for s in unit.serve:
                who = f"unit {name}'s {s.type} serve"
                host, port = s.host_port
                ranges = [(port, port)]
                if (pr := s.passive_range) is not None:
                    ranges.append(pr)
                for lo, hi in ranges:
                    for h2, lo2, hi2, who2 in taken:
                        if lo <= hi2 and lo2 <= hi and (h2 == host or {h2, host} & _WILDCARD):
                            raise ValueError(f"{who} ({host}:{lo}-{hi}) overlaps {who2}'s "
                                             f"({h2}:{lo2}-{hi2})")  # fmt: skip
                    taken.append((host, lo, hi, who))

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

    def mount_paths(self, unit: str) -> list[Path]:
        """The unit's mount paths, placeholders expanded; the first is the
        process's own mount."""
        values = {"hostname": self.hostname, "unit": unit}
        return [Path(ph.expand(m.path, values)) for m in self.cfg.units[unit].mounts]

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

    @property
    def tune_dir(self) -> Path:
        # The probe process's conf and rc socket (unit names cannot start with ".").
        return self.cfg.paths.runtime / ".tune"

    @property
    def tune_source(self) -> Path:
        # Probe source data: random, so nothing on the path can compress it.
        return self.cfg.paths.cache / ".tune"

    @property
    def tune_log(self) -> Path:
        return self.cfg.paths.logs / "tune.log"

    def tune_profile(self, backend: str) -> Path:
        # The latest tune's result.
        return self.cfg.paths.state / "tuning" / f"{backend}.json"

    def tune_active(self, backend: str) -> Path:
        # The profile in effect: written only when one is applied.
        return self.cfg.paths.state / "tuning" / f"{backend}.active.json"

    def pool_state(self, pool: str) -> Path:
        return self.cfg.paths.state / "pools" / f"{pool}.json"

    def _placeholders(self, name: str) -> dict[str, str]:
        return {"hostname": self.hostname, "transfer": name, "unit": self.cfg.transfers[name].unit}

    def transfer_source(self, name: str) -> Path:
        return Path(ph.expand(self.cfg.transfers[name].source, self._placeholders(name)))

    def transfer_dest(self, name: str, now: datetime | None = None) -> str:
        """The dest for a run starting at `now` (default: now)."""
        return ph.expand(self.cfg.transfers[name].dest, self._placeholders(name), now)

    def claim_dest(self, name: str) -> str:
        """What a coordinator claim covers: the part of the dest that is the
        same on every run (all of it, unless it has time placeholders)."""
        return ph.fixed_prefix(self.cfg.transfers[name].dest, self._placeholders(name))

    def check_paths(self) -> list[str]:
        """Problems with the paths as they expand on this host, for `verify`
        and agent startup: placeholders that don't expand (an unset
        {env:NAME}), mount paths that would hide something, and transfer
        sources that overlap a mount."""
        problems: list[str] = []
        mounts: dict[Path, str] = {}
        for unit in self.cfg.units:
            try:
                paths = self.mount_paths(unit)
            except ph.PlaceholderError as e:
                problems.append(f"unit {unit}: {e}")
                continue
            for p in paths:
                if why := self._unsafe_mount(p):
                    problems.append(f"unit {unit}: mount path {p} {why}")
                if p in mounts:
                    owner = mounts[p]
                    problems.append(f"unit {unit} mounts {p} twice" if owner == unit
                                    else f"units {owner} and {unit} both mount {p}")  # fmt: skip
                mounts.setdefault(p, unit)
        for a, ua in mounts.items():
            for b, ub in mounts.items():
                if a != b and a.is_relative_to(b):
                    problems.append(f"unit {ua}: mount path {a} is inside unit {ub}'s mount {b}")
        for unit, u in self.cfg.units.items():
            for sv in u.serve:
                for label, f in (("pass_file", sv.pass_file), ("htpasswd", sv.htpasswd),
                                 ("cert", sv.cert), ("key", sv.key),
                                 ("host_key", sv.host_key)):  # fmt: skip
                    where = f"unit {unit}: {sv.type} serve on {sv.addr}: {label} {f}"
                    if f is None:
                        continue
                    if not f.is_file():
                        problems.append(f"{where} does not exist")
                    elif label in ("pass_file", "key", "host_key") and f.stat().st_mode & 0o004:
                        problems.append(f"{where} is readable by everyone")
        for name in self.cfg.transfers:
            try:
                src = self.transfer_source(name)
                self.transfer_dest(name)
            except ph.PlaceholderError as e:
                problems.append(f"transfer {name}: {e}")
                continue
            if not src.is_absolute():
                problems.append(f"transfer {name}: source {src} is not absolute")
            for m, unit in mounts.items():
                if src.is_relative_to(m):
                    problems.append(
                        f"transfer {name}: source {src} is inside unit {unit}'s "
                        f"mount {m}: it would move files from Drive to Drive"
                    )
                elif m.is_relative_to(src):
                    problems.append(f"transfer {name}: source {src} contains unit {unit}'s "
                                    f"mount {m}: a run would walk into the mount")  # fmt: skip
        return problems

    def _unsafe_mount(self, p: Path) -> str | None:
        if not p.is_absolute():
            return "is not absolute"
        if str(p) in _TOP_LEVEL or any(p.is_relative_to(t) for t in _SYSTEM_TREES):
            return "is a system directory: mounting there would hide what is in it"
        for own in (self.cfg.paths.runtime, self.cfg.paths.cache, self.cfg.paths.state,
                    self.cfg.paths.logs):  # fmt: skip
            if p.is_relative_to(own) or own.is_relative_to(p):
                return f"overlaps ferryman's own {own}"
        return None

    def leased_keys(self, pool: str) -> Path:
        # Keys leased from the coordinator, on the runtime (tmpfs) dir only.
        return self.cfg.paths.runtime / "keys" / pool

    @property
    def claims_file(self) -> Path:
        return self.cfg.paths.state / "claims.json"


def read_settings(cfg: Config, unit: str) -> dict[str, dict[str, str]]:
    """Each mount read setting's value and where it comes from: the unit,
    the config's `defaults`, or ferryman's built-in default."""
    u, d = cfg.units[unit], cfg.defaults
    out = {}
    for k in READ_SETTINGS:
        if (v := getattr(u, k)) is not None:
            out[k] = {"value": v, "source": "unit"}
        else:
            # A defaults entry that restates the built-in value changes nothing.
            built_in = type(d).model_fields[k].default
            src = "defaults" if getattr(d, k) != built_in else "built-in"
            out[k] = {"value": getattr(d, k), "source": src}
    return out


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
