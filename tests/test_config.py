from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import ValidationError

from ferryman.config import Config, Layout, resolved


def test_defaults_and_layout(make_config: Callable[..., Config], tmp_path: Path) -> None:
    cfg = make_config(defaults={"uid": 1000, "gid": 984})
    layout = Layout(cfg)
    assert layout.mount_paths("media-tv") == [tmp_path / "remote/media/tv"]
    assert layout.transfer_source("media-tv") == tmp_path / "local/media-tv"
    assert layout.api_socket == tmp_path / "run/api.sock"
    assert layout.rc_socket("media-tv") == tmp_path / "run/units/media-tv/rc.sock"
    assert resolved(cfg, "media-tv") == (1000, 984, "10G", True)
    assert not layout.check_paths()


def test_unit_overrides_defaults(make_config: Callable[..., Config]) -> None:
    cfg = make_config(
        units={"media-tv": {"pool": "main", "cache_max_size": "150G", "warm": False, "uid": 0}}
    )
    assert resolved(cfg, "media-tv") == (0, None, "150G", False)


def test_mount_paths_take_placeholders(make_config: Callable[..., Config]) -> None:
    cfg = make_config(
        hostname="pve-02",
        units={"tv": {"pool": "main", "mounts": [
            {"path": "/pool/{hostname}/{unit}"},
            {"path": "/srv/tv", "allow_other": False, "default_permissions": False},
        ]}},
        transfers={},
    )  # fmt: skip
    assert Layout(cfg).mount_paths("tv") == [Path("/pool/pve-02/tv"), Path("/srv/tv")]
    m = cfg.units["tv"].mounts[1]
    assert (m.allow_other, m.default_permissions) == (False, False)
    assert cfg.units["tv"].mounts[0].allow_other  # true unless said otherwise


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"units": {"u": {}}}, "drive units need a pool"),
        ({"units": {"u": {"pool": "nope"}}}, "unknown pool"),
        ({"units": {"u": {"pool": "main", "mounts": [{"path": "rel/tv"}]}}}, "absolute"),
        ({"units": {"u": {"pool": "main", "mounts": [{"path": "/a/../b"}]}}}, "absolute"),
        ({"units": {"u": {"pool": "main", "mounts": [{"path": "/a/{transfer}"}]}}},
         "not a placeholder here"),
        ({"units": {"u": {"pool": "main", "mounts": [{"path": "/a", "uid": 0}]}}},
         "Extra inputs"),  # VFS settings are per unit: a per-mount one would split the cache
        ({"transfers": {"t": {"unit": "missing"}}}, "unknown unit"),
        ({"transfers": {"t": {"unit": "media-tv", "source": "staging/tv"}}}, "absolute"),
        ({"transfers": {"t": {"unit": "media-tv", "schedule": "every day"}}}, "invalid cron"),
        ({"units": {"bad name": {"pool": "main"}}}, "invalid unit name"),
        ({"bogus": 1}, "Extra inputs"),
    ],
)  # fmt: skip
def test_rejects(make_config: Callable[..., Config], overrides: dict, message: str) -> None:  # type: ignore[type-arg]
    overrides.setdefault("transfers", {})
    with pytest.raises(ValidationError, match=message):
        make_config(**overrides)


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ({"roots": {"local": "/l", "remote": "/r"}}, "roots was removed in 0.2"),
        ({"units": {"u": {"mountpoint": "tv", "pool": "p"}}}, "mountpoint removed in 0.2"),
        ({"units": {"u": {"mount": False, "pool": "p"}}}, "mount removed in 0.2"),
        ({"transfers": {"t": {"unit": "u"}}}, "source is required since 0.2"),
    ],
)
def test_removed_keys_name_their_replacement(data: dict, message: str) -> None:  # type: ignore[type-arg]
    with pytest.raises(ValidationError, match=message):
        Config.model_validate(data)


def test_path_checks(make_config: Callable[..., Config], tmp_path: Path) -> None:
    def problems(units: dict, transfers: dict | None = None) -> list[str]:  # type: ignore[type-arg]
        units = {n: {"pool": "main", "mounts": [{"path": p} for p in ps]}
                 for n, ps in units.items()}  # fmt: skip
        return Layout(make_config(units=units, transfers=transfers or {})).check_paths()

    r = str(tmp_path / "remote")
    assert problems({"a": [f"{r}/a", f"{r}/a2"], "b": [f"{r}/b"]}) == []
    assert "system directory" in problems({"a": ["/etc/tv"]})[0]
    assert "system directory" in problems({"a": ["/mnt"]})[0]
    assert problems({"a": ["/mnt/tv"]}) == []  # below a top-level directory is fine
    assert "overlaps ferryman's own" in problems({"a": [str(tmp_path / "cache/x")]})[0]
    assert "overlaps ferryman's own" in problems({"a": [str(tmp_path)]})[0]
    assert problems({"a": [f"{r}/a"], "b": [f"{r}/a"]}) == [f"units a and b both mount {r}/a"]
    assert problems({"a": [f"{r}/a", f"{r}/a"]}) == [f"unit a mounts {r}/a twice"]
    assert "is inside unit a's mount" in problems({"a": [f"{r}/a"], "b": [f"{r}/a/b"]})[0]
    inside = {"t": {"unit": "a", "source": f"{r}/a/staging"}}
    assert "from Drive to Drive" in problems({"a": [f"{r}/a"]}, inside)[0]
    above = {"t": {"unit": "a", "source": r}}
    assert "walk into the mount" in problems({"a": [f"{r}/a"]}, above)[0]


def test_load_yaml(tmp_path: Path) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(
        "units:\n  docs:\n    backend: alias\n    options: {remote: /srv}\n"
        "    mounts: [{path: /pool/docs}]\n"
    )
    cfg = Config.load(path)
    assert cfg.units["docs"].options == {"remote": "/srv"}
    assert cfg.units["docs"].mounts[0].path == "/pool/docs"
    assert cfg.paths.runtime == Path("/run/ferryman")


def test_transfer_dest_placeholders_and_sync_options(make_config: Callable[..., Config]) -> None:
    t = {"unit": "media-tv", "dest": "b/{hostname}"}
    cfg = make_config(hostname="pve-02", transfers={"t": t})
    assert Layout(cfg).transfer_dest("t") == "b/pve-02"
    for bad in ("b/{host}", "b/{0}", "b/{hostname"):
        with pytest.raises(ValueError, match="dest"):
            make_config(transfers={"t": {"unit": "media-tv", "dest": bad}})
    with pytest.raises(ValueError, match="sync only"):
        make_config(transfers={"t": {"unit": "media-tv", "max_delete": 5}})
    with pytest.raises(ValueError, match="sync only"):
        make_config(transfers={"t": {"unit": "media-tv", "mode": "copy", "allow_empty": True}})


EXAMPLE = Path(__file__).parent.parent / "contrib/config.example.yaml"


def test_example_config_is_valid_and_complete() -> None:
    """contrib/config.example.yaml loads, shows the global sections at their
    defaults, and mentions every setting, so it cannot fall behind the code."""
    import yaml
    from pydantic import BaseModel

    from ferryman.config import Mount, Pool, Transfer, Unit

    cfg = Config.load(EXAMPLE)
    assert not Layout(cfg).check_paths()
    for section in ("paths", "api", "rclone", "health", "notify", "defaults", "tuning"):
        value = getattr(cfg, section)
        assert value == type(value)(), f"{section} differs from its defaults"
    assert cfg.hostname is None and cfg.coordinator is None

    raw = yaml.safe_load(EXAMPLE.read_text())

    def names(model: type[BaseModel]) -> set[str]:
        return {f.alias or n for n, f in model.model_fields.items()}

    assert set(raw) == names(Config)
    for section in ("paths", "api", "rclone", "health", "notify", "defaults", "tuning"):
        model = type(getattr(cfg, section))
        assert set(raw[section]) == names(model), section
    for key, model in (("pools", Pool), ("units", Unit), ("transfers", Transfer)):
        shown = set().union(*(set(v) for v in raw[key].values()))
        assert shown == names(model), f"{key}: missing {names(model) - shown}"
    mounts = [m for u in raw["units"].values() for m in u.get("mounts") or []]
    assert set().union(*(set(m) for m in mounts)) == names(Mount)


# ---------------------------------------------------------- read settings


def test_read_settings_built_in_defaults_and_unit(make_config: Callable[..., Config]) -> None:
    from ferryman.config import read_settings

    cfg = make_config(
        defaults={"buffer_size": "8M", "vfs_cache_mode": "full"},  # full restates the built-in
        units={"media-tv": {"mounts": [{"path": "/pool/tv"}], "pool": "main",
                            "buffer_size": "0", "vfs_read_chunk_size": "4M"}},
    )  # fmt: skip
    assert read_settings(cfg, "media-tv") == {
        "buffer_size": {"value": "0", "source": "unit"},
        "vfs_cache_mode": {"value": "full", "source": "built-in"},
        "vfs_read_chunk_size": {"value": "4M", "source": "unit"},
        "vfs_read_ahead": {"value": "0", "source": "built-in"},
    }


@pytest.mark.parametrize(
    ("override", "error"),
    [
        ({"defaults": {"buffer_size": "16777216"}}, "needs a unit suffix"),
        ({"units": {"m": {"mounts": [{"path": "/m"}], "pool": "main",
                          "vfs_read_chunk_size": "4096"}}},
         "needs a unit suffix"),
        ({"units": {"m": {"mounts": [{"path": "/m"}], "pool": "main", "vfs_cache_mode": "most"}}},
         "vfs_cache_mode"),
        ({"units": {"m": {"mounts": [{"path": "/m"}], "pool": "main", "vfs_cache_mode": "writes",
                          "vfs_read_ahead": "64M"}}}, "only applies with vfs_cache_mode: full"),
        ({"defaults": {"vfs_read_ahead": "64M"},
          "units": {"m": {"mounts": [{"path": "/m"}], "pool": "main",
                          "vfs_cache_mode": "minimal"}}},
         "only applies with vfs_cache_mode: full"),
        ({"units": {"m": {"pool": "main",
                          "buffer_size": "0"}}}, "only apply to mounts"),
    ],
)  # fmt: skip
def test_read_settings_are_validated(
    make_config: Callable[..., Config], override: dict[str, object], error: str
) -> None:
    with pytest.raises(ValidationError, match=error):
        make_config(**override)


def test_mount_argv_carries_read_settings(make_config: Callable[..., Config]) -> None:
    from ferryman.unit import UnitSupervisor

    cfg = make_config(
        units={"media-tv": {"mounts": [{"path": "/pool/tv"}], "pool": "main",
                            "buffer_size": "0", "vfs_read_chunk_size": "4M",
                            "mount_args": ["--buffer-size=1M"]}},
    )  # fmt: skip
    argv = UnitSupervisor("media-tv", cfg, Layout(cfg), None).argv()

    def flag(name: str) -> str:
        return argv[argv.index(name) + 1]

    assert flag("--buffer-size") == "0"
    assert flag("--vfs-cache-mode") == "full"
    assert flag("--vfs-read-chunk-size") == "4M"
    assert flag("--vfs-read-ahead") == "0"
    # mount_args still come last, so they still win.
    assert argv[-1] == "--buffer-size=1M"


def test_bare_off_cache_mode_means_off(tmp_path: Path) -> None:
    # YAML 1.1 turns a bare `off` into false.
    path = tmp_path / "config.yaml"
    path.write_text(
        "pools: {p: {keys_dir: /k, keys: [a]}}\n"
        "defaults: {vfs_cache_mode: off}\n"
        "units:\n  m: {mounts: [{path: /pool/m}], pool: p, vfs_cache_mode: off}\n"
        "  n: {mounts: [{path: /pool/n}], pool: p}\n"
    )
    cfg = Config.load(path)
    assert cfg.defaults.vfs_cache_mode == "off"
    assert cfg.units["m"].vfs_cache_mode == "off"
    path.write_text(path.read_text().replace("vfs_cache_mode: off}", "vfs_cache_mode: on}"))
    with pytest.raises(ValidationError, match="vfs_cache_mode"):
        Config.load(path)  # true has no meaning: still an error


# ------------------------------------------------------------ read-only mounts


def test_the_first_writable_mount_is_the_processs_own(
    make_config: Callable[..., Config], tmp_path: Path
) -> None:
    from ferryman.unit import UnitSupervisor

    def unit(*mounts: dict) -> UnitSupervisor:  # type: ignore[type-arg]
        cfg = make_config(units={"tv": {"pool": "main", "mounts": list(mounts)}}, transfers={})
        return UnitSupervisor("tv", cfg, Layout(cfg), None)

    sup = unit({"path": "/a", "read_only": True}, {"path": "/b"}, {"path": "/c"})
    assert sup.unit.primary == 1 and sup.primary_path == Path("/b")
    argv = sup.argv()
    assert argv[2:4] == ["tv:", "/b"] and "--read-only" not in argv
    kinds = [(m["path"], m["kind"], m["primary"]) for m in sup.status()["mounts"]]
    assert kinds == [("/a", "bind", False), ("/b", "rclone", True), ("/c", "rc", False)]

    # All read-only: the first is rclone's own, read-only as a whole; the rest bind it.
    sup = unit({"path": "/a", "read_only": True}, {"path": "/b", "read_only": True})
    assert sup.unit.primary == 0 and "--read-only" in sup.argv()
    assert [m["kind"] for m in sup.status()["mounts"]] == ["rclone", "bind"]


def test_a_bind_cannot_differ_from_the_mount_it_binds(make_config: Callable[..., Config]) -> None:
    units = {"tv": {"pool": "main", "mounts": [
        {"path": "/a", "allow_other": False},
        {"path": "/b", "read_only": True},  # allow_other defaults to true: differs
    ]}}  # fmt: skip
    with pytest.raises(ValidationError, match="binds /a, so its allow_other is /a's"):
        make_config(units=units, transfers={})
    units["tv"]["mounts"][1]["allow_other"] = False  # type: ignore[index]
    make_config(units=units, transfers={})


async def test_read_only_without_root_fails_that_mount_only(
    make_config: Callable[..., Config], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    from ferryman.unit import UnitSupervisor

    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    cfg = make_config(units={"tv": {"pool": "main", "mounts": [
        {"path": str(tmp_path / "a")}, {"path": str(tmp_path / "b"), "read_only": True}]}},
        transfers={})  # fmt: skip
    sup = UnitSupervisor("tv", cfg, Layout(cfg), None)
    events: list[str] = []
    sup.emit = lambda e: events.append(e.kind)
    mount, path = sup._joined()[0]
    assert not await sup.join(mount, path)
    assert "need root" in sup.mount_states[str(path)]
    assert events == ["mount.lost"]
