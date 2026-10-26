from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from pydantic import ValidationError

from ferryman.config import Config, Layout, resolved


def test_defaults_and_layout(make_config: Callable[..., Config], tmp_path: Path) -> None:
    cfg = make_config(defaults={"uid": 1000, "gid": 984})
    layout = Layout(cfg)
    assert layout.mount_path("media-tv") == tmp_path / "remote/media/tv"
    assert layout.transfer_source("media-tv") == tmp_path / "local/media/tv"
    assert layout.api_socket == tmp_path / "run/api.sock"
    assert layout.rc_socket("media-tv") == tmp_path / "run/units/media-tv/rc.sock"
    assert resolved(cfg, "media-tv") == (1000, 984, "10G", True)


def test_unit_overrides_defaults(make_config: Callable[..., Config]) -> None:
    cfg = make_config(
        units={
            "media-tv": {
                "mountpoint": "media/tv",
                "pool": "main",
                "cache_max_size": "150G",
                "warm": False,
                "uid": 0,
            }
        }
    )
    assert resolved(cfg, "media-tv") == (0, None, "150G", False)


def test_relative_transfer_source(make_config: Callable[..., Config], tmp_path: Path) -> None:
    cfg = make_config(transfers={"t": {"unit": "media-tv", "source": "staging/tv"}})
    assert Layout(cfg).transfer_source("t") == tmp_path / "local/staging/tv"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"units": {"u": {"mountpoint": "x"}}}, "drive units need a pool"),
        ({"units": {"u": {"mountpoint": "x", "pool": "nope"}}}, "unknown pool"),
        ({"units": {"u": {"mountpoint": "/abs", "pool": "main"}}}, "relative path"),
        ({"units": {"u": {"mountpoint": "../up", "pool": "main"}}}, "relative path"),
        (
            {
                "units": {
                    "a": {"mountpoint": "m", "pool": "main"},
                    "b": {"mountpoint": "m", "pool": "main"},
                }
            },
            "share mountpoint",
        ),
        ({"transfers": {"t": {"unit": "missing"}}}, "unknown unit"),
        ({"transfers": {"t": {"unit": "media-tv", "schedule": "every day"}}}, "invalid cron"),
        ({"units": {"bad name": {"mountpoint": "x", "pool": "main"}}}, "invalid unit name"),
        ({"bogus": 1}, "Extra inputs"),
    ],
)
def test_rejects(make_config: Callable[..., Config], overrides: dict, message: str) -> None:  # type: ignore[type-arg]
    overrides.setdefault("transfers", {})
    with pytest.raises(ValidationError, match=message):
        make_config(**overrides)


def test_transfer_only_units_may_share_mountpoint(make_config: Callable[..., Config]) -> None:
    make_config(
        units={
            "a": {"mountpoint": "m", "pool": "main"},
            "b": {"mountpoint": "m", "pool": "main", "mount": False},
        },
        transfers={},
    )


def test_load_yaml(tmp_path: Path) -> None:
    path = tmp_path / "c.yaml"
    path.write_text(
        "roots: {local: /l, remote: /r}\n"
        "units:\n  docs: {mountpoint: docs, backend: alias, options: {remote: /srv}}\n"
    )
    cfg = Config.load(path)
    assert cfg.units["docs"].options == {"remote": "/srv"}
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

    from ferryman.config import Pool, Transfer, Unit

    cfg = Config.load(EXAMPLE)
    assert not Layout(cfg).check_placeholders()
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


# ---------------------------------------------------------- read settings


def test_read_settings_built_in_defaults_and_unit(make_config: Callable[..., Config]) -> None:
    from ferryman.config import read_settings

    cfg = make_config(
        defaults={"buffer_size": "8M", "vfs_cache_mode": "full"},  # full restates the built-in
        units={"media-tv": {"mountpoint": "media/tv", "pool": "main",
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
        ({"units": {"m": {"mountpoint": "m", "pool": "main", "vfs_read_chunk_size": "4096"}}},
         "needs a unit suffix"),
        ({"units": {"m": {"mountpoint": "m", "pool": "main", "vfs_cache_mode": "most"}}},
         "vfs_cache_mode"),
        ({"units": {"m": {"mountpoint": "m", "pool": "main", "vfs_cache_mode": "writes",
                          "vfs_read_ahead": "64M"}}}, "only applies with vfs_cache_mode: full"),
        ({"defaults": {"vfs_read_ahead": "64M"},
          "units": {"m": {"mountpoint": "m", "pool": "main", "vfs_cache_mode": "minimal"}}},
         "only applies with vfs_cache_mode: full"),
        ({"units": {"m": {"mountpoint": "m", "pool": "main", "mount": False,
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
        units={"media-tv": {"mountpoint": "media/tv", "pool": "main",
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
        "roots: {local: /l, remote: /r}\n"
        "pools: {p: {keys_dir: /k, keys: [a]}}\n"
        "defaults: {vfs_cache_mode: off}\n"
        "units:\n  m: {mountpoint: m, pool: p, vfs_cache_mode: off}\n"
        "  n: {mountpoint: n, pool: p}\n"
    )
    cfg = Config.load(path)
    assert cfg.defaults.vfs_cache_mode == "off"
    assert cfg.units["m"].vfs_cache_mode == "off"
    path.write_text(path.read_text().replace("vfs_cache_mode: off}", "vfs_cache_mode: on}"))
    with pytest.raises(ValidationError, match="vfs_cache_mode"):
        Config.load(path)  # true has no meaning: still an error
