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
