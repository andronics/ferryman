"""The controller's config: units independent of agents, bindings, the slice
each agent gets, and what the controller refuses at load."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel, ValidationError

from ferryman.config import (
    AgentBootstrap,
    AgentSpec,
    Binding,
    ControllerConfig,
    ControllerRef,
    HostDefaults,
    Layout,
    Mount,
    Probe,
    Serve,
    Transfer,
    UnitSpec,
    effective_config,
    read_settings,
    resolved,
)
from ferryman.controller import Controller
from tests.conftest import KEYS

TOKEN = "0" * 63
BOOT = AgentBootstrap(controller=ControllerRef(url="http://c", token_file=Path("/t")))


def base(**over: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "pools": {"uploads": {"keys_dir": "/k", "keys": KEYS}},
        "units": {
            "tv": {"drive_id": "0TV", "pool": "uploads", "vfs_read_chunk_size": "64M"},
            "films": {"drive_id": "0FILMS", "pool": "uploads"},
        },
        "agents": {
            "pve-01": {
                "token_sha256": TOKEN + "1",
                "defaults": {"uid": 1000, "cache_max_size": "50G"},
                "units": {"tv": {"mounts": [{"path": "/pool/google/tv"}]}},
                "transfers": {"tv-up": {"unit": "tv", "source": "/pool/incoming/tv"}},
            },
            "emby-01": {
                "token_sha256": TOKEN + "2",
                "units": {
                    "tv": {"mounts": [{"path": "/media/tv", "read_only": True}],
                           "uid": 1001, "buffer_size": "64M"},
                },
            },
        },
    }  # fmt: skip
    data.update(over)
    return data


def load(data: dict[str, Any]) -> ControllerConfig:
    return ControllerConfig.model_validate(data)


def agent_cfg(data: dict[str, Any], agent: str) -> Any:
    return effective_config(BOOT, load(data).slice(agent))


# ------------------------------------------------------------ the model


def test_one_unit_bound_differently_on_two_agents() -> None:
    pve, emby = agent_cfg(base(), "pve-01"), agent_cfg(base(), "emby-01")
    assert [m.path for m in pve.units["tv"].mounts] == ["/pool/google/tv"]
    assert [(m.path, m.read_only) for m in emby.units["tv"].mounts] == [("/media/tv", True)]
    # Both run the same remote.
    assert pve.units["tv"].drive_id == emby.units["tv"].drive_id == "0TV"
    assert pve.pools["uploads"].controller and emby.pools["uploads"].controller
    assert list(pve.transfers) == ["tv-up"] and emby.transfers == {}


def test_read_policy_precedence_and_where_it_came_from() -> None:
    data = base(defaults={"buffer_size": "8M"})
    pve, emby = agent_cfg(data, "pve-01"), agent_cfg(data, "emby-01")
    r = read_settings(pve, "tv")
    assert r["vfs_read_chunk_size"] == {"value": "64M", "source": "unit"}
    assert r["buffer_size"] == {"value": "8M", "source": "defaults"}
    assert r["vfs_cache_mode"] == {"value": "full", "source": "built-in"}
    # emby-01's binding overrides the unit's policy, for that host only.
    assert read_settings(emby, "tv")["buffer_size"] == {"value": "64M", "source": "binding"}


def test_host_settings_precedence() -> None:
    data = base(defaults={"uid": 500, "gid": 500, "cache_max_size": "10G"})
    pve, emby = agent_cfg(data, "pve-01"), agent_cfg(data, "emby-01")
    # controller defaults < agent defaults < binding
    assert resolved(pve, "tv")[:3] == (1000, 500, "50G")
    assert resolved(emby, "tv")[:3] == (1001, 500, "10G")


def test_an_agents_defaults_cannot_change_a_units_policy() -> None:
    data = base()
    data["agents"]["pve-01"]["defaults"]["buffer_size"] = "1G"
    with pytest.raises(ValidationError, match="buffer_size"):
        load(data)


def test_tuning_is_overridden_field_by_field() -> None:
    data = base(tuning={"budget": "8G", "uplink": "home"})
    data["agents"]["pve-01"]["tuning"] = {"uplink": "office"}
    pve, emby = agent_cfg(data, "pve-01"), agent_cfg(data, "emby-01")
    assert (pve.tuning.budget, pve.tuning.uplink) == ("8G", "office")
    assert (emby.tuning.budget, emby.tuning.uplink) == ("8G", "home")


def test_pools_come_from_the_units_bound() -> None:
    data = base()
    data["pools"]["archive"] = {"keys_dir": "/k2", "keys": KEYS}
    data["units"]["photos"] = {"drive_id": "0P", "pool": "archive"}
    data["agents"]["pve-01"]["units"]["photos"] = {}
    cfg = load(data)
    assert cfg.agent_pools("pve-01") == ["archive", "uploads"]
    assert cfg.agent_pools("emby-01") == ["uploads"]


# ------------------------------------------------------------- the slice


def test_slice_version_tracks_only_that_agents_slice() -> None:
    data = base()
    before = {a: load(data).slice(a)["version"] for a in ("pve-01", "emby-01")}
    assert load(data).slice("pve-01")["version"] == before["pve-01"]  # stable
    changed = copy.deepcopy(data)
    changed["agents"]["emby-01"]["units"]["tv"]["uid"] = 1002
    after = {a: load(changed).slice(a)["version"] for a in ("pve-01", "emby-01")}
    assert after["pve-01"] == before["pve-01"] and after["emby-01"] != before["emby-01"]
    # A change to a unit both bind reaches both.
    changed["units"]["tv"]["vfs_read_ahead"] = "0"
    assert load(changed).slice("pve-01")["version"] != before["pve-01"]


def test_slice_carries_no_key_material_and_a_schema() -> None:
    s = load(base()).slice("pve-01")
    assert s["schema"] == 1 and s["pools"] == ["uploads"]
    assert "keys" not in str(s) and "/k" not in str(s)


def test_an_agent_refuses_a_schema_it_does_not_know() -> None:
    s = load(base()).slice("pve-01")
    s["schema"] = 99
    with pytest.raises(ValueError, match="slice schema 99"):
        effective_config(BOOT, s)


def test_bootstrap_holds_only_the_host() -> None:
    AgentBootstrap.model_validate({"controller": {"url": "http://c", "token_file": "/t"},
                                   "hostname": "pve-01"})  # fmt: skip
    with pytest.raises(ValidationError, match="pools, units moved to the controller"):
        AgentBootstrap.model_validate(
            {"controller": {"url": "http://c", "token_file": "/t"}, "units": {}, "pools": {}}
        )


# ------------------------------------------------------- refused at load


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda d: d["agents"]["pve-01"]["units"].update(nope={}), "agent pve-01: unknown unit"),
        (
            lambda d: d["agents"]["emby-01"].update(
                transfers={
                    "x": {"unit": "tv", "source": "/x"},
                    "y": {"unit": "films", "source": "/y"},
                }
            ),
            "transfer y: unit 'films' is not bound",
        ),
        (lambda d: d["units"]["tv"].update(pool="nope"), "unit tv: unknown pool"),
        (lambda d: d["agents"]["emby-01"].update(token_sha256=TOKEN + "1"), "token shared"),
        # The agent's own checks, run on its slice: here an nfs server
        # without a cache mode, set through this host's binding.
        (
            lambda d: d["agents"]["emby-01"]["units"]["tv"].update(
                vfs_cache_mode="off", serve=[{"type": "nfs", "addr": "127.0.0.1:2049"}]
            ),
            "agent emby-01: .*nfs",
        ),
        # Two servers on one port, on one agent.
        (
            lambda d: d["agents"]["pve-01"]["units"]["tv"].update(
                serve=[
                    {"type": "http", "addr": "127.0.0.1:8080"},
                    {"type": "webdav", "addr": "127.0.0.1:8080"},
                ]
            ),
            "agent pve-01: .*8080",
        ),
        (
            lambda d: (
                d["pools"]["uploads"].update(probe={"drive_id": None})
                or d["units"].clear()
                or [a["units"].clear() or a.pop("transfers", None) for a in d["agents"].values()]
            ),
            "pool uploads: probe needs a drive_id",
        ),
    ],
)
def test_refused_at_load(change: Any, message: str) -> None:
    data = base()
    change(data)
    with pytest.raises(ValidationError, match=message):
        load(data)


def test_the_same_port_on_two_agents_is_fine() -> None:
    data = base()
    for a in ("pve-01", "emby-01"):
        data["agents"][a]["units"]["tv"]["serve"] = [{"type": "http", "addr": "127.0.0.1:8080"}]
    load(data)


# ---------------------------------------------------------- sync overlaps


def two_transfers(dest_a: str, mode_a: str, dest_b: str, mode_b: str,
                  unit_b: str = "tv") -> dict[str, Any]:  # fmt: skip
    data = base()
    data["agents"]["pve-01"]["transfers"] = {
        "a": {"unit": "tv", "source": "/a", "dest": dest_a, "mode": mode_a}
    }
    data["agents"]["emby-01"]["units"].setdefault(unit_b, {})
    data["agents"]["emby-01"]["transfers"] = {
        "b": {"unit": unit_b, "source": "/b", "dest": dest_b, "mode": mode_b}}  # fmt: skip
    return data


@pytest.mark.parametrize(
    ("args", "clash"),
    [
        (("backups", "sync", "backups/pve", "move"), True),  # sync contains a move
        (("backups/x", "move", "backups", "sync"), True),  # move inside a sync
        (("backups", "move", "backups", "move"), False),  # moves only add files
        (("backups/{hostname}", "sync", "backups/{hostname}", "sync"), False),  # per agent
        (("backups", "sync", "backups", "sync", "films"), False),  # different drives
        (("b/{env:SITE}", "sync", "b/x", "move"), True),  # unknown here: assume a clash
        (("tv/{weekday}", "sync", "tv/{weekday}", "copy"), True),  # cut at time: tv vs tv
    ],
)
def test_sync_overlaps_are_refused_at_load(args: tuple[str, ...], clash: bool) -> None:
    data = two_transfers(*args)
    if clash:
        with pytest.raises(ValidationError, match="overlap on"):
            load(data)
    else:
        load(data)


def test_a_sync_overlapping_a_transfer_on_its_own_agent_is_refused_too() -> None:
    data = base()
    data["agents"]["pve-01"]["transfers"] = {
        "mirror": {"unit": "tv", "source": "/m", "dest": "", "mode": "sync"},
        "inbox": {"unit": "tv", "source": "/i", "dest": "inbox"},
    }
    with pytest.raises(ValidationError, match=r"mirror .* inbox .*overlap"):
        load(data)


# ---------------------------------------------------------------- probes


def test_probe_drive_first_rotate_or_explicit(tmp_path: Path) -> None:
    data = base(state=str(tmp_path))
    data["pools"]["uploads"]["probe"] = {}
    assert load(data).probe_drives("uploads") == ["0TV", "0FILMS"]
    first = Controller(load(data))
    assert [first.probe_drive("uploads") for _ in range(3)] == ["0TV"] * 3
    data["pools"]["uploads"]["probe"] = {"drive": "rotate"}
    rot = Controller(load(data))
    assert [rot.probe_drive("uploads") for _ in range(3)] == ["0TV", "0FILMS", "0TV"]
    data["pools"]["uploads"]["probe"] = {"drive": "rotate", "drive_id": "0SCRATCH"}
    assert Controller(load(data)).probe_drive("uploads") == "0SCRATCH"


# ---------------------------------------------------------- the example


EXAMPLE = Path(__file__).parent.parent / "contrib/controller.example.yaml"


def test_controller_example_is_valid_and_complete() -> None:
    """contrib/controller.example.yaml loads and mentions every setting of
    the controller's own models, so it cannot fall behind the code."""
    cfg = ControllerConfig.load(EXAMPLE)
    for section in ("listen", "ui", "rclone", "events", "notify", "health", "tuning", "defaults"):
        value = getattr(cfg, section)
        assert value == type(value)(), f"{section} differs from its defaults"
    raw = yaml.safe_load(EXAMPLE.read_text())

    def names(model: type[BaseModel]) -> set[str]:
        return {f.alias or n for n, f in model.model_fields.items()}

    def shown(items: Any) -> set[str]:
        return set().union(*(set(v) for v in items))

    assert set(raw) == names(ControllerConfig)
    assert shown(raw["units"].values()) == names(UnitSpec)
    assert shown(raw["agents"].values()) == names(AgentSpec)
    assert shown(a["defaults"] for a in raw["agents"].values() if "defaults" in a) == names(
        HostDefaults
    )
    assert shown(b for a in raw["agents"].values() for b in a["units"].values()) == names(Binding)
    assert shown(p["probe"] for p in raw["pools"].values() if "probe" in p) == names(Probe)
    bindings = [b for a in raw["agents"].values() for b in a["units"].values()]
    assert shown(m for b in bindings for m in b.get("mounts") or []) == names(Mount)
    assert shown(sv for b in bindings for sv in b.get("serve") or []) == names(Serve)
    transfers = [t for a in raw["agents"].values() for t in (a.get("transfers") or {}).values()]
    assert shown(transfers) == names(Transfer)
    # As each agent would build and check it, short of files on this machine
    # (the example's secrets and certs).
    for agent in cfg.agents:
        flat = effective_config(BOOT, cfg.slice(agent))
        problems = Layout(flat).check_paths()
        assert [p for p in problems if not p.endswith("does not exist")] == [], agent
    assert cfg.agent_pools("emby-01") == ["uploads"]
