"""`ferryman migrate`: 0.2 configs to the controller's config and each
host's agent.yaml, running every unit as 0.2 did."""

from __future__ import annotations

import copy
import hashlib
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from ferryman.cli import app
from ferryman.config import (
    POLICY_FIELDS,
    AgentBootstrap,
    ControllerConfig,
    UnitDefaults,
    effective_config,
    read_settings,
    resolved,
)
from ferryman.migrate import MigrateError, migrate

V02 = Path(__file__).parent / "data/config-0.2.example.yaml"
KEYS = ["sa-01@p.iam.gserviceaccount.com", "sa-02@p.iam.gserviceaccount.com"]


def v02() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(V02.read_text())
    return data


def host(units: dict[str, Any], **over: Any) -> dict[str, Any]:
    return {"pools": {"uploads": {"keys_dir": "/etc/ferryman/keys", "keys": KEYS}},
            "units": units, **over}  # fmt: skip


def run_agent(r: Any, name: str) -> Any:
    boot = AgentBootstrap.model_validate(r.agents[name])
    return effective_config(boot, ControllerConfig.model_validate(r.controller).slice(name))


def test_the_02_example_runs_as_it_did() -> None:
    old = v02()
    r = migrate({"pve-01": old}, controller_url="https://ferryman.example.org")
    cfg = run_agent(r, "pve-01")
    builtin = UnitDefaults().model_dump()
    d = old["defaults"]
    assert set(cfg.units) == set(old["units"]) and set(cfg.transfers) == set(old["transfers"])
    for name, u in old["units"].items():
        new = cfg.units[name]
        assert [m.path for m in new.mounts] == [m["path"] for m in u.get("mounts", [])]
        assert len(new.serve) == len(u.get("serve", []))
        assert (new.drive_id, new.pool, new.backend) == (
            u.get("drive_id"), u.get("pool"), u.get("backend", "drive"))  # fmt: skip
        # Read settings and host settings resolve to what 0.2 resolved.
        for k, v in read_settings(cfg, name).items():
            assert v["value"] == u.get(k, d.get(k, builtin[k])), (name, k)
        uid, gid, cache, warm = resolved(cfg, name)
        assert (uid, gid) == (u.get("uid", d["uid"]), u.get("gid", d["gid"]))
        assert cache == (u.get("cache_max_size") or d["cache_max_size"])
        assert warm == u.get("warm", d["warm"])
    for name, t in old["transfers"].items():
        assert cfg.transfers[name].source == t["source"]
    # A host with no coordinator gets a token; its hash is in the config.
    token = r.tokens["pve-01"]
    assert r.controller["agents"]["pve-01"]["token_sha256"] == hashlib.sha256(
        token.encode()).hexdigest()  # fmt: skip
    assert r.agents["pve-01"]["controller"]["url"] == "https://ferryman.example.org"
    assert any("copy its key files" in n for n in r.notes)


def test_one_unit_on_two_hosts_is_one_unit_and_two_bindings() -> None:
    tv = {"drive_id": "0A", "pool": "uploads"}
    r = migrate(
        {
            "pve-01": host({"tv": {**tv, "mounts": [{"path": "/pool/tv"}]}},
                           defaults={"buffer_size": "64M", "cache_min_free_space": "200G"}),
            "emby-01": host({"tv": {**tv, "uid": 1001,
                                    "mounts": [{"path": "/media/tv", "read_only": True}]}}),
        },
        controller_url="http://c",
    )  # fmt: skip
    c = r.controller
    assert c["units"] == {"tv": {"pool": "uploads", "drive_id": "0A"}}
    assert c["agents"]["pve-01"]["units"]["tv"] == {"mounts": [{"path": "/pool/tv"}],
                                                    "buffer_size": "64M"}  # fmt: skip
    assert c["agents"]["pve-01"]["defaults"] == {"cache_min_free_space": "200G"}
    assert c["agents"]["emby-01"]["units"]["tv"]["uid"] == 1001
    assert "buffer_size" not in c["agents"]["emby-01"]["units"]["tv"]
    assert any("read it differently (buffer_size)" in n for n in r.notes)
    assert read_settings(run_agent(r, "pve-01"), "tv")["buffer_size"]["value"] == "64M"
    assert read_settings(run_agent(r, "emby-01"), "tv")["buffer_size"]["value"] == "16M"


def test_policy_the_hosts_agree_on_goes_on_the_unit() -> None:
    tv = {"drive_id": "0A", "pool": "uploads", "vfs_read_chunk_size": "4M"}
    a, b = ({"tv": {**tv, "mounts": [{"path": f"/{h}/tv"}]}} for h in "ab")
    r = migrate({"a": host(a), "b": host(b)}, controller_url="http://c")
    assert r.controller["units"]["tv"]["vfs_read_chunk_size"] == "4M"
    assert r.controller["agents"]["a"]["units"]["tv"] == {"mounts": [{"path": "/a/tv"}]}


def test_a_unit_defined_differently_is_refused() -> None:
    with pytest.raises(MigrateError, match=r"unit tv: a and b define it differently \(drive_id\)"):
        migrate({"a": host({"tv": {"drive_id": "0A", "pool": "uploads"}}),
                 "b": host({"tv": {"drive_id": "0B", "pool": "uploads"}})},
                controller_url="http://c")  # fmt: skip


def test_hosts_without_a_coordinator_need_the_controllers_url() -> None:
    with pytest.raises(MigrateError, match="--controller-url"):
        migrate({"a": host({})})


def test_a_coordinator_setup_keeps_its_tokens_and_pools() -> None:
    coordinator = {
        "listen": {"host": "0.0.0.0", "port": 8470},
        "pools": {"shared": {"keys_dir": "/k", "keys": KEYS, "probe": {"drive_id": "0P"}}},
        "agents": {"a": {"token_sha256": "a" * 64, "pools": ["shared"]},
                   "gone": {"token_sha256": "b" * 64, "pools": ["shared"]}},
    }  # fmt: skip
    a = {"coordinator": {"url": "https://co", "token_file": "/etc/ferryman/token"},
         "pools": {"shared": {"coordinator": True}},
         "units": {"tv": {"drive_id": "0A", "pool": "shared"}}}  # fmt: skip
    r = migrate({"a": a}, coordinator)
    assert r.controller["agents"]["a"]["token_sha256"] == "a" * 64 and r.tokens == {}
    assert r.controller["pools"]["shared"]["probe"] == {"drive_id": "0P"}
    assert r.agents["a"]["controller"] == {"url": "https://co", "token_file": "/etc/ferryman/token"}
    assert r.controller["state"] == "/var/lib/ferryman-controller"
    assert any("mv /var/lib/ferryman-coordinator" in n for n in r.notes)
    assert any("agent gone" in n for n in r.notes)


def test_notify_becomes_channels_and_a_default_rule() -> None:
    mail = {"to": ["ops@x"], "sendmail": "/usr/sbin/sendmail"}
    r = migrate(
        {
            "a": host({}, notify={"level": "error", "mail": mail}),
            "b": host({}, notify={"level": "warning", "mail": mail,
                                  "webhook": {"url": "https://hook"}}),
        },
        controller_url="http://c",
    )  # fmt: skip
    n = r.controller["notify"]
    assert set(n["channels"]) == {"mail", "webhook"}  # the same mail once
    assert n["rules"] == [{"name": "default", "match": {"severity": "warning"},
                           "send": ["mail", "webhook"]}]  # fmt: skip
    assert any("levels differed" in x for x in r.notes)


def test_tuning_differences_stay_per_agent() -> None:
    hosts = {"a": host({}, tuning={"uplink": "home"}), "b": host({}, tuning={"uplink": "office"})}
    r = migrate(hosts, controller_url="http://c")
    assert r.controller["tuning"] == {"uplink": "home"}
    assert r.controller["agents"]["b"]["tuning"] == {"uplink": "office"}
    assert "tuning" not in r.controller["agents"]["a"]


def test_the_cli_writes_the_configs(tmp_path: Path) -> None:
    old = copy.deepcopy(v02())
    (tmp_path / "pve-01.yaml").write_text(yaml.safe_dump(old))
    out = tmp_path / "out"
    r = CliRunner().invoke(app, ["migrate", f"pve-01={tmp_path / 'pve-01.yaml'}",
                                 "--controller-url", "http://c", "--out", str(out)])  # fmt: skip
    assert r.exit_code == 0, r.output
    ControllerConfig.load(out / "controller.yaml")
    AgentBootstrap.load(out / "agents/pve-01.yaml")
    assert stat.S_IMODE((out / "tokens/pve-01").stat().st_mode) == 0o600
    assert "install" in r.output and "note:" in r.output
    bad = CliRunner().invoke(app, ["migrate", "nonsense"])
    assert bad.exit_code == 1 and "HOST=CONFIG" in bad.output


def test_every_policy_field_is_migrated() -> None:
    """If a read setting is added, migrate must carry it (this fails first)."""
    assert set(POLICY_FIELDS) <= set(UnitDefaults.model_fields)


def test_a_01_config_resolves_its_roots() -> None:
    old = {
        "roots": {"local": "/pool/local", "remote": "/pool/remote"},
        "pools": {"uploads": {"keys_dir": "/etc/ferryman/keys", "keys": KEYS}},
        "units": {
            "media-tv": {"mountpoint": "media/tv", "drive_id": "0A", "pool": "uploads"},
            "backups": {"mountpoint": "backups", "drive_id": "0B", "pool": "uploads",
                        "mount": False},
        },
        "transfers": {
            "media-tv": {"unit": "media-tv"},                  # default: the unit's local dir
            "dumps": {"unit": "backups", "source": "dumps/{hostname}", "mode": "sync",
                      "dest": "d/{weekday}"},                  # relative: under roots.local
            "etc": {"unit": "backups", "source": "/etc", "mode": "copy", "dest": "configs"},
        },
    }  # fmt: skip
    r = migrate({"pve-01": old}, controller_url="http://c")
    cfg = run_agent(r, "pve-01")
    assert [m.path for m in cfg.units["media-tv"].mounts] == ["/pool/remote/media/tv"]
    assert cfg.units["backups"].mounts == []
    assert cfg.transfers["media-tv"].source == "/pool/local/media/tv"
    assert cfg.transfers["dumps"].source == "/pool/local/dumps/{hostname}"
    assert cfg.transfers["etc"].source == "/etc"
    assert any("its parent must exist" in n for n in r.notes)
