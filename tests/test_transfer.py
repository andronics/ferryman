"""Transfers, rotation and health against a fake rclone rc server."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest

from ferryman import mountutil
from ferryman.agent import Agent
from ferryman.config import Config
from ferryman.patterns import match
from ferryman.unit import State
from tests.conftest import KEYS, FakeRclone, serve_unix

QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"


async def test_move_succeeds(agent: Agent, fake_rclone: FakeRclone, tmp_path: Path) -> None:
    result = await agent.transfers["media-tv"].run()
    assert result["ok"] and result["files"] == 3 and result["bytes"] == 1024
    endpoint, params = next(c for c in fake_rclone.calls if c[0].startswith("sync/"))
    assert endpoint == "sync/move"
    assert params["srcFs"] == str(tmp_path / "local/media-tv")
    assert params["dstFs"] == "media-tv:"
    assert params["deleteEmptySrcDirs"] is True
    assert params["_group"] == "transfer-media-tv"
    assert params["_config"] == {"Transfers": 8, "Checkers": 8, "BufferSize": "16M",
                                 "DryRun": False}  # fmt: skip
    # The group is reset before the run so its stats are this run's only.
    assert fake_rclone.endpoints().index("core/stats-reset") < fake_rclone.endpoints().index(
        "sync/move"
    )


async def test_cap_hit_rotates_and_resubmits(agent: Agent, fake_rclone: FakeRclone) -> None:
    fake_rclone.job_outcomes = [QUOTA, None]
    result = await agent.transfers["media-tv"].run()
    assert result["ok"]
    assert fake_rclone.endpoints().count("sync/move") == 2
    sets = [p for e, p in fake_rclone.calls if e == "backend/command"]
    assert len(sets) == 1
    assert sets[0]["fs"] == "media-tv:" and sets[0]["command"] == "set"
    assert sets[0]["opt"]["service_account_file"].endswith(f"{KEYS[1]}.json")
    assert agent.pools["main"].current() == KEYS[1]
    assert agent.units["media-tv"].key == KEYS[1]
    assert KEYS[1] in agent.layout.rclone_conf("media-tv").read_text()


async def test_cap_hit_on_every_key_fails(agent: Agent, fake_rclone: FakeRclone) -> None:
    fake_rclone.job_outcomes = [QUOTA, QUOTA, QUOTA]
    result = await agent.transfers["media-tv"].run()
    assert not result["ok"]
    assert "exhausted" in result["error"]


async def test_other_errors_do_not_rotate(agent: Agent, fake_rclone: FakeRclone) -> None:
    fake_rclone.job_outcomes = ["directory not found"]
    result = await agent.transfers["media-tv"].run()
    assert not result["ok"] and "directory not found" in result["error"]
    assert "backend/command" not in fake_rclone.endpoints()
    assert agent.pools["main"].current() == KEYS[0]


async def test_missing_source_is_nothing_to_do(
    agent: Agent, fake_rclone: FakeRclone, tmp_path: Path
) -> None:
    (tmp_path / "local/media-tv").rmdir()
    result = await agent.transfers["media-tv"].run()
    assert result["ok"] and "does not exist" in result["skipped"]
    assert fake_rclone.calls == []


async def test_unit_not_ready_fails(agent: Agent) -> None:
    agent.units["media-tv"].state = State.BACKOFF
    result = await agent.transfers["media-tv"].run()
    assert not result["ok"] and "not ready" in result["error"]


async def test_dry_run_is_passed_through(agent: Agent, fake_rclone: FakeRclone) -> None:
    await agent.transfers["media-tv"].run(dry_run=True)
    params = next(p for e, p in fake_rclone.calls if e == "sync/move")
    assert params["_config"]["DryRun"] is True


async def test_health_rotates_on_unhandled_cap_hit(agent: Agent, fake_rclone: FakeRclone) -> None:
    fake_rclone.last_error = QUOTA
    report = await agent.check_health()
    assert report == {"media-tv": "healthy"}
    assert agent.pools["main"].current() == KEYS[1]
    assert fake_rclone.endpoints()[-1] == "core/stats-reset"


async def test_health_leaves_cap_hits_to_a_running_transfer(
    agent: Agent, fake_rclone: FakeRclone
) -> None:
    fake_rclone.last_error = QUOTA
    async with agent.transfers["media-tv"].lock:
        await agent.check_health()
    assert agent.pools["main"].current() == KEYS[0]


async def test_stats_view(agent: Agent) -> None:
    rows = await agent.stats("media-*")
    assert rows == [{"unit": "media-tv", "speed": 0, "bytes": 1024, "files": 3,
                     "errors": 0, "last_error": None}]  # fmt: skip


def test_patterns() -> None:
    names = ["media-tv", "media-movies", "arc-code"]
    assert match("", names) == names
    assert match("media-*", names) == ["media-tv", "media-movies"]
    assert match("arc-code, media-tv", names) == ["media-tv", "arc-code"]
    assert match("nope", names) == []


async def test_warm_follows_the_refresh_job(agent: Agent, fake_rclone: FakeRclone) -> None:
    sup = agent.units["media-tv"]
    await sup._warm()
    assert sup.warm is not None and sup.warm["state"] == "done" and sup.warm["error"] is None
    assert fake_rclone.endpoints() == ["vfs/refresh", "job/status"]
    assert fake_rclone.calls[0][1]["recursive"] == "true"
    assert sup.status()["warm"] == sup.warm


async def test_warm_retries_once_on_directory_errors(agent: Agent, fake_rclone: FakeRclone) -> None:
    sup = agent.units["media-tv"]
    sup.warm_retry_delay = 0
    fake_rclone.refresh_results = [{"": "context deadline exceeded"}, {"": "OK"}]
    await sup._warm()
    assert sup.warm is not None and sup.warm["state"] == "done"
    assert fake_rclone.endpoints().count("vfs/refresh") == 2

    fake_rclone.refresh_results = [{"tv": "file does not exist"}] * 2
    await sup._warm()
    assert sup.warm["state"] == "failed"
    assert sup.warm["error"] == "tv: file does not exist"
    assert fake_rclone.endpoints().count("vfs/refresh") == 4


def test_make_mountpoint_creates_only_the_last_directory(tmp_path: Path) -> None:
    (tmp_path / "remote").mkdir(mode=0o700)
    target = tmp_path / "remote/tv"
    uid, gid = os.getuid(), os.getgid()
    mountutil.make_mountpoint(target, uid, gid, 0o2775)
    st = target.stat()
    assert (st.st_uid, st.st_gid, st.st_mode & 0o7777) == (uid, gid, 0o2775)
    # The parent is not touched, and a missing one is an error, not a mkdir -p:
    # a typo in an absolute path must not build a tree at the filesystem root.
    assert (tmp_path / "remote").stat().st_mode & 0o7777 == 0o700
    with pytest.raises(FileNotFoundError, match="does not exist"):
        mountutil.make_mountpoint(tmp_path / "typo/tv", uid, gid, 0o2775)
    assert not (tmp_path / "typo").exists()
    mountutil.make_mountpoint(target, uid, gid, 0o2775)  # idempotent


async def test_dest_expands_hostname_and_options_reach_the_job(
    make_config: Callable[..., Config], fake_rclone: FakeRclone, tmp_path: Path
) -> None:
    cfg = make_config(
        hostname="pve-01",
        units={"backups": {"pool": "main"}},
        transfers={"dump": {"unit": "backups", "mode": "sync", "source": str(tmp_path / "dump"),
                            "dest": "proxmox/{hostname}/dump", "exclude": ["*.tmp"],
                            "min_age": "10m", "max_delete": 20}},
    )  # fmt: skip
    agent = Agent(cfg)
    agent.job_poll_interval = 0.01
    sup = agent.units["backups"]
    sup.layout.runtime_dir("backups").mkdir(parents=True)
    sup.state = State.READY
    (tmp_path / "dump").mkdir()
    (tmp_path / "dump/vzdump-qemu-100.vma.zst").write_text("x")
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("backups")):
        result = await agent.transfers["dump"].run()
    assert result["ok"], result
    endpoint, params = next(c for c in fake_rclone.calls if c[0].startswith("sync/"))
    assert endpoint == "sync/sync"
    assert params["dstFs"] == "backups:proxmox/pve-01/dump"
    assert params["_filter"] == {"ExcludeRule": ["*.tmp"], "MinAge": "10m"}
    assert params["_config"]["MaxDelete"] == 20
    assert "deleteEmptySrcDirs" not in params
    assert agent.transfers["dump"].status()["dest"] == "proxmox/pve-01/dump"


async def test_sync_from_an_empty_source_is_refused(
    make_config: Callable[..., Config], fake_rclone: FakeRclone, tmp_path: Path
) -> None:
    def build(allow_empty: bool) -> Agent:
        t = {"unit": "media-tv", "mode": "sync", "allow_empty": allow_empty}
        agent = Agent(make_config(transfers={"mirror": t}))
        agent.job_poll_interval = 0.01
        agent.units["media-tv"].state = State.READY
        return agent

    (tmp_path / "local/mirror").mkdir(parents=True)
    agent = build(allow_empty=False)
    result = await agent.transfers["mirror"].run()
    assert not result["ok"] and "is empty" in result["error"]
    assert fake_rclone.calls == []

    agent = build(allow_empty=True)
    agent.layout.runtime_dir("media-tv").mkdir(parents=True)
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("media-tv")):
        result = await agent.transfers["mirror"].run()
    assert result["ok"], result


async def test_a_run_keeps_one_dest_across_rotation_retries(
    make_config: Callable[..., Config], fake_rclone: FakeRclone, tmp_path: Path
) -> None:
    cfg = make_config(units={"media-tv": {"pool": "main", "drive_id": "0A"}},
                      transfers={"media-tv": {"unit": "media-tv",
                                              "dest": "in/{date}/{transfer}"}})  # fmt: skip
    agent = Agent(cfg)
    agent.job_poll_interval = 0.01
    sup = agent.units["media-tv"]
    sup.layout.runtime_dir("media-tv").mkdir(parents=True)
    lease = await agent.pools["main"].lease()
    sup.key, sup.key_path = lease.key, lease.path
    sup.state = State.READY
    (tmp_path / "local/media-tv").mkdir(parents=True)
    fake_rclone.job_outcomes = [QUOTA, None]
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("media-tv")):
        result = await agent.transfers["media-tv"].run()
    assert result["ok"], result
    dests = [p["dstFs"] for e, p in fake_rclone.calls if e.startswith("sync/")]
    today = datetime.now().strftime("%Y-%m-%d")
    assert dests == [f"media-tv:in/{today}/media-tv"] * 2
    assert result["dest"] == f"in/{today}/media-tv"
