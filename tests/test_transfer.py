"""Transfers, rotation and health against a fake rclone rc server."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest

from ferryman.agent import Agent
from ferryman.config import Config
from ferryman.patterns import match
from ferryman.unit import State
from tests.conftest import KEYS, FakeRclone, serve_unix

QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"


@pytest.fixture
async def agent(
    make_config: Callable[..., Config], fake_rclone: FakeRclone, tmp_path: Path
) -> AsyncIterator[Agent]:
    """An agent whose media-tv unit is 'ready' on the fake rc server, with
    something staged to move."""
    # mount: false - these tests drive the rc API only; real mounts are
    # covered by test_integration.
    agent = Agent(make_config(units={"media-tv": {"mountpoint": "media/tv", "pool": "main",
                                                  "drive_id": "0A", "mount": False}}))  # fmt: skip
    agent.job_poll_interval = 0.01
    sup = agent.units["media-tv"]
    sup.layout.runtime_dir("media-tv").mkdir(parents=True)
    sup.write_conf()  # settles the pool on its first key
    sup.state = State.READY
    (tmp_path / "local/media/tv").mkdir(parents=True)
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("media-tv")):
        yield agent


async def test_move_succeeds(agent: Agent, fake_rclone: FakeRclone, tmp_path: Path) -> None:
    result = await agent.transfers["media-tv"].run()
    assert result["ok"] and result["files"] == 3 and result["bytes"] == 1024
    endpoint, params = next(c for c in fake_rclone.calls if c[0].startswith("sync/"))
    assert endpoint == "sync/move"
    assert params["srcFs"] == str(tmp_path / "local/media/tv")
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
    (tmp_path / "local/media/tv").rmdir()
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
