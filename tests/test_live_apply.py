"""Applying a new config in place: only what changed is touched, and nothing
a running transfer uses changes under it."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from ferryman.agent import Agent
from ferryman.config import Config, unit_spec


@pytest.fixture
def make(make_config: Callable[..., Config], tmp_path: Path) -> Callable[..., Config]:
    """Two units (tv, films), a transfer on each."""

    def build(tv: dict[str, Any] | None = None, films: dict[str, Any] | None = None,
              transfers: dict[str, Any] | None = None, **over: Any) -> Config:  # fmt: skip
        units = {
            "tv": {"drive_id": "0A", "pool": "main",
                   "mounts": [{"path": str(tmp_path / "m/tv")}], **(tv or {})},
            "films": {"drive_id": "0B", "pool": "main",
                      "mounts": [{"path": str(tmp_path / "m/films")}], **(films or {})},
        }  # fmt: skip
        if transfers is None:
            transfers = {"tv-up": {"unit": "tv"}, "films-up": {"unit": "films"}}
        return make_config(units=units, transfers=transfers, **over)

    return build


async def settle(agent: Agent) -> None:
    await asyncio.gather(*agent._settling)


async def test_only_the_changed_unit_is_swapped(make: Callable[..., Config]) -> None:
    agent = Agent(make())
    tv, films = agent.units["tv"], agent.units["films"]
    agent.apply_config(make(tv={"uid": 1001}), {"version": "2"})
    await settle(agent)
    assert agent.units["films"] is films and agent.units["tv"] is not tv
    assert agent.units["tv"].unit.uid == 1001
    assert agent.config_state == {"version": "2", "state": "applied", "error": None}


async def test_units_come_and_go(make: Callable[..., Config], tmp_path: Path) -> None:
    agent = Agent(make())
    films = agent.units["films"]
    music = {"drive_id": "0C", "pool": "main", "mounts": [{"path": str(tmp_path / "m/music")}]}
    cfg = make(transfers={"films-up": {"unit": "films"}})
    unit = type(cfg.units["films"])
    cfg = cfg.model_copy(
        update={"units": {"films": cfg.units["films"], "music": unit.model_validate(music)}}
    )
    agent.apply_config(cfg, {"version": "2"})
    await settle(agent)
    assert set(agent.units) == {"films", "music"} and agent.units["films"] is films
    assert set(agent.transfers) == {"films-up"}


async def test_a_change_in_where_a_value_comes_from_restarts_nothing(
    make: Callable[..., Config],
) -> None:
    # 16M from the defaults, then the same 16M set on the unit itself.
    agent = Agent(make())
    tv = agent.units["tv"]
    agent.apply_config(make(tv={"buffer_size": "16M"}), {"version": "2"})
    assert agent.units["tv"] is tv
    # ...but a different value does.
    agent.apply_config(make(tv={"buffer_size": "32M"}), {"version": "3"})
    await settle(agent)
    assert agent.units["tv"] is not tv


def test_unit_spec_covers_what_the_process_is_built_from(make: Callable[..., Config]) -> None:
    base = make()
    assert unit_spec(base, "tv") == unit_spec(make(), "tv")
    for changed in (
        make(tv={"vfs_cache_mode": "writes"}),
        make(defaults={"cache_min_free_space": "1G"}),
        make(rclone={"log_level": "DEBUG"}),
        make(health={"stat_timeout": 3}),
        make(tv={"mounts": [{"path": "/elsewhere/tv"}]}),
    ):
        assert unit_spec(changed, "tv") != unit_spec(base, "tv")


async def test_a_unit_in_use_waits_for_its_transfer(make: Callable[..., Config]) -> None:
    agent = Agent(make())
    tv, runner = agent.units["tv"], agent.transfers["tv-up"]
    await runner.lock.acquire()  # tv-up is running
    agent.apply_config(make(tv={"uid": 1001}), {"version": "2"})
    assert agent.units["tv"] is tv
    assert agent.config_state["state"] == "pending"
    assert agent.config_state["pending"] == ["restart unit tv after transfer tv-up"]
    runner.lock.release()
    agent.on_transfer_done("tv-up")
    await settle(agent)
    assert agent.units["tv"] is not tv and agent.units["tv"].unit.uid == 1001
    assert agent.config_state["state"] == "applied" and "pending" not in agent.config_state


async def test_a_running_transfer_finishes_as_it_started(make: Callable[..., Config]) -> None:
    agent = Agent(make())
    runner = agent.transfers["tv-up"]
    await runner.lock.acquire()
    moved = {"tv-up": {"unit": "tv", "dest": "elsewhere"}, "films-up": {"unit": "films"}}
    agent.apply_config(make(transfers=moved), {"version": "2"})
    # Still the running one, on its own config.
    assert agent.transfers["tv-up"] is runner and runner.t.dest == ""
    assert runner.status()["dest"] == ""
    assert agent.config_state["pending"] == ["update transfer tv-up after its run"]
    runner.lock.release()
    agent.on_transfer_done("tv-up")
    assert agent.transfers["tv-up"] is not runner
    assert agent.transfers["tv-up"].t.dest == "elsewhere"
    assert agent.config_state["state"] == "applied"


async def test_a_removed_transfer_keeps_working_until_its_run_ends(
    make: Callable[..., Config],
) -> None:
    agent = Agent(make())
    runner = agent.transfers["tv-up"]
    await runner.lock.acquire()
    agent.apply_config(make(transfers={"films-up": {"unit": "films"}}), {"version": "2"})
    assert runner.status()["transfer"] == "tv-up"  # its source and dest still resolve
    assert agent.config_state["pending"] == ["remove transfer tv-up after its run"]
    runner.lock.release()
    agent.on_transfer_done("tv-up")
    assert set(agent.transfers) == {"films-up"}


async def test_a_unit_removed_under_a_running_transfer_waits(make: Callable[..., Config]) -> None:
    agent = Agent(make())
    runner = agent.transfers["tv-up"]
    await runner.lock.acquire()
    cfg = make(transfers={"films-up": {"unit": "films"}})
    cfg = cfg.model_copy(update={"units": {"films": cfg.units["films"]}})
    agent.apply_config(cfg, {"version": "2"})
    assert "tv" in agent.units  # still there for the run
    assert sorted(agent.config_state["pending"]) == [
        "remove transfer tv-up after its run", "stop unit tv after transfer tv-up"]  # fmt: skip
    runner.lock.release()
    agent.on_transfer_done("tv-up")
    await settle(agent)
    assert set(agent.units) == {"films"} and set(agent.transfers) == {"films-up"}
    assert agent.config_state["state"] == "applied"
