"""Starting an agent from its own file and the controller's slice: fetched,
cached, started from the cache in an outage, waited for with neither, and
checked on the host first."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import Any

import httpx
import pytest

from ferryman.agent import Agent
from ferryman.bootstrap import Rejected, load_cached, slice_file, start_config
from ferryman.config import AgentBootstrap, ControllerConfig
from ferryman.controller import Controller, build_app, token_hash
from ferryman.controller_pool import ControllerClient, ControllerPool
from tests.conftest import KEYS, write_keys

A, B, C = KEYS


class Switch(httpx.AsyncBaseTransport):
    """The controller, reachable or not."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner, self.down, self.calls = inner, False, 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.down:
            raise httpx.ConnectError("connection refused")
        return await self.inner.handle_async_request(request)


def controller_config(tmp_path: Path, **binding: Any) -> ControllerConfig:
    write_keys(tmp_path / "keys", KEYS)
    return ControllerConfig.model_validate({
        "state": str(tmp_path / "co"),
        "watch_timeout": 0.3,
        "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
        "units": {"backups": {"drive_id": "0A", "pool": "main"}},
        "agents": {"site-a": {"token_sha256": token_hash("token-a"),
                              "units": {"backups": binding}}},
    })  # fmt: skip


@pytest.fixture
def boot(tmp_path: Path) -> AgentBootstrap:
    (tmp_path / "token").write_text("token-a")
    return AgentBootstrap.model_validate({
        "controller": {"url": "http://co", "token_file": str(tmp_path / "token")},
        "hostname": "site-a",
        "paths": {k: str(tmp_path / "agent" / k) for k in ("runtime", "cache", "state", "logs")},
    })  # fmt: skip


@pytest.fixture
def co(tmp_path: Path) -> Controller:
    return Controller(controller_config(tmp_path))


def client(transport: httpx.AsyncBaseTransport) -> ControllerClient:
    return ControllerClient("http://co", "token-a", transport=transport)


async def test_starts_from_the_controller_and_caches_its_slice(
    boot: AgentBootstrap, co: Controller
) -> None:
    start = await start_config(boot, client(httpx.ASGITransport(app=build_app(co))))
    assert start.source == "controller" and start.rejected is None
    assert list(start.cfg.units) == ["backups"] and start.cfg.pools["main"].controller
    assert start.cfg.hostname == "site-a"
    assert load_cached(boot) == start.slice
    assert stat.S_IMODE(slice_file(boot).stat().st_mode) == 0o600


async def test_an_outage_starts_from_the_cache(boot: AgentBootstrap, co: Controller) -> None:
    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    first = await start_config(boot, client(switch))
    switch.down = True
    again = await start_config(boot, client(switch))
    assert again.source == "cache" and again.slice == first.slice


async def test_with_no_cache_it_waits_for_the_controller(
    boot: AgentBootstrap, co: Controller, monkeypatch: pytest.MonkeyPatch
) -> None:
    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    switch.down = True
    real_sleep = __import__("asyncio").sleep

    async def sleep(_: float) -> None:  # the retry delay, made instant
        if switch.calls >= 3:
            switch.down = False
        await real_sleep(0)

    monkeypatch.setattr("ferryman.bootstrap.asyncio.sleep", sleep)
    start = await start_config(boot, client(switch))
    assert start.source == "controller" and switch.calls >= 4


async def test_a_slice_the_host_refuses_falls_back_to_the_cached_one(
    boot: AgentBootstrap, tmp_path: Path
) -> None:
    good = Controller(controller_config(tmp_path))
    await start_config(boot, client(httpx.ASGITransport(app=build_app(good))))
    # Mounted inside the agent's own state dir: only the host can tell.
    bad = Controller(controller_config(
        tmp_path, mounts=[{"path": str(boot.paths.state / "mnt")}]))  # fmt: skip
    start = await start_config(boot, client(httpx.ASGITransport(app=build_app(bad))))
    assert start.source == "cache" and start.rejected and "ferryman's own" in start.rejected
    assert start.cfg.units["backups"].mounts == []
    # With no previous config to fall back on, it can't start.
    slice_file(boot).unlink()
    with pytest.raises(Rejected, match="ferryman's own"):
        await start_config(boot, client(httpx.ASGITransport(app=build_app(bad))))


async def test_the_leased_key_survives_a_restart(tmp_path: Path, co: Controller) -> None:
    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    keys = tmp_path / "state" / "controller" / "keys" / "main"
    first = await ControllerPool("main", client(switch), keys).lease()
    switch.down = True
    # A new process, the controller away: the key on disk is still leased.
    pool = ControllerPool("main", client(switch), keys)
    assert pool.current == first.key and pool.epoch == first.epoch and pool.size == 3
    assert await pool.lease() == first
    assert stat.S_IMODE((keys / "lease").stat().st_mode) == 0o600
    assert json.loads((keys / "lease").read_text())["key"] == A


async def test_the_agent_tracks_the_controllers_version(
    boot: AgentBootstrap, tmp_path: Path, co: Controller
) -> None:
    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    await start_config(boot, client(switch))
    switch.down = True
    start = await start_config(boot, client(switch))
    agent = Agent(start.cfg, controller_transport=switch, bootstrap=boot, start=start)
    assert agent.config_state["state"] == "cached"
    # The controller is back, on the same version: confirmed.
    agent.on_slice({"version": start.slice["version"]})
    assert agent.config_state["state"] == "applied"
    # A newer one that runs here: applied in place, and cached.
    newer = Controller(controller_config(tmp_path, uid=1001)).cfg.slice("site-a")
    agent.on_slice(newer)
    assert agent.config_state == {"version": newer["version"], "state": "applied", "error": None}
    assert agent.units["backups"].unit.uid == 1001
    assert load_cached(boot) == newer
    # One that doesn't: rejected, with why; the applied one stays in force.
    bad = Controller(controller_config(
        tmp_path, mounts=[{"path": str(boot.paths.state / "x")}])).cfg.slice("site-a")  # fmt: skip
    agent.on_slice(bad)
    assert (
        agent.config_state["state"] == "rejected"
        and "ferryman's own" in agent.config_state["error"]
    )
    assert agent.config_state["version"] == newer["version"] and load_cached(boot) == newer
    assert (await agent.report())["config"]["state"] == "rejected"


async def test_a_rejected_version_is_waited_on_not_spun_on(
    boot: AgentBootstrap, tmp_path: Path, co: Controller
) -> None:
    import asyncio

    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    start = await start_config(boot, client(switch))
    agent = Agent(start.cfg, controller_transport=switch, bootstrap=boot, start=start)
    # A newer version this host refuses (a mount inside its state dir).
    await co.reload(controller_config(tmp_path, mounts=[{"path": str(boot.paths.state / "m")}]))
    switch.calls = 0
    task = asyncio.create_task(agent._config_loop())
    await asyncio.sleep(0.5)  # watch_timeout is 0.3s: a couple of long polls at most
    task.cancel()
    assert agent.config_state["state"] == "rejected"
    assert switch.calls <= 4


async def test_a_cached_config_is_confirmed_as_soon_as_the_controller_is_back(
    boot: AgentBootstrap, co: Controller
) -> None:
    import asyncio

    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    await start_config(boot, client(switch))
    switch.down = True
    start = await start_config(boot, client(switch))
    switch.down = False
    agent = Agent(start.cfg, controller_transport=switch, bootstrap=boot, start=start)
    task = asyncio.create_task(agent._config_loop())
    await asyncio.sleep(0.1)  # well inside one long poll
    task.cancel()
    assert agent.config_state["state"] == "applied"
