"""The controller's API, the agent's controller pool, and two agents sharing
one pool through it."""

from __future__ import annotations

import asyncio
import json
import stat
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from ferryman.agent import Agent
from ferryman.config import Config, ControllerConfig, Ui, overlaps
from ferryman.controller import Controller, build_app, token_hash
from ferryman.controller_pool import ControllerClient, ControllerError, ControllerPool
from ferryman.pool import PoolExhausted
from ferryman.unit import State
from tests.conftest import KEYS, FakeRclone, serve_unix, write_keys

A, B, C = KEYS
TOKENS = {"site-a": "token-a", "site-b": "token-b", "nobody": "token-c"}
QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"


@pytest.fixture
def controller(tmp_path: Path) -> Controller:
    write_keys(tmp_path / "keys", KEYS)
    cfg = ControllerConfig.model_validate(
        {
            "state": str(tmp_path / "co-state"),
            "watch_timeout": 0.5,
            "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
            # An agent may lease the pools of the units bound to it.
            "units": {"tv": {"drive_id": "0A", "pool": "main"}},
            "agents": {
                "site-a": {"token_sha256": token_hash("token-a"), "units": {"tv": {}}},
                "site-b": {"token_sha256": token_hash("token-b"), "units": {"tv": {}}},
                "nobody": {"token_sha256": token_hash("token-c")},
            },
        }
    )
    return Controller(cfg)


@pytest.fixture
def transport(controller: Controller) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=build_app(controller))


def client(transport: httpx.AsyncBaseTransport, agent: str) -> ControllerClient:
    return ControllerClient("http://co", TOKENS[agent], transport=transport)


async def raw(transport: httpx.AsyncBaseTransport, method: str, path: str,
              token: str | None = None, **kw: Any) -> httpx.Response:  # fmt: skip
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(transport=transport, base_url="http://co") as c:
        return await c.request(method, path, headers=headers, **kw)


# ----------------------------------------------------------------- server


async def test_auth_and_scope(transport: httpx.ASGITransport) -> None:
    assert (await raw(transport, "GET", "/healthz")).status_code == 200
    assert (await raw(transport, "GET", "/v2/pools/main/lease")).status_code == 401
    assert (await raw(transport, "GET", "/v2/pools/main/lease", "wrong")).status_code == 401
    assert (await raw(transport, "GET", "/v2/pools/main/lease", "token-c")).status_code == 403
    assert (await raw(transport, "GET", "/v2/pools/nope/lease", "token-a")).status_code == 404
    r = await raw(transport, "GET", "/v2/pools/main/lease", "token-a")
    body = r.json()
    assert body["key"] == A and body["epoch"] == 0 and body["size"] == 3
    assert body["credentials"]["client_email"] == A


async def test_cap_hits_from_two_agents_rotate_once(transport: httpx.ASGITransport) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    ra, rb = await asyncio.gather(
        a.request("POST", "/v2/pools/main/exhausted", json={"expected": A}),
        b.request("POST", "/v2/pools/main/exhausted", json={"expected": A}),
    )
    assert ra["key"] == rb["key"] == B
    assert ra["epoch"] == rb["epoch"] == 1


async def test_all_exhausted_is_a_409_with_frees_at(transport: httpx.ASGITransport) -> None:
    a = client(transport, "site-a")
    await a.request("GET", "/v2/pools/main/lease")  # settles on A
    for key in (A, B):
        await a.request("POST", "/v2/pools/main/exhausted", json={"expected": key})
    # Only C is left: rotating by hand stays on it, a 409 rather than a server error.
    r = await raw(transport, "POST", "/v2/pools/main/exhausted", "token-a")
    assert r.status_code == 409 and "no other usable key" in r.json()["error"]
    with pytest.raises(PoolExhausted) as exc:
        await a.request("POST", "/v2/pools/main/exhausted", json={"expected": C})
    assert exc.value.frees_at > 0
    # Every key exhausted: rotating by hand gets the same answer.
    r = await raw(transport, "POST", "/v2/pools/main/exhausted", "token-a")
    assert r.status_code == 409 and "frees_at" in r.json()


async def test_watch_wakes_on_rotation_and_times_out(transport: httpx.ASGITransport) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    # Nothing changes: the long poll returns the same epoch after the timeout.
    r = await a.request("GET", "/v2/pools/main/watch", params={"epoch": 0})
    assert r["epoch"] == 0
    watcher = asyncio.create_task(a.request("GET", "/v2/pools/main/watch", params={"epoch": 0}))
    await asyncio.sleep(0.05)
    await b.request("POST", "/v2/pools/main/exhausted", json={"expected": A})
    r = await asyncio.wait_for(watcher, 1)
    assert r["epoch"] == 1 and r["key"] == B


def test_overlaps() -> None:
    assert overlaps("", "anything")
    assert overlaps("backups", "backups/pve-01")
    assert overlaps("a/b", "a/b")
    assert not overlaps("backups", "backups-old")
    assert not overlaps("a/b", "a/c")


# ------------------------------------------------------------ agent client


async def test_controller_pool_writes_leased_keys(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    pool = ControllerPool("main", client(transport, "site-a"), tmp_path / "leased")
    lease = await pool.lease()
    assert lease.key == A and pool.size == 3 and pool.rotates
    assert json.loads(lease.path.read_text())["client_email"] == A
    assert stat.S_IMODE(lease.path.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "leased").stat().st_mode) == 0o700

    lease2 = await pool.exhausted(A)
    assert lease2.key == B and lease2.epoch == 1
    assert not lease.path.exists()  # the old key's credentials are gone
    assert (await pool.info())["epoch"] == 1


async def test_controller_outage_keeps_last_lease(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    class Flaky(httpx.AsyncBaseTransport):
        down = False

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if self.down:
                raise httpx.ConnectError("connection refused")
            return await transport.handle_async_request(request)

    flaky = Flaky()
    pool = ControllerPool("main", client(flaky, "site-a"), tmp_path / "leased")
    with pytest.raises(ControllerError):
        flaky.down = True
        await pool.lease()  # never leased: nothing to fall back on
    flaky.down = False
    first = await pool.lease()
    flaky.down = True
    assert await pool.lease() == first
    with pytest.raises(ControllerError):
        await pool.exhausted(A)  # rotation needs the controller


# ------------------------------------------------------- two agents, one pool


def agent_config(tmp_path: Path, site: str, dest: str = "", mode: str = "move") -> Config:
    root = tmp_path / site
    hostname = site
    (root / "token").parent.mkdir(parents=True, exist_ok=True)
    (root / "token").write_text(TOKENS[site] + "\n")
    return Config.model_validate(
        {
            "hostname": hostname,
            "controller": {"url": "http://co", "token_file": str(root / "token")},
            "paths": {k: str(root / k) for k in ("runtime", "cache", "state", "logs")},
            "pools": {"main": {"controller": True}},
            "units": {"media": {"pool": "main", "drive_id": "0A"}},
            "transfers": {"media": {"unit": "media", "source": str(root / "local/media"),
                                    "dest": dest, "mode": mode}},
        }
    )  # fmt: skip


async def ready_agent(agent: Agent) -> None:
    """Mark the unit ready on its (fake) process with the current lease."""
    sup = agent.units["media"]
    lease = await agent.pools["main"].lease()
    sup.layout.runtime_dir("media").mkdir(parents=True, exist_ok=True)
    sup.key, sup.key_path = lease.key, lease.path
    sup.write_conf()
    sup.state = State.READY
    agent.layout.transfer_source("media").mkdir(parents=True, exist_ok=True)


@pytest.fixture
async def two_agents(
    transport: httpx.ASGITransport, tmp_path: Path
) -> AsyncIterator[tuple[Agent, FakeRclone, Agent, FakeRclone]]:
    a = Agent(agent_config(tmp_path, "site-a"), controller_transport=transport)
    b = Agent(agent_config(tmp_path, "site-b"), controller_transport=transport)
    fa, fb = FakeRclone(), FakeRclone()
    for agent in (a, b):
        agent.job_poll_interval = 0.01
    async with (
        serve_unix(fa.app(), a.layout.rc_socket("media")),
        serve_unix(fb.app(), b.layout.rc_socket("media")),
    ):
        await ready_agent(a)
        await ready_agent(b)
        yield a, fa, b, fb


async def test_cap_hit_on_one_agent_moves_the_other(
    two_agents: tuple[Agent, FakeRclone, Agent, FakeRclone],
) -> None:
    a, fa, b, fb = two_agents
    pool_b = b.pools["main"]
    assert isinstance(pool_b, ControllerPool)
    watch = asyncio.create_task(pool_b.watch(b.apply))
    try:
        fa.job_outcomes = [QUOTA, None]
        result = await a.transfers["media"].run()
        assert result["ok"]

        # site-b never hit the cap; the controller's epoch moved it anyway.
        async with asyncio.timeout(2):
            while b.units["media"].key != B:
                await asyncio.sleep(0.01)
        set_b = [p for e, p in fb.calls if e == "backend/command"]
        assert len(set_b) == 1
        assert set_b[0]["opt"]["service_account_file"].endswith(f"{B}.json")
        assert Path(set_b[0]["opt"]["service_account_file"]).exists()
        assert B in b.layout.rclone_conf("media").read_text()
    finally:
        watch.cancel()


# ------------------------------------------------------------- UI, commands


@pytest.fixture
def ui_transport(controller: Controller) -> httpx.ASGITransport:
    co = Controller(controller.cfg.model_copy(update={"ui": Ui(enabled=True)}))
    return httpx.ASGITransport(app=build_app(co))


UI = {"X-authentik-username": "stephen", "X-Ferryman-UI": "1"}


async def ui(transport: httpx.AsyncBaseTransport, method: str, path: str,
             headers: dict[str, str] | None = None, **kw: Any) -> httpx.Response:  # fmt: skip
    async with httpx.AsyncClient(transport=transport, base_url="http://co") as c:
        return await c.request(method, path, headers=UI if headers is None else headers, **kw)


async def test_ui_is_off_by_default(transport: httpx.ASGITransport) -> None:
    assert (await ui(transport, "GET", "/ui/api/state")).status_code == 404


async def test_ui_needs_the_auth_proxy_and_the_csrf_header(
    ui_transport: httpx.ASGITransport,
) -> None:
    assert (await ui(ui_transport, "GET", "/ui/", headers={})).status_code == 403
    page = await ui(ui_transport, "GET", "/ui/")
    assert page.status_code == 200 and "<title>ferryman</title>" in page.text
    assert (await ui(ui_transport, "GET", "/ui/api/state", headers={})).status_code == 403
    r = await ui(ui_transport, "POST", "/ui/api/pools/main/rotate",
                 headers={"X-authentik-username": "stephen"})  # fmt: skip
    assert r.status_code == 403 and "X-Ferryman-UI" in r.json()["error"]
    assert page.headers["cache-control"] == "no-cache"
    state = (await ui(ui_transport, "GET", "/ui/api/state")).json()
    assert state["user"] == "stephen"
    assert len(state["page"]) == 16  # open tabs reload when this changes
    assert [a["agent"] for a in state["agents"]] == ["site-a", "site-b", "nobody"]


async def test_ui_rotate_moves_the_pool(ui_transport: httpx.ASGITransport) -> None:
    r = await ui(ui_transport, "POST", "/ui/api/pools/main/rotate")
    assert r.json() == {"pool": "main", "current": B, "epoch": 1}
    # A keeps its quota: rotating by hand is not a cap hit.
    state = (await ui(ui_transport, "GET", "/ui/api/state")).json()
    assert not any(k["exhausted_until"] for k in state["pools"][0]["keys"])
    assert (await ui(ui_transport, "POST", "/ui/api/pools/nope/rotate")).status_code == 404


async def test_report_command_and_result_round_trip(
    ui_transport: httpx.ASGITransport, tmp_path: Path, fake_rclone: FakeRclone
) -> None:
    agent = Agent(agent_config(tmp_path, "site-a"), controller_transport=ui_transport)
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("media")):
        await ready_agent(agent)
        assert agent.controller
        # Commands only go to units an agent has reported.
        r = await ui(ui_transport, "POST", "/ui/api/agents/site-a/units/media/restart")
        assert r.status_code == 404
        await agent.controller.request("PUT", "/v2/report", json=await agent.report())
        state = (await ui(ui_transport, "GET", "/ui/api/state")).json()
        reported = state["agents"][0]
        assert reported["report"]["status"]["hostname"] == "site-a"
        assert reported["report"]["stats"][0]["unit"] == "media"

        r = await ui(ui_transport, "POST", "/ui/api/agents/site-a/transfers/media/run",
                     json={"dry_run": True})  # fmt: skip
        assert r.status_code == 200 and r.json()["state"] == "pending"
        assert r.json()["by"] == "stephen"

        loop = asyncio.create_task(agent._command_loop())
        try:
            async with asyncio.timeout(3):
                while (await ui(ui_transport, "GET", "/ui/api/state")).json()["commands"][0][
                    "state"
                ] != "done":
                    await asyncio.sleep(0.02)
                while agent.transfers["media"].last is None:
                    await asyncio.sleep(0.02)
        finally:
            loop.cancel()
            await agent.stop()
    cmd = (await ui(ui_transport, "GET", "/ui/api/state")).json()["commands"][0]
    assert cmd["result"] == {"ok": True, "started": True}
    assert agent.transfers["media"].last["dry_run"] is True
    sync_call = next(p for e, p in fake_rclone.calls if e.startswith("sync/"))
    assert sync_call["_config"]["DryRun"] is True


async def test_agent_reports_bad_commands(
    transport: httpx.ASGITransport, controller: Controller, tmp_path: Path
) -> None:
    agent = Agent(agent_config(tmp_path, "site-a"), controller_transport=transport)
    cmd = await controller.queue("site-a", "restart", "nope", "test")
    assert (await agent.execute(cmd)) == {"ok": False, "error": "not found: nope"}
    cmd = await controller.queue("site-a", "explode", "media", "test")
    assert "unknown action" in (await agent.execute(cmd))["error"]
    assert [c["state"] for c in controller.commands] == ["failed", "failed"]


async def test_controller_metrics(transport: httpx.ASGITransport, controller: Controller) -> None:
    controller.report("site-a", {"status": {"units": [{"state": "ready"}, {"state": "backoff"}]}})
    text = (await raw(transport, "GET", "/metrics")).text
    assert 'ferryman_agent_units_ready{agent="site-a"} 1.0' in text
    assert 'ferryman_agent_units{agent="site-a"} 2.0' in text
    assert f'ferryman_pool_current_key{{pool="main",key="{A}"}} 1.0' not in text  # not settled
    await raw(transport, "GET", "/v2/pools/main/lease", "token-a")
    text = (await raw(transport, "GET", "/metrics")).text
    assert f'ferryman_pool_current_key{{pool="main",key="{A}"}} 1.0' in text
    assert 'ferryman_agent_last_report_timestamp_seconds{agent="site-b"}' not in text


# ------------------------------------------------------------------ tuning


async def test_tune_lock_one_agent_per_uplink(
    transport: httpx.ASGITransport, controller: Controller
) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    assert (await a.request("POST", "/v2/tune-lock/home", json={"ttl": 600}))["granted"]
    r = await b.request("POST", "/v2/tune-lock/home", json={"ttl": 600})
    assert r == {"granted": False, "holder": "site-a", "until": r["until"]}
    assert (await b.request("POST", "/v2/tune-lock/office", json={"ttl": 600}))["granted"]
    await b.request("DELETE", "/v2/tune-lock/home")  # not b's to release
    assert not (await b.request("POST", "/v2/tune-lock/home"))["granted"]
    await a.request("DELETE", "/v2/tune-lock/home")
    assert (await b.request("POST", "/v2/tune-lock/home"))["granted"]
    # A holder that never releases loses the lock when it expires.
    controller.tune_locks["home"]["until"] = 0
    assert (await a.request("POST", "/v2/tune-lock/home"))["granted"]
    assert set(controller.state()["tune_locks"]) == {"home", "office"}


async def test_tuner_waits_for_the_uplink_lock(
    agent: Agent, transport: httpx.ASGITransport, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ferryman.config import Tuning
    from ferryman.tuning import TuneBusy

    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(uplink="home")})
    agent.controller = client(transport, "site-a")
    await client(transport, "site-b").request("POST", "/v2/tune-lock/home")
    tuned: list[str] = []

    async def fake_tune(sup: Any) -> Any:
        tuned.append(sup.name)
        raise RuntimeError("stop here")

    monkeypatch.setattr(agent.tuner, "_tune", fake_tune)
    with pytest.raises(TuneBusy, match="site-b is tuning on uplink home"):
        await agent.tuner.run()
    assert tuned == []
    await client(transport, "site-b").request("DELETE", "/v2/tune-lock/home")
    with pytest.raises(RuntimeError):
        await agent.tuner.run()
    assert tuned == ["media-tv"]
    # Released even though the tune blew up.
    assert (await client(transport, "site-b").request("POST", "/v2/tune-lock/home"))["granted"]


async def test_ui_tune_button_queues_a_command(
    ui_transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    assert (await ui(ui_transport, "POST", "/ui/api/agents/site-a/tune")).status_code == 404
    agent = Agent(agent_config(tmp_path, "site-a"), controller_transport=ui_transport)
    assert agent.controller
    await agent.controller.request("PUT", "/v2/report", json=await agent.report())
    r = await ui(ui_transport, "POST", "/ui/api/agents/site-a/tune")
    assert r.status_code == 200
    assert (r.json()["action"], r.json()["target"]) == ("tune", "drive")
    # The agent refuses a tune it cannot run, and says why.
    result = await agent.execute({"id": 1, "action": "tune", "target": "drive", "args": {}})
    assert result == {"ok": False, "error": "no ready drive unit to borrow a drive and key from"}
    await agent.stop()


# ------------------------------------------------------- config for agents


def write_config(path: Path, tmp_path: Path, **units: dict[str, Any]) -> Path:
    """A controller config whose site-a binds `units` (default: tv)."""
    data = {
        "state": str(tmp_path / "co-state"),
        "watch_timeout": 0.5,
        "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
        "units": {"tv": {"drive_id": "0A", "pool": "main"},
                  "films": {"drive_id": "0B", "pool": "main"}},
        "agents": {
            "site-a": {"token_sha256": token_hash("token-a"), "units": units or {"tv": {}}},
            "site-b": {"token_sha256": token_hash("token-b"), "units": {"films": {}}},
        },
    }  # fmt: skip
    path.write_text(json.dumps(data))  # JSON is YAML
    return path


@pytest.fixture
def reloading(tmp_path: Path) -> Controller:
    write_keys(tmp_path / "keys", KEYS)
    path = write_config(tmp_path / "controller.yaml", tmp_path)
    return Controller(ControllerConfig.load(path), path)


async def test_an_agent_gets_its_slice_and_waits_for_changes(reloading: Controller) -> None:
    transport = httpx.ASGITransport(app=build_app(reloading))
    a = client(transport, "site-a")
    first = await a.request("GET", "/v2/config")
    assert list(first["units"]) == ["tv"] and first["pools"] == ["main"]
    # Nothing changed: the long poll ends after watch_timeout with the same version.
    same = await a.request("GET", "/v2/config", params={"version": first["version"]})
    assert same == {"version": first["version"]}
    # A reload that changes site-a's slice answers its waiting poll at once.
    waiting = asyncio.create_task(
        a.request("GET", "/v2/config", params={"version": first["version"]})
    )
    await asyncio.sleep(0.05)
    assert reloading.path
    write_config(reloading.path, reloading.path.parent, tv={"uid": 1001})
    await reloading.check_config()
    changed = await asyncio.wait_for(waiting, 1)
    assert changed["version"] != first["version"]
    assert changed["units"]["tv"]["unit"]["uid"] == 1001


async def test_a_reload_wakes_only_the_agents_whose_slice_changed(
    reloading: Controller, tmp_path: Path
) -> None:
    b_before = reloading._slices["site-b"]["version"]
    assert reloading.path
    write_config(reloading.path, tmp_path, tv={"uid": 1001})
    await reloading.check_config()
    assert reloading._slices["site-b"]["version"] == b_before
    write_config(reloading.path, tmp_path, tv={"uid": 1001}, films={})
    changed = await reloading.reload(ControllerConfig.load(reloading.path))
    assert changed == ["site-a"]
    assert reloading.cfg.agent_pools("site-a") == ["main"]


async def test_a_bad_config_leaves_the_previous_one_in_force(
    reloading: Controller, tmp_path: Path
) -> None:
    version = reloading._slices["site-a"]["version"]
    assert reloading.path
    write_config(reloading.path, tmp_path, nope={})  # binds a unit that doesn't exist
    await reloading.check_config()
    assert reloading.config_error and "unknown unit 'nope'" in reloading.config_error
    assert reloading._slices["site-a"]["version"] == version
    # Unchanged since: not re-read (and the error stays) until the file changes.
    await reloading.check_config()
    assert reloading.config_error
    write_config(reloading.path, tmp_path)
    await reloading.check_config(force=True)
    assert reloading.config_error is None


async def test_v1_is_gone(transport: httpx.ASGITransport) -> None:
    r = await raw(transport, "GET", "/v1/pools/main/lease", "token-a")
    assert r.status_code == 410 and "/v2" in r.json()["error"]
