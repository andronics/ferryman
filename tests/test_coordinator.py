"""The coordinator's API, the agent's coordinator pool, and two agents sharing
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
from ferryman.config import Config, CoordinatorConfig, Ui
from ferryman.coordinator import Coordinator, build_app, overlaps, token_hash
from ferryman.coordinator_pool import CoordinatorClient, CoordinatorError, CoordinatorPool
from ferryman.pool import PoolExhausted
from ferryman.unit import State
from tests.conftest import KEYS, FakeRclone, serve_unix, write_keys

A, B, C = KEYS
TOKENS = {"site-a": "token-a", "site-b": "token-b", "nobody": "token-c"}
QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"


@pytest.fixture
def coordinator(tmp_path: Path) -> Coordinator:
    write_keys(tmp_path / "keys", KEYS)
    cfg = CoordinatorConfig.model_validate(
        {
            "state": str(tmp_path / "co-state"),
            "watch_timeout": 0.5,
            "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
            "agents": {
                "site-a": {"token_sha256": token_hash("token-a"), "pools": ["main"]},
                "site-b": {"token_sha256": token_hash("token-b"), "pools": ["main"]},
                "nobody": {"token_sha256": token_hash("token-c"), "pools": []},
            },
        }
    )
    return Coordinator(cfg)


@pytest.fixture
def transport(coordinator: Coordinator) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=build_app(coordinator))


def client(transport: httpx.AsyncBaseTransport, agent: str) -> CoordinatorClient:
    return CoordinatorClient("http://co", TOKENS[agent], transport=transport)


async def raw(transport: httpx.AsyncBaseTransport, method: str, path: str,
              token: str | None = None, **kw: Any) -> httpx.Response:  # fmt: skip
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(transport=transport, base_url="http://co") as c:
        return await c.request(method, path, headers=headers, **kw)


# ----------------------------------------------------------------- server


async def test_auth_and_scope(transport: httpx.ASGITransport) -> None:
    assert (await raw(transport, "GET", "/healthz")).status_code == 200
    assert (await raw(transport, "GET", "/v1/pools/main/lease")).status_code == 401
    assert (await raw(transport, "GET", "/v1/pools/main/lease", "wrong")).status_code == 401
    assert (await raw(transport, "GET", "/v1/pools/main/lease", "token-c")).status_code == 403
    assert (await raw(transport, "GET", "/v1/pools/nope/lease", "token-a")).status_code == 404
    r = await raw(transport, "GET", "/v1/pools/main/lease", "token-a")
    body = r.json()
    assert body["key"] == A and body["epoch"] == 0 and body["size"] == 3
    assert body["credentials"]["client_email"] == A


async def test_cap_hits_from_two_agents_rotate_once(transport: httpx.ASGITransport) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    ra, rb = await asyncio.gather(
        a.request("POST", "/v1/pools/main/exhausted", json={"expected": A}),
        b.request("POST", "/v1/pools/main/exhausted", json={"expected": A}),
    )
    assert ra["key"] == rb["key"] == B
    assert ra["epoch"] == rb["epoch"] == 1


async def test_all_exhausted_is_a_409_with_frees_at(transport: httpx.ASGITransport) -> None:
    a = client(transport, "site-a")
    await a.request("GET", "/v1/pools/main/lease")  # settles on A
    for _ in range(2):
        await a.request("POST", "/v1/pools/main/exhausted", json={})
    with pytest.raises(PoolExhausted) as exc:
        await a.request("POST", "/v1/pools/main/exhausted", json={})
    assert exc.value.frees_at > 0


async def test_watch_wakes_on_rotation_and_times_out(transport: httpx.ASGITransport) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    # Nothing changes: the long poll returns the same epoch after the timeout.
    r = await a.request("GET", "/v1/pools/main/watch", params={"epoch": 0})
    assert r["epoch"] == 0
    watcher = asyncio.create_task(a.request("GET", "/v1/pools/main/watch", params={"epoch": 0}))
    await asyncio.sleep(0.05)
    await b.request("POST", "/v1/pools/main/exhausted", json={"expected": A})
    r = await asyncio.wait_for(watcher, 1)
    assert r["epoch"] == 1 and r["key"] == B


def test_overlaps() -> None:
    assert overlaps("", "anything")
    assert overlaps("backups", "backups/pve-01")
    assert overlaps("a/b", "a/b")
    assert not overlaps("backups", "backups-old")
    assert not overlaps("a/b", "a/c")


async def test_claims(transport: httpx.ASGITransport) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")

    def claim(t: str, dest: str, mode: str) -> dict[str, str]:
        return {"transfer": t, "target": "drive-1", "dest": dest, "mode": mode}

    r = await a.request("PUT", "/v1/claims", json=[claim("a-mirror", "backups/a", "sync")])
    assert r == {"accepted": ["a-mirror"], "conflicts": []}
    r = await b.request("PUT", "/v1/claims", json=[
        claim("b-up", "backups", "move"),        # contains a's mirror: refused
        claim("b-media", "media", "move"),       # elsewhere: fine
    ])  # fmt: skip
    assert r["accepted"] == ["b-media"]
    assert r["conflicts"] == [{"transfer": "b-up", "owner": "site-a",
                               "owner_transfer": "a-mirror"}]  # fmt: skip
    # Two moves into the same place are fine.
    r = await a.request("PUT", "/v1/claims", json=[claim("a-media", "media", "move")])
    assert r["accepted"] == ["a-media"]
    # a dropped its mirror by re-claiming without it, so b's move is now fine.
    r = await b.request("PUT", "/v1/claims", json=[claim("b-up", "backups", "move")])
    assert r["accepted"] == ["b-up"]


# ------------------------------------------------------------ agent client


async def test_coordinator_pool_writes_leased_keys(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    pool = CoordinatorPool("main", client(transport, "site-a"), tmp_path / "leased")
    lease = await pool.lease()
    assert lease.key == A and pool.size == 3 and pool.rotates
    assert json.loads(lease.path.read_text())["client_email"] == A
    assert stat.S_IMODE(lease.path.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "leased").stat().st_mode) == 0o700

    lease2 = await pool.exhausted(A)
    assert lease2.key == B and lease2.epoch == 1
    assert not lease.path.exists()  # the old key's credentials are gone
    assert (await pool.info())["epoch"] == 1


async def test_coordinator_outage_keeps_last_lease(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    class Flaky(httpx.AsyncBaseTransport):
        down = False

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if self.down:
                raise httpx.ConnectError("connection refused")
            return await transport.handle_async_request(request)

    flaky = Flaky()
    pool = CoordinatorPool("main", client(flaky, "site-a"), tmp_path / "leased")
    with pytest.raises(CoordinatorError):
        flaky.down = True
        await pool.lease()  # never leased: nothing to fall back on
    flaky.down = False
    first = await pool.lease()
    flaky.down = True
    assert await pool.lease() == first
    with pytest.raises(CoordinatorError):
        await pool.exhausted(A)  # rotation needs the coordinator


# ------------------------------------------------------- two agents, one pool


def agent_config(tmp_path: Path, site: str, dest: str = "", mode: str = "move") -> Config:
    root = tmp_path / site
    hostname = site
    (root / "token").parent.mkdir(parents=True, exist_ok=True)
    (root / "token").write_text(TOKENS[site] + "\n")
    return Config.model_validate(
        {
            "hostname": hostname,
            "coordinator": {"url": "http://co", "token_file": str(root / "token")},
            "paths": {k: str(root / k) for k in ("runtime", "cache", "state", "logs")},
            "pools": {"main": {"coordinator": True}},
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
    a = Agent(agent_config(tmp_path, "site-a"), coordinator_transport=transport)
    b = Agent(agent_config(tmp_path, "site-b"), coordinator_transport=transport)
    fa, fb = FakeRclone(), FakeRclone()
    for agent in (a, b):
        agent.job_poll_interval = 0.01
        agent.claim_blocks = {}  # claims have their own tests
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
    assert isinstance(pool_b, CoordinatorPool)
    watch = asyncio.create_task(pool_b.watch(b.apply))
    try:
        fa.job_outcomes = [QUOTA, None]
        result = await a.transfers["media"].run()
        assert result["ok"]

        # site-b never hit the cap; the coordinator's epoch moved it anyway.
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


async def test_conflicting_sync_claim_blocks_the_transfer(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    owner = client(transport, "site-a")
    await owner.request("PUT", "/v1/claims", json=[
        {"transfer": "mirror", "target": "0A", "dest": "backups", "mode": "sync"}])  # fmt: skip

    b = Agent(agent_config(tmp_path, "site-b", dest="backups/b", mode="move"),
              coordinator_transport=transport)  # fmt: skip
    # Never granted before: blocked until the coordinator answers.
    assert "awaiting" in b.claim_blocks["media"]
    task = asyncio.create_task(b._claims_loop())
    try:
        async with asyncio.timeout(2):
            while "awaiting" in b.claim_blocks.get("media", "awaiting"):
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
    assert "site-a's transfer mirror" in b.claim_blocks["media"]
    result = await b.transfers["media"].run()
    assert not result["ok"] and "overlaps" in result["error"]


async def test_granted_claims_survive_a_coordinator_outage(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    a = Agent(agent_config(tmp_path, "site-a"), coordinator_transport=transport)
    task = asyncio.create_task(a._claims_loop())
    try:
        async with asyncio.timeout(2):
            while a.claim_blocks:
                await asyncio.sleep(0.01)
    finally:
        task.cancel()
    # A restarted agent that can't reach the coordinator still runs it.
    restarted = Agent(agent_config(tmp_path, "site-a"))
    assert restarted.claim_blocks == {}


async def test_hostname_dests_keep_two_nodes_syncs_apart(
    transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    """One config on every node: `{hostname}` gives each sync its own folder."""
    agents = [
        Agent(agent_config(tmp_path, site, dest="backups/{hostname}", mode="sync"),
              coordinator_transport=transport)
        for site in ("site-a", "site-b")
    ]  # fmt: skip
    for agent in agents:
        assert agent.coordinator
        r = await agent.coordinator.request("PUT", "/v1/claims", json=agent._claims())
        assert r == {"accepted": ["media"], "conflicts": []}
    claims = await raw(transport, "GET", "/v1/claims", "token-a")
    assert claims.json()["site-b"][0]["dest"] == "backups/site-b"


# ------------------------------------------------------------- UI, commands


@pytest.fixture
def ui_transport(coordinator: Coordinator) -> httpx.ASGITransport:
    co = Coordinator(coordinator.cfg.model_copy(update={"ui": Ui(enabled=True)}))
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
    state = (await ui(ui_transport, "GET", "/ui/api/state")).json()
    assert state["user"] == "stephen"
    assert [a["agent"] for a in state["agents"]] == ["site-a", "site-b", "nobody"]


async def test_ui_rotate_moves_the_pool(ui_transport: httpx.ASGITransport) -> None:
    r = await ui(ui_transport, "POST", "/ui/api/pools/main/rotate")
    assert r.json() == {"pool": "main", "current": B, "epoch": 1}
    assert (await ui(ui_transport, "POST", "/ui/api/pools/nope/rotate")).status_code == 404


async def test_report_command_and_result_round_trip(
    ui_transport: httpx.ASGITransport, tmp_path: Path, fake_rclone: FakeRclone
) -> None:
    agent = Agent(agent_config(tmp_path, "site-a"), coordinator_transport=ui_transport)
    agent.claim_blocks = {}
    async with serve_unix(fake_rclone.app(), agent.layout.rc_socket("media")):
        await ready_agent(agent)
        assert agent.coordinator
        # Commands only go to units an agent has reported.
        r = await ui(ui_transport, "POST", "/ui/api/agents/site-a/units/media/restart")
        assert r.status_code == 404
        await agent.coordinator.request("PUT", "/v1/report", json=await agent.report())
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
    transport: httpx.ASGITransport, coordinator: Coordinator, tmp_path: Path
) -> None:
    agent = Agent(agent_config(tmp_path, "site-a"), coordinator_transport=transport)
    cmd = await coordinator.queue("site-a", "restart", "nope", "test")
    assert (await agent.execute(cmd)) == {"ok": False, "error": "not found: nope"}
    cmd = await coordinator.queue("site-a", "explode", "media", "test")
    assert "unknown action" in (await agent.execute(cmd))["error"]
    assert [c["state"] for c in coordinator.commands] == ["failed", "failed"]


async def test_coordinator_metrics(
    transport: httpx.ASGITransport, coordinator: Coordinator
) -> None:
    coordinator.report("site-a", {"status": {"units": [{"state": "ready"}, {"state": "backoff"}]}})
    text = (await raw(transport, "GET", "/metrics")).text
    assert 'ferryman_agent_units_ready{agent="site-a"} 1.0' in text
    assert 'ferryman_agent_units{agent="site-a"} 2.0' in text
    assert f'ferryman_pool_current_key{{pool="main",key="{A}"}} 1.0' not in text  # not settled
    await raw(transport, "GET", "/v1/pools/main/lease", "token-a")
    text = (await raw(transport, "GET", "/metrics")).text
    assert f'ferryman_pool_current_key{{pool="main",key="{A}"}} 1.0' in text
    assert 'ferryman_agent_last_report_timestamp_seconds{agent="site-b"}' not in text


# ------------------------------------------------------------------ tuning


async def test_tune_lock_one_agent_per_uplink(
    transport: httpx.ASGITransport, coordinator: Coordinator
) -> None:
    a, b = client(transport, "site-a"), client(transport, "site-b")
    assert (await a.request("POST", "/v1/tune-lock/home", json={"ttl": 600}))["granted"]
    r = await b.request("POST", "/v1/tune-lock/home", json={"ttl": 600})
    assert r == {"granted": False, "holder": "site-a", "until": r["until"]}
    assert (await b.request("POST", "/v1/tune-lock/office", json={"ttl": 600}))["granted"]
    await b.request("DELETE", "/v1/tune-lock/home")  # not b's to release
    assert not (await b.request("POST", "/v1/tune-lock/home"))["granted"]
    await a.request("DELETE", "/v1/tune-lock/home")
    assert (await b.request("POST", "/v1/tune-lock/home"))["granted"]
    # A holder that never releases loses the lock when it expires.
    coordinator.tune_locks["home"]["until"] = 0
    assert (await a.request("POST", "/v1/tune-lock/home"))["granted"]
    assert set(coordinator.state()["tune_locks"]) == {"home", "office"}


async def test_tuner_waits_for_the_uplink_lock(
    agent: Agent, transport: httpx.ASGITransport, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ferryman.config import Tuning
    from ferryman.tuning import TuneBusy

    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(uplink="home")})
    agent.coordinator = client(transport, "site-a")
    await client(transport, "site-b").request("POST", "/v1/tune-lock/home")
    tuned: list[str] = []

    async def fake_tune(sup: Any) -> Any:
        tuned.append(sup.name)
        raise RuntimeError("stop here")

    monkeypatch.setattr(agent.tuner, "_tune", fake_tune)
    with pytest.raises(TuneBusy, match="site-b is tuning on uplink home"):
        await agent.tuner.run()
    assert tuned == []
    await client(transport, "site-b").request("DELETE", "/v1/tune-lock/home")
    with pytest.raises(RuntimeError):
        await agent.tuner.run()
    assert tuned == ["media-tv"]
    # Released even though the tune blew up.
    assert (await client(transport, "site-b").request("POST", "/v1/tune-lock/home"))["granted"]


async def test_ui_tune_button_queues_a_command(
    ui_transport: httpx.ASGITransport, tmp_path: Path
) -> None:
    assert (await ui(ui_transport, "POST", "/ui/api/agents/site-a/tune")).status_code == 404
    agent = Agent(agent_config(tmp_path, "site-a"), coordinator_transport=ui_transport)
    assert agent.coordinator
    await agent.coordinator.request("PUT", "/v1/report", json=await agent.report())
    r = await ui(ui_transport, "POST", "/ui/api/agents/site-a/tune")
    assert r.status_code == 200
    assert (r.json()["action"], r.json()["target"]) == ("tune", "drive")
    # The agent refuses a tune it cannot run, and says why.
    result = await agent.execute({"id": 1, "action": "tune", "target": "drive", "args": {}})
    assert result == {"ok": False, "error": "no ready drive unit to borrow a drive and key from"}
    await agent.stop()
