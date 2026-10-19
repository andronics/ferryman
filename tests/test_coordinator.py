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
from ferryman.config import Config, CoordinatorConfig
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
    (root / "token").parent.mkdir(parents=True, exist_ok=True)
    (root / "token").write_text(TOKENS[site] + "\n")
    return Config.model_validate(
        {
            "coordinator": {"url": "http://co", "token_file": str(root / "token")},
            "paths": {k: str(root / k) for k in ("runtime", "cache", "state", "logs")},
            "roots": {"local": str(root / "local"), "remote": str(root / "remote")},
            "pools": {"main": {"coordinator": True}},
            "units": {"media": {"mountpoint": "media", "pool": "main", "drive_id": "0A",
                                "mount": False}},
            "transfers": {"media": {"unit": "media", "dest": dest, "mode": mode}},
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
    (agent.layout.local_path("media")).mkdir(parents=True, exist_ok=True)


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
