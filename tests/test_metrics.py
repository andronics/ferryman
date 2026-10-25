"""/metrics, the TCP probes, events from the agent, and restart dedupe."""

from __future__ import annotations

import asyncio
import re

import httpx

from ferryman.agent import Agent
from ferryman.api import build_app, build_tcp_app
from ferryman.metrics import agent_metrics
from ferryman.unit import State
from tests.conftest import KEYS, FakeRclone

QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"
SAMPLE = re.compile(r'^[a-z_]+(\{([a-z_]+="[^"]*",?)*\})? -?[0-9.e+-]+$')


def samples(text: str) -> dict[str, float]:
    out = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        assert SAMPLE.match(line), line
        name, value = line.rsplit(" ", 1)
        out[name] = float(value)
    return out


async def test_agent_metrics(agent: Agent, fake_rclone: FakeRclone) -> None:
    await agent.transfers["media-tv"].run()
    fake_rclone.job_outcomes = ["directory not found"]
    await agent.transfers["media-tv"].run()
    text = await agent_metrics(agent)
    m = samples(text)
    assert "# TYPE ferryman_transfer_runs_total counter" in text
    assert m['ferryman_unit_ready{unit="media-tv"}'] == 1
    assert m['ferryman_unit_state{unit="media-tv",state="ready"}'] == 1
    assert m['ferryman_unit_state{unit="media-tv",state="backoff"}'] == 0
    assert m['ferryman_rclone_bytes{unit="media-tv"}'] == 1024
    assert m['ferryman_transfer_runs_total{transfer="media-tv",result="ok"}'] == 1
    assert m['ferryman_transfer_runs_total{transfer="media-tv",result="failed"}'] == 1
    assert m['ferryman_transfer_bytes_total{transfer="media-tv"}'] == 1024
    assert m['ferryman_transfer_last_success{transfer="media-tv"}'] == 0
    assert 'ferryman_transfer_last_success_timestamp_seconds{transfer="media-tv"}' in m
    assert m['ferryman_pool_keys{pool="main"}'] == 3
    assert m[f'ferryman_pool_current_key{{pool="main",key="{KEYS[0]}"}}'] == 1


async def test_probes_and_metrics_over_tcp_app(agent: Agent) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=build_tcp_app(agent)), base_url="http://a"
    ) as c:
        assert (await c.get("/healthz")).json() == {"ok": True}
        assert (await c.get("/readyz")).status_code == 200
        agent.units["media-tv"].state = State.BACKOFF
        r = await c.get("/readyz")
        assert r.status_code == 503 and r.json()["waiting"] == {"media-tv": "backoff"}
        r = await c.get("/metrics")
        assert r.headers["content-type"].startswith("text/plain; version=0.0.4")
        # Control stays on the unix socket.
        assert (await c.post("/v1/pools/main/rotate")).status_code == 404


async def test_events_from_transfers_and_rotation(agent: Agent, fake_rclone: FakeRclone) -> None:
    fake_rclone.job_outcomes = ["directory not found"]
    await agent.transfers["media-tv"].run()
    await agent.transfers["media-tv"].run()
    fake_rclone.job_outcomes = [QUOTA]
    await agent.transfers["media-tv"].run()
    kinds = [(e.kind, e.subject) for e in agent.notifier.history]
    assert kinds == [
        ("transfer.failed", "media-tv"),
        ("transfer.recovered", "media-tv"),
        ("pool.rotated", "main"),
    ]
    assert "cap hit on " + KEYS[0] in agent.notifier.history[-1].message
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=build_app(agent)), base_url="http://a"
    ) as c:
        rows = (await c.get("/v1/events", params={"limit": 2})).json()
    assert [r["kind"] for r in rows] == ["transfer.recovered", "pool.rotated"]


async def test_concurrent_restart_triggers_restart_once(agent: Agent) -> None:
    sup = agent.units["media-tv"]
    calls = 0

    async def slow_restart() -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)

    sup.restart = slow_restart  # type: ignore[method-assign]
    first, second = agent.restart(sup), agent.restart(sup)
    assert first is second and sup.restarting
    report = await agent.check_health()
    assert report == {"media-tv": "restarting"}
    await first
    assert calls == 1 and not sup.restarting
