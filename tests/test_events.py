"""Events: an agent's outbox, its delivery to the controller through outages,
the controller's store, and the events the controller raises itself."""

from __future__ import annotations

import asyncio
import json
import stat
import time
from pathlib import Path

import httpx
import pytest

from ferryman import events as ev
from ferryman.agent import Agent
from ferryman.bootstrap import start_config
from ferryman.config import AgentBootstrap, Notifications
from ferryman.controller import Controller, build_app
from ferryman.events import EventStore, Outbox
from ferryman.notify import Event
from tests.test_bootstrap import KEYS, Switch, client, controller_config


def event(kind: str = "unit.failed", subject: str = "tv", at: float | None = None) -> Event:
    e = Event(kind, subject, "boom", "error")
    return e if at is None else Event(kind, subject, "boom", "error", at=at)


# ------------------------------------------------------------------ outbox


def test_the_outbox_is_durable_and_numbered(tmp_path: Path) -> None:
    path = tmp_path / "outbox.jsonl"
    box = Outbox(path)
    box.append(event())
    box.append(event("unit.recovered"))
    assert [e["seq"] for e in box.pending()] == [1, 2]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    # A restart: same entries, same id, numbering carries on.
    again = Outbox(path)
    assert again.id == box.id and [e["kind"] for e in again.pending()] == [
        "unit.failed", "unit.recovered"]  # fmt: skip
    again.ack(1)
    again.append(event())
    assert [e["seq"] for e in Outbox(path).pending()] == [2, 3]
    # Everything acknowledged: still numbered on from where it was.
    again.ack(3)
    assert Outbox(path).seq == 3 and Outbox(path).pending() == []


def test_a_full_outbox_drops_the_oldest_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ev, "OUTBOX_MAX_EVENTS", 5)
    box = Outbox(tmp_path / "o.jsonl")
    for i in range(8):
        box.append(event(subject=f"u{i}"))
    kinds = [e["kind"] for e in box.pending()]
    assert len(kinds) <= 5 and "events.dropped" in kinds
    assert box.pending()[-1]["subject"] == "u7"
    # Too old: dropped as well.
    old = Outbox(tmp_path / "p.jsonl", clock=lambda: 1e9)
    old.append(event(at=1e9 - 8 * 86400))
    old.append(event(at=1e9))
    assert [e["kind"] for e in old.pending()][-2:] == ["events.dropped", "unit.failed"]


# ------------------------------------------------------------------- store


def test_the_store_keeps_each_event_once(tmp_path: Path) -> None:
    store = EventStore(tmp_path, retention=86400)
    batch = [{"seq": 1, **json.loads(json.dumps(event().__dict__))},
             {"seq": 2, **event("unit.recovered").__dict__}]  # fmt: skip
    assert store.ingest("pve-01", "box-a", batch) == 2
    assert store.ingest("pve-01", "box-a", batch) == 2  # a resend
    assert len(store.recent) == 2
    # A new outbox (the agent's state was wiped): its numbers start again.
    assert store.ingest("pve-01", "box-b", [{"seq": 1, **event().__dict__}]) == 1
    assert len(store.recent) == 3
    # Kept across a controller restart, newest first.
    again = EventStore(tmp_path, retention=86400)
    assert [e["agent"] for e in again.latest()] == ["pve-01"] * 3
    assert again.seqs["pve-01"] == {"id": "box-b", "seq": 1}


def test_the_store_forgets_after_its_retention(tmp_path: Path) -> None:
    store = EventStore(tmp_path, retention=86400)
    store.ingest("a", "x", [{"seq": 1, **event(at=time.time() - 2 * 86400).__dict__},
                            {"seq": 2, **event().__dict__}])  # fmt: skip
    assert len(EventStore(tmp_path, retention=86400).recent) == 1


# ------------------------------------------------- delivery through outages


async def test_events_wait_out_an_outage_and_arrive_in_order(tmp_path: Path) -> None:
    co = Controller(controller_config(tmp_path))
    switch = Switch(httpx.ASGITransport(app=build_app(co)))
    (tmp_path / "token").write_text("token-a")
    boot = AgentBootstrap.model_validate({
        "controller": {"url": "http://co", "token_file": str(tmp_path / "token")},
        "paths": {k: str(tmp_path / "agent" / k) for k in ("runtime", "cache", "state", "logs")},
    })  # fmt: skip
    start = await start_config(boot, client(switch))
    agent = Agent(start.cfg, controller_transport=switch, bootstrap=boot, start=start)
    assert agent.outbox is not None and agent.notifier.sink is not None
    switch.down = True
    sender = asyncio.create_task(agent._events_loop())
    for i in range(3):
        agent.notifier.emit(Event("unit.failed", "backups", f"try {i}", "error"))
    await asyncio.sleep(0.05)
    assert len(agent.outbox.entries) == 3  # nowhere to go
    # The agent restarts during the outage: the outbox survives.
    sender.cancel()
    agent2 = Agent(start.cfg, controller_transport=switch, bootstrap=boot, start=start)
    assert agent2.outbox is not None and len(agent2.outbox.entries) == 3
    switch.down = False
    sender = asyncio.create_task(agent2._events_loop())
    async with asyncio.timeout(2):
        while agent2.outbox.entries:
            await asyncio.sleep(0.01)
    agent2.notifier.emit(Event("unit.recovered", "backups", "ok", "info", resolves=True))
    async with asyncio.timeout(2):
        while len(co.events.recent) < 4:
            await asyncio.sleep(0.01)
    sender.cancel()
    got = [(e["agent"], e["message"]) for e in co.events.recent]
    assert got == [("site-a", "try 0"), ("site-a", "try 1"), ("site-a", "try 2"),
                   ("site-a", "ok")]  # fmt: skip


# ------------------------------------------------- the controller's own


def kinds(co: Controller) -> list[str]:
    return [e["kind"] for e in co.events.recent if e["agent"] is None]


async def test_silent_agents_and_their_return(tmp_path: Path) -> None:
    co = Controller(controller_config(tmp_path))
    co.check_agents()
    assert kinds(co) == []
    co.started -= 3600  # never heard from, for an hour
    co.check_agents()
    co.check_agents()  # raised once
    assert kinds(co) == ["agent.silent"]
    co.report("site-a", {})
    assert kinds(co) == ["agent.silent", "agent.recovered"]


async def test_rejected_configs(tmp_path: Path) -> None:
    co = Controller(controller_config(tmp_path))
    rejected = {"config": {"state": "rejected", "version": "1", "offered": "2", "error": "no"}}
    co.report("site-a", rejected)
    co.report("site-a", rejected)  # the same one: raised once
    co.report("site-a", {"config": {"state": "applied", "version": "3"}})
    assert kinds(co) == ["agent.config_rejected", "agent.config_applied"]
    assert "version 2 refused, 1 stays in force: no" in co.events.recent[0]["message"]


async def test_an_invalid_reload_and_its_fix(tmp_path: Path) -> None:
    path = tmp_path / "controller.yaml"
    path.write_text("units: {nope: [}")
    co = Controller(controller_config(tmp_path), path)
    await co.check_config(force=True)
    assert kinds(co) == ["config.invalid"]
    data = controller_config(tmp_path).model_dump(mode="json")
    path.write_text(json.dumps(data))
    await co.check_config(force=True)
    assert kinds(co) == ["config.invalid", "config.reloaded"]


async def test_an_exhausted_pool_and_its_recovery(tmp_path: Path) -> None:
    co = Controller(controller_config(tmp_path))
    pool = co.pools["main"]
    pool.current()
    for key in KEYS[:2]:
        await co.exhausted("site-a", pool, key)
    with pytest.raises(Exception, match="every key is exhausted"):
        await co.exhausted("site-a", pool, KEYS[2])
    co.check_pools()
    assert kinds(co) == ["pool.exhausted"]
    pool.clock = lambda: time.time() + 2 * 86400  # the window has passed
    co.check_pools()
    assert kinds(co) == ["pool.exhausted", "pool.recovered"]


async def test_a_backlog_through_the_api_is_summed_up(tmp_path: Path) -> None:
    notify = Notifications.model_validate({"channels": {"ops": {"webhook": {"url": "http://h"}}}})
    co = Controller(controller_config(tmp_path).model_copy(update={"notify": notify}))
    c = client(httpx.ASGITransport(app=build_app(co)))
    t0 = time.time() - 3600  # an hour-old backlog
    batch = [
        {"seq": 1, **Event("unit.failed", "backups", "down", "error", at=t0).__dict__},
        {"seq": 2, **Event("unit.recovered", "backups", "up", "info", True, at=t0 + 60).__dict__},
    ]
    r = await c.request("POST", "/v2/events", json={"outbox": "x", "events": batch})
    assert r == {"acked": 2}
    assert [i["what"] for i in co.alerts.queues["ops"]] == ["summary"]
    assert co.alerts.problems == {}
