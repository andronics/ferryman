"""Rules that act: planned when they fire, delayed and cancelled, limited,
never on late events, carried out as commands (or a rotation), and kept
across a controller restart."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from ferryman.config import ControllerConfig
from ferryman.controller import Controller, token_hash
from tests.conftest import KEYS, write_keys

UNIT_FAILED = {"kind": "unit.failed"}


class Clock:
    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now


def config(tmp_path: Path, *rules: dict[str, Any], enabled: bool = True) -> ControllerConfig:
    write_keys(tmp_path / "keys", KEYS)
    return ControllerConfig.model_validate({
        "state": str(tmp_path / "co"),
        "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
        "units": {"tv": {"drive_id": "0A", "pool": "main"}},
        "agents": {"site-a": {"token_sha256": token_hash("token-a"), "units": {"tv": {}},
                              "transfers": {"tv-up": {"unit": "tv", "source": "/s"}}}},
        "notify": {"channels": {"ops": {"webhook": {"url": "http://h"}}}, "rules": list(rules)},
        "actions": {"enabled": enabled},
    })  # fmt: skip


def controller(tmp_path: Path, *rules: dict[str, Any], **kw: Any) -> tuple[Controller, Clock]:
    co = Controller(config(tmp_path, *rules, **kw))
    clock = Clock()
    co.alerts.clock = clock
    return co, clock


seq = [0]


def report(co: Controller, kind: str, subject: str = "tv", late: bool = False,
           severity: str = "error", resolves: bool = False) -> None:  # fmt: skip
    """An event from site-a, as its outbox delivers it."""
    seq[0] += 1
    at = time.time() - (3600 if late else 0)
    co.events.ingest("site-a", "box", [{"seq": seq[0], "kind": kind, "subject": subject,
                                        "message": "m", "severity": severity,
                                        "resolves": resolves, "at": at}])  # fmt: skip
    co.alerts.end_batch("site-a")


def kinds(co: Controller) -> list[str]:
    return [e["kind"] for e in co.events.recent if e["kind"].startswith("action.")]


def commands(co: Controller) -> list[tuple[str, str, str]]:
    return [(c["action"], c["target"], c["by"]) for c in co.commands]


async def test_a_crash_loop_restarts_and_is_limited(tmp_path: Path) -> None:
    co, clock = controller(tmp_path, {
        "name": "heal", "match": UNIT_FAILED, "send": ["ops"],
        "do": {"action": "restart", "limit": {"count": 2, "within": "1h"}}})  # fmt: skip
    report(co, "unit.failed")
    await co.alerts.due()
    assert commands(co) == [("restart", "tv", "rule:heal")]
    assert kinds(co) == ["action.taken"]
    # The notification says what it does.
    assert co.alerts.queues["ops"][0]["doing"] == "restart unit tv on site-a"
    for _ in range(2):  # recovered, failed again: each a new problem
        report(co, "unit.recovered", severity="info", resolves=True)
        report(co, "unit.failed")
        await co.alerts.due()
    assert len(commands(co)) == 2
    assert kinds(co)[-1] == "action.limited"
    # An hour later the limit has room again.
    clock.now += 3601
    report(co, "unit.recovered", severity="info", resolves=True)
    report(co, "unit.failed")
    await co.alerts.due()
    assert len(commands(co)) == 3


async def test_a_recovery_cancels_a_delayed_action(tmp_path: Path) -> None:
    rule = {"name": "heal", "match": UNIT_FAILED, "do": {"action": "restart", "delay": "2m"}}
    co, clock = controller(tmp_path, rule)
    report(co, "unit.failed")
    await co.alerts.due()
    assert commands(co) == [] and len(co.alerts.waiting) == 1  # not yet
    report(co, "unit.recovered", severity="info", resolves=True)
    clock.now += 121
    await co.alerts.due()
    assert commands(co) == [] and co.alerts.waiting == {}
    assert kinds(co) == ["action.cancelled"]
    # Not done, so it doesn't count against the limit.
    assert all(not v for v in co.alerts.taken.values())


async def test_a_delayed_action_survives_a_controller_restart(tmp_path: Path) -> None:
    rule = {"name": "heal", "match": UNIT_FAILED, "do": {"action": "restart", "delay": "2m"}}
    co, clock = controller(tmp_path, rule)
    report(co, "unit.failed")
    again = Controller(config(tmp_path, rule))
    again.alerts.clock = lambda: clock.now + 121
    await again.alerts.due()
    assert commands(again) == [("restart", "tv", "rule:heal")]
    await again.alerts.due()
    assert len(commands(again)) == 1  # once


async def test_no_action_on_a_late_event(tmp_path: Path) -> None:
    co, _ = controller(tmp_path, {"name": "heal", "match": UNIT_FAILED,
                                  "do": {"action": "restart"}})  # fmt: skip
    report(co, "unit.failed", late=True)
    await co.alerts.due()
    assert commands(co) == [] and kinds(co) == ["action.skipped"]


async def test_dry_run_and_actions_off_only_plan(tmp_path: Path) -> None:
    co, _ = controller(tmp_path, {"name": "heal", "match": UNIT_FAILED,
                                  "do": {"action": "restart", "dry_run": True}})  # fmt: skip
    report(co, "unit.failed")
    await co.alerts.due()
    assert commands(co) == [] and kinds(co) == ["action.planned"]
    off, _ = controller(tmp_path / "off", {"name": "heal", "match": UNIT_FAILED,
                                           "do": {"action": "restart"}}, enabled=False)  # fmt: skip
    report(off, "unit.failed")
    await off.alerts.due()
    assert commands(off) == [] and "actions are off" in off.events.recent[-1]["message"]


async def test_every_acts_again_while_the_problem_stays_open(tmp_path: Path) -> None:
    co, clock = controller(tmp_path, {"name": "heal", "match": UNIT_FAILED,
                                      "do": {"action": "restart", "every": "10m"}})  # fmt: skip
    report(co, "unit.failed")
    await co.alerts.due()
    report(co, "unit.failed")  # still open, too soon
    await co.alerts.due()
    assert len(commands(co)) == 1
    clock.now += 601
    report(co, "unit.failed")
    await co.alerts.due()
    assert len(commands(co)) == 2


async def test_retry_run_and_tune_are_commands(tmp_path: Path) -> None:
    co, _ = controller(
        tmp_path,
        {"name": "retry", "match": {"kind": "transfer.failed"}, "do": {"action": "retry"}},
        {"name": "catch-up", "match": {"kind": "unit.recovered"},
         "do": {"action": "run", "transfer": "tv-up", "agent": "{agent}"}},
        {"name": "retune", "match": {"kind": "tune.drift"}, "do": {"action": "tune"}},
    )  # fmt: skip
    report(co, "transfer.failed", subject="tv-up")
    report(co, "unit.recovered", severity="info", resolves=True)
    report(co, "tune.drift", subject="drive", severity="warning")
    await co.alerts.due()
    assert sorted(commands(co)) == [("run", "tv-up", "rule:catch-up"),
                                    ("run", "tv-up", "rule:retry"),
                                    ("tune", "drive", "rule:retune")]  # fmt: skip


async def test_rotate_keeps_the_current_keys_quota(tmp_path: Path) -> None:
    co, _ = controller(tmp_path, {"name": "quota", "match": {"kind": "transfer.failed"},
                                  "do": {"action": "rotate"}})  # fmt: skip
    pool = co.pools["main"]
    pool.current()
    report(co, "transfer.failed", subject="tv-up")
    await co.alerts.due()
    assert pool.current() == KEYS[1]
    assert not any(k["exhausted_until"] for k in pool.describe()["keys"])
    assert "now on" in co.events.recent[-1]["message"]


async def test_a_target_gone_since_planning_fails_cleanly(tmp_path: Path) -> None:
    co, _ = controller(tmp_path, {"name": "heal", "match": UNIT_FAILED,
                                  "do": {"action": "restart"}})  # fmt: skip
    report(co, "unit.failed", subject="films")  # not bound on site-a
    await co.alerts.due()
    assert commands(co) == [] and kinds(co) == ["action.failed"]
    assert "isn't bound" in co.events.recent[-1]["message"]
