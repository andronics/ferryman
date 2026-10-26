"""The controller's notifications: rules, problems and recoveries, batching
per channel, backlogs summed up, and the channels themselves."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from ferryman.alerts import LATE, Alerts, render
from ferryman.config import Notifications

NOW = 1_800_000_000.0


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> float:
        return self.now


def ev(
    kind: str,
    subject: str = "tv",
    agent: str | None = "pve-01",
    severity: str = "error",
    at: float = NOW,
    received: float | None = None,
    resolves: bool = False,
) -> dict[str, Any]:
    return {"kind": kind, "subject": subject, "message": "m", "severity": severity, "at": at,
            "agent": agent, "received": at if received is None else received,
            "resolves": resolves}  # fmt: skip


def alerts(tmp_path: Path, clock: Clock | None = None, **cfg: Any) -> Alerts:
    data = {"channels": {"ops": {"webhook": {"url": "http://hook"}},
                         "ha": {"webhook": {"url": "http://ha"}}}, **cfg}  # fmt: skip
    return Alerts(Notifications.model_validate(data), tmp_path, clock=clock or Clock())


def sent(a: Alerts, channel: str = "ops") -> list[str]:
    return [i["what"] for i in a.queues[channel]]


def test_without_rules_every_warning_goes_everywhere(tmp_path: Path) -> None:
    """The default: what a 0.2 agent did on its own."""
    a = alerts(tmp_path)
    a.on_event(ev("unit.failed"))
    a.on_event(ev("pool.rotated", "uploads", severity="info"))  # below warning
    assert sent(a, "ops") == sent(a, "ha") == ["problem"]


def test_an_open_problem_is_sent_once_then_every_repeat(tmp_path: Path) -> None:
    clock = Clock()
    a = alerts(tmp_path, clock, repeat="1h")
    a.on_event(ev("unit.failed"))
    a.on_event(ev("unit.failed"))  # still open, told just now
    assert sent(a) == ["problem"]
    clock.now += 3601
    a.on_event(ev("unit.failed", at=clock.now))
    assert sent(a) == ["problem", "still"]
    # It survives a controller restart, as open.
    again = alerts(tmp_path, clock, repeat="1h")
    again.on_event(ev("unit.failed", at=clock.now))
    assert sent(again) == []


def test_a_recovery_closes_its_own_problem_only(tmp_path: Path) -> None:
    a = alerts(tmp_path)
    a.on_event(ev("unit.failed", "tv"))
    a.on_event(ev("unit.failed", "tv", agent="emby-01"))
    a.on_event(ev("unit.recovered", "tv", severity="info", resolves=True))
    assert sent(a) == ["problem", "problem", "recovery"]
    assert [p["agent"] for p in a.problems.values()] == ["emby-01"]
    # A recovery nobody was told the problem of is not sent.
    a.on_event(ev("unit.recovered", "films", severity="info", resolves=True))
    assert sent(a) == ["problem", "problem", "recovery"]


def test_count_within_a_window_per_agent_and_unit(tmp_path: Path) -> None:
    rule = {"name": "crash-loop", "match": {"kind": "unit.failed"},
            "when": {"count": 3, "within": "15m"}, "send": ["ops"]}  # fmt: skip
    a = alerts(tmp_path, rules=[rule])
    for t in (0, 600, 1200):  # 3 in 20 minutes: never 3 within 15
        a.on_event(ev("unit.failed", at=NOW + t))
    assert sent(a) == []
    a.on_event(ev("unit.failed", at=NOW + 1300))
    assert sent(a) == ["problem"]
    # Another agent's failures count apart.
    a.on_event(ev("unit.failed", agent="emby-01", at=NOW + 1300))
    assert sent(a) == ["problem"]


def test_a_unit_down_on_every_agent_bound_to_it(tmp_path: Path) -> None:
    a = Alerts(
        Notifications.model_validate({
            "channels": {"ha": {"webhook": {"url": "http://ha"}}},
            "rules": [{"name": "everywhere", "match": {"kind": "unit.failed"},
                       "when": {"agents": "all-bound", "within": 300}, "send": ["ha"]}],
        }),
        tmp_path, bound=lambda unit: {"pve-01", "emby-01"} if unit == "tv" else set(),
        clock=Clock(),
    )  # fmt: skip
    a.on_event(ev("unit.failed", agent="pve-01"))
    a.on_event(ev("unit.failed", agent="pve-01", at=NOW + 10))
    assert sent(a, "ha") == []
    a.on_event(ev("unit.failed", agent="emby-01", at=NOW + 20))
    assert sent(a, "ha") == ["problem"]


def test_match_by_glob_agent_and_severity(tmp_path: Path) -> None:
    a = alerts(tmp_path, rules=[
        {"name": "units", "match": {"kind": "unit.*", "agent": "talos-*"}, "send": ["ops"]},
        {"name": "quiet", "match": {"kind": "agent.silent", "agent": "controller"},
         "send": ["ha"]},
        {"name": "rotations", "match": {"kind": "pool.rotated"}, "send": ["ha"]},
    ])  # fmt: skip
    a.on_event(ev("unit.failed", agent="pve-01"))
    a.on_event(ev("unit.failed", agent="talos-02"))
    a.on_event(ev("agent.silent", "talos-03", agent=None, severity="warning"))
    a.on_event(ev("pool.rotated", "uploads", severity="info"))
    a.on_event(ev("pool.rotated", "uploads", severity="info"))  # info: each one
    assert sent(a, "ops") == ["problem"]
    assert sent(a, "ha") == ["problem", "event", "event"]
    a.on_event(ev("agent.recovered", "talos-03", agent=None, severity="info", resolves=True))
    assert sent(a, "ha")[-1] == "recovery"


def test_a_backlog_that_failed_and_recovered_is_one_summary(tmp_path: Path) -> None:
    a = alerts(tmp_path)
    late = NOW + LATE + 600  # when the backlog arrives
    a.on_event(ev("unit.failed", at=NOW, received=late))
    a.on_event(ev("unit.failed", at=NOW + 60, received=late))
    a.on_event(ev("unit.recovered", severity="info", resolves=True, at=NOW + 120, received=late))
    a.on_event(ev("transfer.failed", "tv-up", at=NOW + 130, received=late))  # still open
    assert sent(a) == []  # held to the end of the batch
    a.end_batch("pve-01")
    assert sent(a) == ["problem", "summary"]
    _, body = render(a.queues["ops"])
    assert "while pve-01 couldn't reach the controller" in body
    assert "unit.failed tv x2, recovered" in body
    assert "transfer.failed tv-up" in body


def test_rules_are_checked_against_channels(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="unknown channel 'nope'"):
        alerts(tmp_path, rules=[{"name": "x", "send": ["nope"]}])
    with pytest.raises(ValidationError, match="count or agents"):
        alerts(tmp_path, rules=[{"name": "x", "when": {"within": 60}, "send": ["ops"]}])
    with pytest.raises(ValidationError, match="exactly one of"):
        Notifications.model_validate({"channels": {"x": {}}})
    with pytest.raises(ValidationError, match="invalid duration"):
        alerts(tmp_path, repeat="soon")


def test_a_reload_closes_the_problems_of_rules_that_are_gone(tmp_path: Path) -> None:
    a = alerts(tmp_path)
    a.on_event(ev("unit.failed"))
    assert a.problems
    a.update(Notifications.model_validate(
        {"channels": {"ops": {"webhook": {"url": "http://hook"}}},
         "rules": [{"name": "other", "match": {"kind": "pool.*"}, "send": ["ops"]}]}))  # fmt: skip
    assert a.problems == {}
    assert json.loads((tmp_path / "problems.json").read_text()) == {}


# ---------------------------------------------------------------- sending


async def test_one_message_per_channel(tmp_path: Path) -> None:
    posts: list[tuple[str, dict[str, Any]]] = []

    def hook(request: httpx.Request) -> httpx.Response:
        posts.append((str(request.url), json.loads(request.content)))
        return httpx.Response(200)

    a = alerts(tmp_path)
    a.transport = httpx.MockTransport(hook)
    a.on_event(ev("unit.failed", "tv"))
    a.on_event(ev("unit.failed", "films", agent="emby-01"))
    await a.flush()
    assert sorted(u for u, _ in posts) == ["http://ha", "http://hook"]
    payload = dict(posts)["http://hook"]
    assert payload["title"] == "[ferryman] 2 problem(s)"
    assert [e["subject"] for e in payload["events"]] == ["tv", "films"]
    assert a.channel_status["ops"]["error"] is None


async def test_a_failing_channel_is_recorded_not_raised(tmp_path: Path) -> None:
    a = alerts(tmp_path)
    a.transport = httpx.MockTransport(lambda r: httpx.Response(500))
    a.on_event(ev("unit.failed"))
    await a.flush()
    assert "500" in a.channel_status["ops"]["error"]


async def test_sendmail_channel(tmp_path: Path) -> None:
    out = tmp_path / "mail"
    sendmail = tmp_path / "sendmail"
    sendmail.write_text(f"#!/bin/sh\ncat > {out}\n")
    sendmail.chmod(0o755)
    a = Alerts(Notifications.model_validate(
        {"channels": {"m": {"sendmail": {"sendmail": str(sendmail), "to": ["ops@x"],
                                         "from": "ferryman@x"}}}}), tmp_path)  # fmt: skip
    a.on_event(ev("unit.failed"))
    await a.flush()
    mail = out.read_text()
    assert "Subject: [ferryman] unit failed tv on pve-01" in mail and "To: ops@x" in mail


async def test_smtp_channel(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            seen["server"] = (host, port)

        def __enter__(self) -> FakeSMTP:
            return self

        def __exit__(self, *_: Any) -> None:
            pass

        def starttls(self, context: Any) -> None:
            seen["tls"] = True

        def login(self, user: str, password: str) -> None:
            seen["login"] = (user, password)

        def send_message(self, msg: Any) -> None:
            seen["to"] = msg["To"]

    monkeypatch.setattr("ferryman.alerts.smtplib.SMTP", FakeSMTP)
    (tmp_path / "pass").write_text("s3cret\n")
    a = Alerts(Notifications.model_validate({"channels": {"m": {"smtp": {
        "host": "smtp.x", "user": "ferryman", "pass_file": str(tmp_path / "pass"),
        "from": "ferryman@x", "to": ["ops@x", "me@x"]}}}}), tmp_path)  # fmt: skip
    a.on_event(ev("unit.failed"))
    await a.flush()
    assert seen == {"server": ("smtp.x", 587), "tls": True, "login": ("ferryman", "s3cret"),
                    "to": "ops@x, me@x"}  # fmt: skip


def test_render_one_and_many() -> None:
    one = [{"what": "problem", "event": ev("unit.failed"), "channels": ["ops"]}]
    assert render(one)[0] == "[ferryman] unit failed tv on pve-01"
    back = ev("unit.recovered", agent="emby-01", severity="info")
    many = [*one, {"what": "recovery", "event": back, "channels": ["ops"]}]
    title, body = render(many)
    assert title == "[ferryman] 1 problem(s), 1 recovered"
    assert body.count("\n") == 2 and time.strftime("%F", time.localtime(NOW)) in body
