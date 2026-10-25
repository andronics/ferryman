"""Events, the notifier's dedupe/batching, and its mail and webhook sinks."""

from __future__ import annotations

import json
import stat
from pathlib import Path

import httpx

from ferryman.config import Notify
from ferryman.notify import Event, Notifier


def notifier(**cfg: object) -> Notifier:
    return Notifier(Notify.model_validate({"webhook": {"url": "http://hook"}, **cfg}), "pve-01")


def test_open_problems_are_sent_once_and_recoveries_only_after_them() -> None:
    n = notifier()
    n.emit(Event("unit.failed", "media-tv", "rclone exited with 1", "error", at=1000))
    n.emit(Event("unit.failed", "media-tv", "rclone exited with 1", "error", at=1010))
    n.emit(Event("unit.recovered", "media-tv", "ready again", "info", resolves=True))
    # A recovery nobody was told about the problem of is not news.
    n.emit(Event("unit.recovered", "media-movies", "ready again", "info", resolves=True))
    # Repeats after `repeat` seconds while still open.
    n.emit(Event("transfer.failed", "dump", "boom", "error", at=1000))
    n.emit(Event("transfer.failed", "dump", "boom", "error", at=1000 + 21601))
    assert [(e.kind, e.subject) for e in n._queue] == [
        ("unit.failed", "media-tv"),
        ("unit.recovered", "media-tv"),
        ("transfer.failed", "dump"),
        ("transfer.failed", "dump"),
    ]
    assert len(n.history) == 6  # everything is kept for `ferryman events`


def test_level_filters_what_is_sent() -> None:
    n = notifier(level="error")
    n.emit(Event("pool.rotated", "main", "now on sa-02", "info"))
    n.emit(Event("warm.failed", "media-tv", "deadline", "warning"))
    n.emit(Event("warm.done", "media-tv", "took 3s", "info", resolves=True))  # never opened
    assert n._queue == []
    n = notifier(level="info")
    n.emit(Event("pool.rotated", "main", "now on sa-02", "info"))
    n.emit(Event("pool.rotated", "main", "now on sa-03", "info"))  # info never dedupes
    assert len(n._queue) == 2


def test_disabled_notifier_only_records() -> None:
    n = Notifier(Notify(), "pve-01")
    assert not n.enabled
    n.emit(Event("unit.failed", "x", "y", "error"))
    assert n._queue == [] and len(n.history) == 1


async def test_batch_goes_out_as_one_webhook_post() -> None:
    posts: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(json.loads(request.content))
        return httpx.Response(200)

    n = notifier()
    n.transport = httpx.MockTransport(handler)
    n.emit(Event("unit.failed", "media-tv", "rclone exited with 1", "error"))
    n.emit(Event("transfer.failed", "dump", "unit backups is not ready", "error"))
    n.emit(Event("unit.recovered", "media-tv", "ready again", "info", resolves=True))
    await n.flush()
    assert len(posts) == 1
    body = posts[0]
    assert body["title"] == "[ferryman] pve-01: 2 problem(s), 1 recovered"
    assert "transfer.failed dump: unit backups is not ready" in str(body["text"])
    kinds = [e["kind"] for e in body["events"]]  # type: ignore[attr-defined]
    assert kinds == ["unit.failed", "transfer.failed", "unit.recovered"]
    await n.flush()  # nothing queued: nothing sent
    assert len(posts) == 1


async def test_mail_goes_through_sendmail(tmp_path: Path) -> None:
    out = tmp_path / "mail.eml"
    sendmail = tmp_path / "sendmail"
    sendmail.write_text(f'#!/bin/sh\necho "$@" > {out}.args\ncat > {out}\n')
    sendmail.chmod(sendmail.stat().st_mode | stat.S_IXUSR)
    n = Notifier(
        Notify.model_validate(
            {"mail": {"to": ["ops@example.org"], "from": "ferry@example.org",
                      "sendmail": str(sendmail)}}
        ),
        "pve-01",
    )  # fmt: skip
    n.emit(Event("pool.exhausted", "main", "every key is exhausted", "error"))
    await n.flush()
    mail = out.read_text()
    assert "Subject: [ferryman] pve-01: pool exhausted main" in mail
    assert "To: ops@example.org" in mail and "From: ferry@example.org" in mail
    assert "pool.exhausted main: every key is exhausted" in mail
    assert (tmp_path / "mail.eml.args").read_text().split() == ["-t", "-oi"]


async def test_a_failing_sink_does_not_raise(tmp_path: Path) -> None:
    n = Notifier(Notify.model_validate({"mail": {"to": ["x@y"], "sendmail": "/bin/false"}}), "h")
    n.emit(Event("unit.failed", "u", "m", "error"))
    await n.flush()  # logged, not raised
