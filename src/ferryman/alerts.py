"""The controller's notifications: rules over every agent's events and its
own, sent through channels (SMTP, sendmail, a webhook).

A rule matches events (kind, severity, agent, subject) and, with `when`,
fires only once enough of them happen within a window: N events, or events
from N agents (`all-bound`: every agent bound to the unit). Windows count
when events happened, not when they arrived, so a backlog delivered at once
doesn't look like a burst.

A firing rule opens a problem (keyed by the rule, the event's family and
its `per` values). An open problem is sent once, then at most every
`repeat`; the matching recovery (`unit.recovered` for `unit.failed`, same
agent and subject) closes it, and is sent only if the problem was. Info
events that aren't recoveries open nothing: each match is sent.

What arrives late (an agent's backlog after it couldn't reach the
controller) waits for the end of its batch: a problem that opened and
closed entirely inside it is sent as one summary line, not an alarm and
its recovery. Everything for one channel within `batch` goes out as one
message: a network blip that restarts every unit on every host is one
email.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import fnmatch
import json
import logging
import os
import smtplib
import ssl
import time
from collections.abc import Callable
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import httpx

from .config import Channel, Notifications, Rule, RuleMatch
from .notify import SEVERITIES

log = logging.getLogger(__name__)

LATE = 120.0  # an event received this long after it happened came in a backlog
SEND_TIMEOUT = 30.0

Item = dict[str, Any]


def _who(e: dict[str, Any]) -> str:
    return str(e.get("agent") or "controller")


def _family(kind: str) -> str:
    return kind.split(".", 1)[0]


def _clock(t: float) -> str:
    return time.strftime("%F %T", time.localtime(t))


class Alerts:
    def __init__(self, cfg: Notifications, state: Path,
                 bound: Callable[[str], set[str]] = lambda _: set(),
                 clock: Callable[[], float] = time.time) -> None:  # fmt: skip
        self.cfg = cfg
        self.rules = cfg.effective_rules()
        self.path = state / "problems.json"
        # The agents bound to a unit (for `agents: all-bound`).
        self.bound = bound
        self.clock = clock
        self.problems: dict[str, dict[str, Any]] = {}
        with contextlib.suppress(FileNotFoundError, ValueError):
            self.problems = json.loads(self.path.read_text())
        # (rule, per values) -> recent matches: (when, agent)
        self.windows: dict[str, collections.deque[tuple[float, str]]] = {}
        self.queues: dict[str, list[Item]] = collections.defaultdict(list)
        # agent -> what its late events produced, held to the end of the batch
        self.replay: dict[str, list[Item]] = collections.defaultdict(list)
        self.rule_status: dict[str, dict[str, Any]] = {}
        self.channel_status: dict[str, dict[str, Any]] = {}
        self.wake: asyncio.Event | None = None
        self.transport: httpx.AsyncBaseTransport | None = None  # tests

    def update(self, cfg: Notifications) -> None:
        """A reloaded config. Open problems carry on; those of rules that are
        gone close quietly."""
        self.cfg, self.rules = cfg, cfg.effective_rules()
        names = {r.name for r in self.rules}
        gone = [k for k, p in self.problems.items() if p["rule"] not in names]
        for k in gone:
            del self.problems[k]
        if gone:
            self._save()

    # ------------------------------------------------------------ matching

    def _matches(self, m: RuleMatch, e: dict[str, Any]) -> bool:
        if m.kind and not fnmatch.fnmatchcase(e["kind"], m.kind):
            return False
        if m.severity and SEVERITIES.index(e["severity"]) < SEVERITIES.index(m.severity):
            return False
        if m.agent and not fnmatch.fnmatchcase(_who(e), m.agent):
            return False
        return not (m.subject and not fnmatch.fnmatchcase(e["subject"], m.subject))

    def _values(self, rule: Rule, e: dict[str, Any]) -> dict[str, str]:
        per = rule.when.per if rule.when and rule.when.per else ["agent", "subject"]
        return {f: _who(e) if f == "agent" else str(e[f]) for f in per}

    def _condition(self, rule: Rule, key: str, e: dict[str, Any]) -> bool:
        w = rule.when
        assert w is not None
        seen = self.windows.setdefault(key, collections.deque(maxlen=1000))
        seen.append((e["at"], _who(e)))
        while seen and seen[0][0] < e["at"] - w.within:
            seen.popleft()
        if w.count is not None:
            return len(seen) >= w.count
        agents = {a for _, a in seen}
        if w.agents == "all-bound":
            bound = self.bound(e["subject"])
            return bool(bound) and bound <= agents
        assert isinstance(w.agents, int)
        return len(agents) >= w.agents

    # ------------------------------------------------------------- events

    def on_event(self, e: dict[str, Any]) -> None:
        late = e.get("agent") is not None and e["received"] - e["at"] > LATE
        if e.get("resolves"):
            self._recover(e, late)
            return
        for rule in self.rules:
            if not self._matches(rule.match, e):
                continue
            values = self._values(rule, e)
            key = json.dumps([rule.name, _family(e["kind"]), *values.values()])
            if rule.when and not self._condition(rule, key, e):
                continue
            self._fire(rule, key, values, e, late)

    def _fire(self, rule: Rule, key: str, values: dict[str, str], e: dict[str, Any],
              late: bool) -> None:  # fmt: skip
        now = self.clock()
        self.rule_status[rule.name] = {"fired": now, "channels": rule.send}
        if e["severity"] == "info":
            self._out(rule.send, {"what": "event", "event": e}, late, e)
            return
        p = self.problems.get(key)
        if p is None:
            self.problems[key] = {"rule": rule.name, "family": _family(e["kind"]),
                                  "values": values, "opened": e["at"], "sent": now,
                                  "count": 1, "kind": e["kind"], "subject": e["subject"],
                                  "agent": _who(e)}  # fmt: skip
            self._out(rule.send, {"what": "problem", "event": e, "key": key}, late, e)
        else:
            p["count"] += 1
            if now - p["sent"] < self.cfg.repeat:
                self._save()
                return  # still open, and told recently
            p["sent"] = now
            self._out(rule.send, {"what": "still", "event": e, "key": key,
                                  "count": p["count"], "since": p["opened"]}, late, e)  # fmt: skip
        self._save()

    def _recover(self, e: dict[str, Any], late: bool) -> None:
        for key, p in list(self.problems.items()):
            if p["family"] != _family(e["kind"]):
                continue
            # The recovery's kind differs from the problem's (unit.recovered
            # vs unit.failed): only agent and subject must agree.
            if any(v != (_who(e) if f == "agent" else str(e[f]))
                   for f, v in p["values"].items() if f != "kind"):  # fmt: skip
                continue
            del self.problems[key]
            rule = next((r for r in self.rules if r.name == p["rule"]), None)
            if rule is not None:
                self._out(rule.send, {"what": "recovery", "event": e, "key": key,
                                      "since": p["opened"], "count": p["count"],
                                      "problem": p["kind"]}, late, e)  # fmt: skip
        self._save()

    def _out(self, channels: list[str], item: Item, late: bool, e: dict[str, Any]) -> None:
        item = {**item, "channels": channels}
        if late:
            self.replay[_who(e)].append(item)
        else:
            self._queue(item)

    def _queue(self, item: Item) -> None:
        for ch in item["channels"]:
            self.queues[ch].append(item)
        if self.wake is not None:
            self.wake.set()

    def end_batch(self, agent: str) -> None:
        """An agent's batch is stored: send what its late events produced,
        summing up the problems that opened and closed inside it."""
        items = self.replay.pop(agent, [])
        if not items:
            return
        closed = {i["key"] for i in items if i["what"] == "recovery"}
        opened = {i["key"] for i in items if i["what"] == "problem"}
        over = closed & opened
        summary: dict[str, list[str]] = collections.defaultdict(list)
        times = [i["event"]["at"] for i in items]
        for i in items:
            if i.get("key") in over:
                if i["what"] == "recovery":
                    e = i["event"]
                    for ch in i["channels"]:
                        summary[ch].append(f"{i['problem']} {e['subject']} x{i['count']}, "
                                           f"recovered {_clock(e['at'])}")  # fmt: skip
                continue
            self._queue(i)
        span = f"{_clock(min(times))} - {_clock(max(times))}"
        for ch, lines in summary.items():
            self._queue({"what": "summary", "channels": [ch], "agent": agent, "span": span,
                         "lines": lines, "event": {"at": max(times)}})  # fmt: skip

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.problems, indent=1, sort_keys=True))
        os.replace(tmp, self.path)

    # ------------------------------------------------------------ sending

    async def run(self) -> None:
        """Send what's queued, a `batch` after the first of it, until
        cancelled (then once more)."""
        self.wake = asyncio.Event()
        if any(self.queues.values()):
            self.wake.set()
        try:
            while True:
                await self.wake.wait()
                await asyncio.sleep(self.cfg.batch)
                self.wake.clear()
                await self.flush()
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(self.flush())
            raise

    async def flush(self) -> None:
        queues, self.queues = self.queues, collections.defaultdict(list)
        for name, items in queues.items():
            ch = self.cfg.channels.get(name)
            if ch is None or not items:
                continue
            subject, body = render(items)
            try:
                await self._send(ch, subject, body, items)
                self.channel_status[name] = {"sent": self.clock(), "error": None}
            except Exception as e:
                log.error("notify: channel %s failed: %s", name, e)
                self.channel_status[name] = {"sent": None, "error": str(e), "at": self.clock()}

    async def _send(self, ch: Channel, subject: str, body: str, items: list[Item]) -> None:
        if ch.webhook:
            payload = {"title": subject, "text": body,
                       "events": [i["event"] for i in items if "kind" in i["event"]]}  # fmt: skip
            async with httpx.AsyncClient(timeout=SEND_TIMEOUT, transport=self.transport) as c:
                r = await c.post(ch.webhook.url, json=payload, headers=ch.webhook.headers)
                r.raise_for_status()
            return
        msg = EmailMessage()
        msg["Subject"] = subject
        msg.set_content(body)
        if ch.smtp:
            msg["From"], msg["To"] = ch.smtp.sender, ", ".join(ch.smtp.to)
            await asyncio.to_thread(_smtp, ch, msg)
        elif ch.sendmail:
            m = ch.sendmail
            msg["From"] = m.sender or f"ferryman@{os.uname().nodename}"
            msg["To"] = ", ".join(m.to)
            await _sendmail(m.sendmail, msg)

    def describe(self) -> dict[str, Any]:
        return {"problems": sorted(self.problems.values(), key=lambda p: p["opened"]),
                "rules": self.rule_status, "channels": self.channel_status}  # fmt: skip


def render(items: list[Item]) -> tuple[str, str]:
    lines = []
    for i in items:
        e = i["event"]
        if i["what"] == "summary":
            lines.append(f"while {i['agent']} couldn't reach the controller ({i['span']}):")
            lines += [f"    {line}" for line in i["lines"]]
            continue
        head = f"{_clock(e['at'])}  {e['severity'].upper():7}  {_who(e)}: {e['kind']} " \
            f"{e['subject']}: {e['message']}"  # fmt: skip
        if i["what"] == "still":
            head += f"  (still, {i['count']}x since {_clock(i['since'])})"
        lines.append(head)
    alarms = [i for i in items if i["what"] in ("problem", "still")]
    recoveries = [i for i in items if i["what"] == "recovery"]
    if len(items) == 1 and items[0]["what"] != "summary":
        e = items[0]["event"]
        title = f"{e['kind'].replace('.', ' ')} {e['subject']} on {_who(e)}"
    else:
        parts = [f"{len(alarms)} problem(s)"] if alarms else []
        parts += [f"{len(recoveries)} recovered"] if recoveries else []
        title = ", ".join(parts) or f"{len(items)} notification(s)"
    return f"[ferryman] {title}", "\n".join(lines) + "\n"


def _smtp(ch: Channel, msg: EmailMessage) -> None:
    s = ch.smtp
    assert s is not None
    ctx = ssl.create_default_context()
    cls = smtplib.SMTP_SSL if s.ssl else smtplib.SMTP
    kw: dict[str, Any] = {"context": ctx} if s.ssl else {}
    with cls(s.host, s.port, timeout=SEND_TIMEOUT, **kw) as conn:
        if s.starttls:
            conn.starttls(context=ctx)
        if s.user and s.pass_file:
            conn.login(s.user, s.pass_file.read_text().strip())
        conn.send_message(msg)


async def _sendmail(binary: str, msg: EmailMessage) -> None:
    proc = await asyncio.create_subprocess_exec(
        binary, "-t", "-oi",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip
    try:
        _, err = await asyncio.wait_for(proc.communicate(msg.as_bytes()), SEND_TIMEOUT)
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"{binary} timed out") from None
    if proc.returncode:
        raise RuntimeError(f"{binary} exited {proc.returncode}: {err.decode().strip()}")
