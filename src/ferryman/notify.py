"""Events and the notifier: state changes worth a human's attention, sent by
mail (the host's sendmail) and/or a JSON webhook.

Every event is kept in a short in-memory history (`ferryman events`, the
controller's UI). Only those at or above `notify.level` are sent, and not
one by one: a network blip restarts every unit at once, so events are
collected for `batch` seconds and go out as one message. A problem that
stays open (a unit crash-looping, a transfer failing every run) is sent once
and then at most every `repeat` seconds, and its recovery is sent only if
the problem was.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from email.message import EmailMessage
from typing import Any

import httpx

from .config import Notify

log = logging.getLogger(__name__)

SEVERITIES = ("info", "warning", "error")
HISTORY = 200
SEND_TIMEOUT = 30.0


@dataclass(frozen=True)
class Event:
    # <family>.<what>: unit.failed, transfer.recovered, pool.rotated, ...
    kind: str
    subject: str  # the unit, transfer or pool it is about
    message: str
    severity: str = "warning"
    # A recovery: closes the problem open on (family, subject).
    resolves: bool = False
    at: float = field(default_factory=time.time)

    @property
    def family(self) -> str:
        return self.kind.split(".", 1)[0]

    def line(self) -> str:
        return f"{self.kind} {self.subject}: {self.message}"


Emit = Callable[[Event], None]


def discard(_: Event) -> None:
    pass


class Notifier:
    def __init__(self, cfg: Notify, hostname: str) -> None:
        self.cfg = cfg
        self.hostname = hostname
        self.history: collections.deque[Event] = collections.deque(maxlen=HISTORY)
        self._queue: list[Event] = []
        self._wake = asyncio.Event()
        # (family, subject) -> when that open problem was last sent
        self._open: dict[tuple[str, str], float] = {}
        self.transport: httpx.AsyncBaseTransport | None = None  # tests
        # Where every event also goes: an agent's outbox, for the controller.
        self.sink: Callable[[Event], None] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.mail or self.cfg.webhook)

    def emit(self, event: Event) -> None:
        self.history.append(event)
        log.debug("event %s", event.line())
        if self.sink is not None:
            self.sink(event)
        if not self.enabled or not self._wanted(event):
            return
        self._queue.append(event)
        self._wake.set()

    def _wanted(self, e: Event) -> bool:
        key = (e.family, e.subject)
        if e.resolves:
            # Only recoveries from problems someone was told about.
            return self._open.pop(key, None) is not None
        if SEVERITIES.index(e.severity) < SEVERITIES.index(self.cfg.level):
            return False
        if e.severity == "info":
            return True  # informational: nothing opens
        last = self._open.get(key)
        if last is not None and e.at - last < self.cfg.repeat:
            return False
        self._open[key] = e.at
        return True

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        return [asdict(e) for e in list(self.history)[-limit:]]

    async def run(self) -> None:
        """Send queued events, batched, until cancelled (flushing first)."""
        try:
            while True:
                await self._wake.wait()
                await asyncio.sleep(self.cfg.batch)
                await self.flush()
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(self.flush())
            raise

    async def flush(self) -> None:
        events, self._queue = self._queue, []
        self._wake.clear()
        if not events:
            return
        subject, body = self.render(events)
        if self.cfg.mail:
            try:
                await self._mail(subject, body)
            except Exception as e:
                log.error("notify: mail failed: %s", e)
        if self.cfg.webhook:
            try:
                await self._webhook(subject, body, events)
            except Exception as e:
                log.error("notify: webhook failed: %s", e)

    def render(self, events: list[Event]) -> tuple[str, str]:
        if len(events) == 1:
            e = events[0]
            title = f"{e.kind.replace('.', ' ')} {e.subject}"
        else:
            problems = sum(not e.resolves and e.severity != "info" for e in events)
            recoveries = sum(e.resolves for e in events)
            parts = [f"{problems} problem(s)"] if problems else []
            parts += [f"{recoveries} recovered"] if recoveries else []
            title = ", ".join(parts) or f"{len(events)} event(s)"
        lines = [
            f"{time.strftime('%F %T', time.localtime(e.at))}  {e.severity.upper():7}  {e.line()}"
            for e in events
        ]
        return f"[ferryman] {self.hostname}: {title}", "\n".join(lines) + "\n"

    async def _mail(self, subject: str, body: str) -> None:
        assert self.cfg.mail is not None
        m = self.cfg.mail
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["To"] = ", ".join(m.to)
        msg["From"] = m.sender or f"ferryman@{self.hostname}"
        msg.set_content(body)
        proc = await asyncio.create_subprocess_exec(
            m.sendmail, "-t", "-oi",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )  # fmt: skip
        try:
            _, err = await asyncio.wait_for(proc.communicate(msg.as_bytes()), SEND_TIMEOUT)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise RuntimeError(f"{m.sendmail} timed out") from None
        if proc.returncode:
            raise RuntimeError(f"{m.sendmail} exited {proc.returncode}: {err.decode().strip()}")

    async def _webhook(self, subject: str, body: str, events: list[Event]) -> None:
        assert self.cfg.webhook is not None
        w = self.cfg.webhook
        payload = {
            "host": self.hostname,
            "title": subject,
            "text": body,
            "events": [asdict(e) for e in events],
        }
        async with httpx.AsyncClient(timeout=SEND_TIMEOUT, transport=self.transport) as c:
            r = await c.post(w.url, json=payload, headers=w.headers)
            r.raise_for_status()
