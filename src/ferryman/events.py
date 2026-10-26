"""Events on their way to the controller, and the controller's store of them.

Agents never notify anyone. Every event an agent emits goes into its
outbox, a file in its state directory, numbered in order. A sender loop
posts the outbox to the controller and drops what the controller
acknowledges. Events wait there through a controller outage, and an agent
restart, and arrive late with their original times.

The controller stores every agent's events, and its own, in one log with a
bounded history; its rules (notifications) act on them. A batch sent twice
is stored once: each agent's events are kept by sequence number. An outbox
has a random id, so an agent whose state was wiped (its numbers start
again) is not mistaken for one repeating itself.
"""

from __future__ import annotations

import collections
import contextlib
import json
import logging
import os
import secrets
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .notify import Event

log = logging.getLogger(__name__)

OUTBOX_MAX_EVENTS = 10_000
OUTBOX_MAX_AGE = 7 * 86400.0
BATCH = 500
RECENT = 5000  # events the controller keeps in memory, newest last


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    os.replace(tmp, path)


class Outbox:
    """An agent's events not yet acknowledged by the controller, in order."""

    def __init__(self, path: Path, clock: Callable[[], float] = time.time) -> None:
        self.path = path
        self.meta = path.with_suffix(".meta")
        self.clock = clock
        self.entries: list[dict[str, Any]] = []
        with contextlib.suppress(FileNotFoundError):
            for line in path.read_text().splitlines():
                with contextlib.suppress(ValueError):
                    self.entries.append(json.loads(line))
        try:
            meta = json.loads(self.meta.read_text())
            self.id, self.seq = str(meta["id"]), int(meta["seq"])
        except (FileNotFoundError, ValueError, KeyError):
            self.id, self.seq = secrets.token_hex(8), 0
        self.seq = max([self.seq, *(e["seq"] for e in self.entries)])
        # Set on every append: the sender waits on it when there's nothing to send.
        self.wake: Any = None

    def append(self, event: Event) -> None:
        self._trim()
        self._add(asdict(event))

    def _add(self, fields: dict[str, Any]) -> None:
        self.seq += 1
        entry = {"seq": self.seq, **fields}
        self.entries.append(entry)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())
        _write_atomic(self.meta, json.dumps({"id": self.id, "seq": self.seq}))
        if self.wake is not None:
            self.wake.set()

    def _trim(self) -> None:
        """Keep the newest OUTBOX_MAX_EVENTS from the last OUTBOX_MAX_AGE,
        recording what was dropped as an event of its own."""
        cutoff = self.clock() - OUTBOX_MAX_AGE
        keep = [e for e in self.entries if e["at"] >= cutoff][-(OUTBOX_MAX_EVENTS - 2) :]
        dropped = len(self.entries) - len(keep)
        if not dropped:
            return
        kept = {e["seq"] for e in keep}
        gone = [e for e in self.entries if e["seq"] not in kept]
        self.entries = keep
        self._rewrite()
        span = (time.strftime("%F %T", time.localtime(gone[0]["at"])),
                time.strftime("%F %T", time.localtime(gone[-1]["at"])))  # fmt: skip
        log.warning("outbox full: dropped %d undelivered event(s)", dropped)
        self._add(asdict(Event(
            "events.dropped", "outbox",
            f"{dropped} event(s) from {span[0]} to {span[1]} dropped undelivered "
            "(the controller was unreachable too long)", "error",
        )))  # fmt: skip

    def pending(self, limit: int = BATCH) -> list[dict[str, Any]]:
        return self.entries[:limit]

    def ack(self, seq: int) -> None:
        """The controller has everything up to `seq`."""
        before = len(self.entries)
        self.entries = [e for e in self.entries if e["seq"] > seq]
        if len(self.entries) != before:
            self._rewrite()

    def _rewrite(self) -> None:
        _write_atomic(self.path, "".join(json.dumps(e) + "\n" for e in self.entries))


Listener = Callable[[dict[str, Any]], None]


class EventStore:
    """The controller's log of every agent's events and its own."""

    def __init__(self, state: Path, retention: float, clock: Callable[[], float] = time.time
                 ) -> None:  # fmt: skip
        self.path = state / "events.jsonl"
        self.seqs_path = state / "events-seq.json"
        self.retention = retention
        self.clock = clock
        self.recent: collections.deque[dict[str, Any]] = collections.deque(maxlen=RECENT)
        # agent -> {"id": its outbox's id, "seq": the last stored}
        self.seqs: dict[str, dict[str, Any]] = {}
        # Called with each stored event (the rules).
        self.listeners: list[Listener] = []
        with contextlib.suppress(FileNotFoundError, ValueError):
            self.seqs = json.loads(self.seqs_path.read_text())
        self.compact()

    def compact(self) -> None:
        """Drop events older than the retention, and load the rest."""
        cutoff = self.clock() - self.retention
        kept = []
        with contextlib.suppress(FileNotFoundError):
            for line in self.path.read_text().splitlines():
                with contextlib.suppress(ValueError):
                    e = json.loads(line)
                    if e.get("at", 0) >= cutoff:
                        kept.append(e)
        if self.path.exists():
            _write_atomic(self.path, "".join(json.dumps(e) + "\n" for e in kept))
        self.recent.clear()
        self.recent.extend(kept)

    def ingest(self, agent: str, outbox: str, events: list[dict[str, Any]]) -> int:
        """Store an agent's batch; returns the last sequence number stored."""
        known = self.seqs.get(agent)
        if known is None or known.get("id") != outbox:
            known = {"id": outbox, "seq": 0}  # a new outbox: its numbers start again
        last = int(known["seq"])
        now = self.clock()
        for e in sorted(events, key=lambda e: int(e["seq"])):
            seq = int(e["seq"])
            if seq <= last:
                continue  # already stored (a resend)
            self._store({**e, "agent": agent, "received": now})
            last = seq
        self.seqs[agent] = {"id": outbox, "seq": last}
        _write_atomic(self.seqs_path, json.dumps(self.seqs))
        return last

    def emit(self, kind: str, subject: str, message: str, severity: str = "warning",
             resolves: bool = False) -> None:  # fmt: skip
        """An event of the controller's own."""
        now = self.clock()
        log.log(logging.INFO if severity == "info" else logging.WARNING,
                "event %s %s: %s", kind, subject, message)  # fmt: skip
        self._store({"kind": kind, "subject": subject, "message": message,
                     "severity": severity, "resolves": resolves, "at": now,
                     "agent": None, "received": now})  # fmt: skip

    def _store(self, e: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a") as f:
            f.write(json.dumps(e) + "\n")
        self.recent.append(e)
        for listener in self.listeners:
            try:
                listener(e)
            except Exception:
                log.exception("event listener failed on %s", e.get("kind"))

    def latest(self, limit: int = 200) -> list[dict[str, Any]]:
        """Newest first, by when they happened."""
        return sorted(self.recent, key=lambda e: e["at"], reverse=True)[:limit]
