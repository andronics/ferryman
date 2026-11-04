"""Events: state changes worth a human's attention, as an agent sees them.

An agent keeps a short history of its events (`ferryman events`) and, with
a controller, passes every one to its outbox (events.py), which delivers
them to the controller. Agents never notify anyone: the controller decides
what becomes a notification (alerts.py).
"""

from __future__ import annotations

import collections
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

log = logging.getLogger(__name__)

SEVERITIES = ("info", "warning", "error")
HISTORY = 200


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
    """An agent's events: a short history, and a sink (the outbox)."""

    def __init__(self, hostname: str) -> None:
        self.hostname = hostname
        self.history: collections.deque[Event] = collections.deque(maxlen=HISTORY)
        self.sink: Callable[[Event], None] | None = None

    def emit(self, event: Event) -> None:
        self.history.append(event)
        log.debug("event %s", event.line())
        if self.sink is not None:
            try:
                self.sink(event)
            except OSError as e:  # the outbox's disk: never fail the caller
                log.error("event %s not stored for the controller: %s", event.kind, e)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        return [asdict(e) for e in list(self.history)[-limit:]]
