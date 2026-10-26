"""An agent's events: a short history, and every one to its sink (the
outbox); agents never notify anyone."""

from __future__ import annotations

from ferryman.notify import Event, Notifier


def test_events_are_kept_and_passed_on() -> None:
    n = Notifier("pve-01")
    got: list[Event] = []
    n.sink = got.append
    n.emit(Event("unit.failed", "tv", "boom", "error"))
    n.emit(Event("unit.recovered", "tv", "ok", "info", resolves=True))
    assert [e["kind"] for e in n.recent()] == ["unit.failed", "unit.recovered"]
    assert [e.kind for e in got] == ["unit.failed", "unit.recovered"]


def test_a_failing_sink_does_not_raise() -> None:
    n = Notifier("pve-01")

    def broken(_: Event) -> None:
        raise OSError("disk full")

    n.sink = broken
    n.emit(Event("unit.failed", "tv", "boom", "error"))
    assert len(n.recent()) == 1
