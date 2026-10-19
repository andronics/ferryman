"""Minimal sd_notify(3): READY/STOPPING/WATCHDOG to systemd when it asks."""

from __future__ import annotations

import os
import socket


def notify(message: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC) as s:
        s.connect(addr)
        s.sendall(message.encode())


def watchdog_interval() -> float | None:
    """Seconds between WATCHDOG=1 pings (half the configured timeout), or None."""
    usec = os.environ.get("WATCHDOG_USEC")
    pid = os.environ.get("WATCHDOG_PID")
    if not usec or (pid and int(pid) != os.getpid()):
        return None
    return int(usec) / 2_000_000
