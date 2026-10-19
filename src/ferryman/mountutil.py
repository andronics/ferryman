"""FUSE mountpoint helpers. Anything that touches the mounted path itself runs
in a killable subprocess with a timeout: a wedged FUSE endpoint blocks every
syscall on it, and one hung stat on the event loop would freeze every unit."""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

log = logging.getLogger(__name__)


def _unescape(field: str) -> str:
    # mountinfo octal-escapes space, tab, newline and backslash.
    out, i = [], 0
    while i < len(field):
        if field[i] == "\\" and len(field[i + 1 : i + 4]) == 3 and field[i + 1 : i + 4].isdigit():
            out.append(chr(int(field[i + 1 : i + 4], 8)))
            i += 4
        else:
            out.append(field[i])
            i += 1
    return "".join(out)


def is_mounted(path: Path, mountinfo: Path = Path("/proc/self/mountinfo")) -> bool:
    """True if something is mounted exactly at `path` (the kernel mount table
    only; a wedged endpoint is still 'mounted' - see `responsive`)."""
    target = str(path)
    with mountinfo.open() as f:
        for line in f:
            fields = line.split(" ")
            if len(fields) > 4 and _unescape(fields[4]) == target:
                return True
    return False


async def _run(*argv: str, timeout: float) -> int | None:
    """Exit code, or None if it timed out (the process is killed)."""
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
    )
    try:
        return await asyncio.wait_for(proc.wait(), timeout)
    except TimeoutError:
        proc.kill()
        return None


async def responsive(path: Path, timeout: float = 10.0) -> bool:
    return await _run("stat", "--", str(path), timeout=timeout) == 0


def _fusermount() -> str | None:
    return shutil.which("fusermount3") or shutil.which("fusermount")


async def detach(path: Path) -> bool:
    """Lazy unmount: detach the path now and let open handles drain."""
    fm = _fusermount()
    argv = (fm, "-uz", str(path)) if fm else ("umount", "-l", str(path))
    return await _run(*argv, timeout=30) == 0


async def unmount(path: Path) -> bool:
    """Unmount, falling back to a lazy detach when the mount is busy. A busy
    mount followed by killing rclone would leave a dead endpoint ("Transport
    endpoint is not connected") that blocks every restart."""
    if not is_mounted(path):
        return True
    fm = _fusermount()
    argv = (fm, "-u", str(path)) if fm else ("umount", str(path))
    if await _run(*argv, timeout=30) == 0:
        return True
    log.warning("mount busy, detaching lazily: %s", path)
    return await detach(path)


async def clear_stale(path: Path, timeout: float = 10.0) -> None:
    """Detach a mount at `path` that no longer answers (left by a crashed
    rclone). A live mount is left alone."""
    if is_mounted(path) and not await responsive(path, timeout):
        log.warning("detaching stale FUSE endpoint: %s", path)
        await detach(path)
