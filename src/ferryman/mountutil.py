"""FUSE mountpoint helpers. Anything that touches the mounted path itself runs
in a killable subprocess with a timeout: a wedged FUSE endpoint blocks every
syscall on it, and one hung stat on the event loop would freeze every unit."""

from __future__ import annotations

import asyncio
import logging
import os
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


def make_mountpoint(path: Path, uid: int | None, gid: int | None, mode: int) -> None:
    """Create the mount directory itself, owned by the unit and with `mode`
    (the agent runs as root, so a plain mkdir would leave it root:root
    0755). Never its parents: mount paths are absolute, and a typo in one
    should fail here rather than create a tree at the filesystem root."""
    if path.is_dir():
        return
    if not path.parent.is_dir():
        raise FileNotFoundError(
            f"{path.parent} does not exist (ferryman creates only the mount directory itself)"
        )
    try:
        path.mkdir(mode=mode)
    except FileExistsError:
        return
    os.chmod(path, mode)  # mkdir's mode is masked by the umask
    if uid is not None or gid is not None:
        os.chown(path, -1 if uid is None else uid, -1 if gid is None else gid)
