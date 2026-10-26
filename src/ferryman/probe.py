"""Whether an exhausted key can upload again.

Drive has no API for how much of a key's daily upload cap is left, so the
only test is an upload: a few random bytes into a folder on a shared drive,
deleted again if they land. The cap refuses every upload, small ones too,
with the same "User rate limit exceeded." a real transfer gets. The cap is
widely reported to roll (each upload counts for 24 hours from when it was
made) rather than reset, so a key often has room again long before its
quota_window ends; a probe finds out.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from .pool import QUOTA_MSG, is_quota_error

log = logging.getLogger(__name__)

PROBE_DIR = ".ferryman-probe"
PROBE_TIMEOUT = 120.0


@dataclass(frozen=True)
class ProbeResult:
    # "ok": the upload landed, the key has room again; "capped": refused by
    # the cap; "error": anything else (unreachable, no access to the drive),
    # which says nothing about the cap.
    result: str
    message: str = ""


def _conf(key_file: Path, drive_id: str) -> str:
    opts = {
        "type": "drive",
        "scope": "drive",
        "service_account_file": str(key_file),
        "team_drive": drive_id,
        # The cap as a fatal error, as units have it, not hours of retries.
        "stop_on_upload_limit": "true",
    }
    return "[probe]\n" + "".join(f"{k} = {v}\n" for k, v in opts.items())


async def _rclone(binary: str, workdir: Path, *args: str, data: bytes | None = None,
                  timeout: float = PROBE_TIMEOUT) -> tuple[int, str]:  # fmt: skip
    env = {k: v for k, v in os.environ.items()
           if k not in ("NOTIFY_SOCKET", "WATCHDOG_USEC", "WATCHDOG_PID")}  # fmt: skip
    proc = await asyncio.create_subprocess_exec(
        binary, "--config", str(workdir / "rclone.conf"), "--cache-dir", str(workdir / "cache"),
        "--low-level-retries", "2", *args,
        stdin=asyncio.subprocess.PIPE if data is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE, env=env,
    )  # fmt: skip
    try:
        _, err = await asyncio.wait_for(proc.communicate(data), timeout)
    except TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await proc.wait()
        return -1, f"timed out after {timeout:.0f}s"
    assert proc.returncode is not None
    return proc.returncode, err.decode(errors="replace")


async def probe(binary: str, key_file: Path, drive_id: str, size: int, workdir: Path,
                name: str, timeout: float = PROBE_TIMEOUT) -> ProbeResult:  # fmt: skip
    """Upload `size` random bytes as <PROBE_DIR>/<name> with the key, then
    delete them. `workdir` holds the probe's rclone.conf while it runs."""
    conf = _write_conf(workdir, _conf(key_file, drive_id))
    remote = f"probe:{PROBE_DIR}/{name}"
    try:
        code, err = await _rclone(binary, workdir, "rcat", remote, data=os.urandom(size),
                                  timeout=timeout)  # fmt: skip
        if code == 0:
            code, err = await _rclone(binary, workdir, "deletefile", remote, timeout=timeout)
            if code != 0:
                log.warning("probe: could not delete %s: %s", remote, _last_line(err))
            return ProbeResult("ok")
        if is_quota_error(err):
            return ProbeResult("capped", QUOTA_MSG)
        return ProbeResult("error", _last_line(err) or f"rclone exited with {code}")
    except OSError as e:  # rclone missing or not executable
        return ProbeResult("error", f"cannot run {binary}: {e.strerror or e}")
    finally:
        conf.unlink(missing_ok=True)


def _write_conf(workdir: Path, text: str) -> Path:
    workdir.mkdir(parents=True, exist_ok=True, mode=0o700)
    conf = workdir / "rclone.conf"
    fd = os.open(conf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)
    return conf


def _last_line(text: str) -> str:
    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    return lines[-1] if lines else ""
