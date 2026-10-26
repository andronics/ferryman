"""Key probes: the rclone upload test, the pool's probe bookkeeping, and the
controller returning a key to its pool."""

from __future__ import annotations

import asyncio
import json
import stat
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ferryman.config import ControllerConfig
from ferryman.config import Pool as PoolConfig
from ferryman.controller import Controller
from ferryman.pool import LocalPool, PoolExhausted
from ferryman.probe import PROBE_DIR, probe
from tests.conftest import KEYS, write_keys

A, B, C = KEYS
QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"


def fake_rclone(tmp_path: Path, rcat: str = "ok", stderr: str = "", sleep: float = 0) -> Path:
    """An `rclone` that logs its arguments and config, and answers rcat with
    success ("ok") or `stderr` and exit 1."""
    log = tmp_path / "rclone.log"
    script = tmp_path / "rclone"
    script.write_text(f"""#!/bin/sh
conf=$2
echo "$*" >> {log}
cp "$conf" {tmp_path}/last.conf
case " $* " in
  *" rcat "*)
    cat > {tmp_path}/uploaded
    sleep {sleep}
    if [ "{rcat}" = ok ]; then exit 0; fi
    printf '%s\\n' "{stderr}" >&2
    exit 1;;
esac
exit 0
""")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script


def calls(tmp_path: Path) -> list[str]:
    return (tmp_path / "rclone.log").read_text().splitlines()


# ------------------------------------------------------------------- probe


async def test_probe_uploads_then_deletes(tmp_path: Path) -> None:
    rclone = fake_rclone(tmp_path)
    r = await probe(str(rclone), tmp_path / "k.json", "0ADRIVE", 1024, tmp_path / "w", "sa-01")
    assert r.result == "ok"
    rcat, delete = calls(tmp_path)
    assert rcat.endswith(f"rcat probe:{PROBE_DIR}/sa-01")
    assert delete.endswith(f"deletefile probe:{PROBE_DIR}/sa-01")
    assert (tmp_path / "uploaded").stat().st_size == 1024
    conf = (tmp_path / "last.conf").read_text()
    assert "team_drive = 0ADRIVE" in conf and "stop_on_upload_limit = true" in conf
    assert f"service_account_file = {tmp_path / 'k.json'}" in conf
    # The config with the key's path is gone once the probe is done.
    assert not (tmp_path / "w" / "rclone.conf").exists()


async def test_probe_sees_the_cap(tmp_path: Path) -> None:
    rclone = fake_rclone(tmp_path, rcat="fail", stderr=f"Failed to rcat: {QUOTA}")
    r = await probe(str(rclone), tmp_path / "k.json", "0A", 10, tmp_path / "w", "sa-01")
    assert r.result == "capped"
    assert len(calls(tmp_path)) == 1  # nothing landed, nothing to delete


async def test_probe_other_errors_are_inconclusive(tmp_path: Path) -> None:
    rclone = fake_rclone(tmp_path, rcat="fail", stderr="Failed to rcat: drive not found")
    r = await probe(str(rclone), tmp_path / "k.json", "0A", 10, tmp_path / "w", "sa-01")
    assert (r.result, r.message) == ("error", "Failed to rcat: drive not found")


async def test_probe_without_rclone_or_in_time(tmp_path: Path) -> None:
    r = await probe(str(tmp_path / "nope"), tmp_path / "k.json", "0A", 10, tmp_path / "w", "x")
    assert r.result == "error" and "cannot run" in r.message
    slow = fake_rclone(tmp_path, sleep=5)
    r = await probe(str(slow), tmp_path / "k.json", "0A", 10, tmp_path / "w", "x", timeout=0.3)
    assert r.result == "error" and "timed out" in r.message


# -------------------------------------------------------------------- pool


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def pool(tmp_path: Path, clock: Clock) -> LocalPool:
    write_keys(tmp_path / "keys", KEYS)
    cfg = PoolConfig(keys_dir=tmp_path / "keys", keys=KEYS, quota_window=86400,
                     probe={"drive_id": "0A", "interval": 3600})  # fmt: skip
    return LocalPool("main", cfg, tmp_path / "state/main.json", clock=clock)


async def test_probes_fall_due_an_interval_after_the_hit_then_each_probe(
    pool: LocalPool, clock: Clock
) -> None:
    pool.current()
    await pool.advance(expected=A)
    assert pool.probe_due() == []
    clock.now += 3600
    assert pool.probe_due() == [(A, 1_000_000.0)]
    assert not await pool.record_probe(A, 1_000_000.0, "capped", "User rate limit exceeded.")
    assert pool.probe_due() == []
    assert A not in pool.available()
    clock.now += 3600
    assert pool.probe_due() == [(A, 1_000_000.0)]


async def test_a_good_probe_returns_the_key(pool: LocalPool, clock: Clock) -> None:
    pool.current()
    await pool.advance(expected=A)
    clock.now += 3600
    # B is still current and fine: A goes back without a rotation.
    assert not await pool.record_probe(A, 1_000_000.0, "ok")
    assert pool.available() == [A, B, C]
    assert pool.current() == B and pool.epoch == 1
    d = {k["key"]: k for k in pool.describe()["keys"]}
    assert d[A]["exhausted_until"] is None and d[A]["probe"]["result"] == "ok"
    assert pool.describe()["probe_interval"] == 3600


async def test_a_good_probe_rescues_an_exhausted_pool(pool: LocalPool, clock: Clock) -> None:
    pool.current()
    await pool.advance(expected=A)
    await pool.advance(expected=B)
    with pytest.raises(PoolExhausted):
        await pool.advance(expected=C)
    clock.now += 3600
    assert await pool.record_probe(B, 1_000_000.0, "ok")
    assert pool.current() == B and pool.epoch == 3


async def test_a_probe_does_not_undo_a_newer_cap_hit(pool: LocalPool, clock: Clock) -> None:
    pool.current()
    await pool.advance(expected=A)
    clock.now += 3600
    due = pool.probe_due()
    # While the probe ran, A came back on its own... and hit the cap again.
    state = json.loads(pool.state_file.read_text())
    state["exhausted"][A] = clock.now
    pool.state_file.write_text(json.dumps(state))
    assert not await pool.record_probe(A, due[0][1], "ok")
    assert A not in pool.available()


async def test_no_probes_without_config_or_for_bad_files(tmp_path: Path, clock: Clock) -> None:
    write_keys(tmp_path / "keys", KEYS)
    plain = LocalPool("p", PoolConfig(keys_dir=tmp_path / "keys", keys=KEYS),
                      tmp_path / "p.json", clock=clock)  # fmt: skip
    plain.current()
    await plain.advance(expected=A)
    clock.now += 86000
    assert plain.probe_due() == []
    probing = LocalPool("q", PoolConfig(keys_dir=tmp_path / "keys", keys=KEYS,
                                        probe={"drive_id": "0A"}),
                        tmp_path / "q.json", clock=clock)  # fmt: skip
    probing.current()
    await probing.advance(expected=A)
    clock.now += 3600
    (tmp_path / "keys" / f"{A}.json").write_text("not json")
    assert probing.probe_due() == []


# ------------------------------------------------------------------ config


def test_probe_is_a_controller_setting(make_config: Any, tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="probe is a controller setting"):
        make_config(pools={"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS,
                                    "probe": {"drive_id": "0A"}}})  # fmt: skip
    cfg = ControllerConfig.model_validate(
        {"pools": {"main": {"keys_dir": "/k", "keys": KEYS, "probe": {"drive_id": "0A"}}}}
    )
    probe_cfg = cfg.pools["main"].probe
    assert probe_cfg is not None and probe_cfg.interval == 3600 and probe_cfg.size == "1K"
    for bad in ({"drive_id": "0A", "interval": 60}, {"drive_id": "0A", "size": "lots"}, {}):
        with pytest.raises(ValidationError):
            ControllerConfig.model_validate(
                {"pools": {"main": {"keys_dir": "/k", "keys": KEYS, "probe": bad}}}
            )


# ------------------------------------------------------------- controller


async def test_controller_probe_wakes_watchers_of_an_exhausted_pool(
    tmp_path: Path, clock: Clock
) -> None:
    write_keys(tmp_path / "keys", KEYS)
    rclone = fake_rclone(tmp_path)
    cfg = ControllerConfig.model_validate({
        "state": str(tmp_path / "co"),
        "watch_timeout": 5,
        "rclone": {"binary": str(rclone)},
        "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": [A, B],
                           "probe": {"drive_id": "0ADRIVE", "interval": 300}}},
    })  # fmt: skip
    co = Controller(cfg)
    pool = co.pools["main"]
    pool.clock = clock
    pool.current()
    await pool.advance(expected=A)
    with pytest.raises(PoolExhausted):
        await pool.advance(expected=B)
    epoch = pool.epoch
    clock.now += 300
    # An agent waiting out the exhausted pool is woken by the probe.
    watcher = asyncio.create_task(co.watch(pool, epoch, 5))
    await asyncio.sleep(0.05)
    assert [k for k, _ in pool.probe_due()] == [A, B]
    assert await co.probe_key(pool, A, 1_000_000.0) == "ok"
    lease = await asyncio.wait_for(watcher, 2)
    assert lease["key"] == A and lease["epoch"] == epoch + 1
    assert "team_drive = 0ADRIVE" in (tmp_path / "last.conf").read_text()
