from __future__ import annotations

import json
from pathlib import Path

import pytest

from ferryman.config import Pool as PoolConfig
from ferryman.pool import LocalPool, PoolError, PoolExhausted, is_quota_error
from tests.conftest import KEYS, write_keys

A, B, C = KEYS


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
    cfg = PoolConfig(keys_dir=tmp_path / "keys", keys=KEYS, quota_window=100)
    return LocalPool("main", cfg, tmp_path / "state/main.json", clock=clock)


def test_current_settles_on_first_key(pool: LocalPool) -> None:
    assert pool.current() == A
    assert json.loads(pool.state_file.read_text())["current"] == A


async def test_advance_in_order_and_wraps(pool: LocalPool, clock: Clock) -> None:
    assert pool.current() == A
    assert await pool.advance() == B
    assert await pool.advance() == C
    # A's window has passed by now, so the pool wraps back to it.
    clock.now += 101
    assert await pool.advance() == A


async def test_advance_if_current_rotates_once(pool: LocalPool) -> None:
    pool.current()
    # Three consumers all hit the cap on A at once: only the first rotates.
    results = [await pool.advance(expected=A) for _ in range(3)]
    assert results == [B, B, B]
    assert pool.available() == [B, C]


async def test_exhausted_then_frees(pool: LocalPool, clock: Clock) -> None:
    pool.current()
    await pool.advance()
    await pool.advance()
    with pytest.raises(PoolExhausted) as exc:
        await pool.advance()
    assert exc.value.frees_at == 1_000_000.0 + 100
    with pytest.raises(PoolExhausted):
        pool.current()
    # All three free together; the recorded current key (C) is usable again.
    clock.now += 101
    assert pool.current() == C


async def test_current_moves_off_exhausted_key(pool: LocalPool) -> None:
    pool.current()
    state = json.loads(pool.state_file.read_text())
    state["exhausted"] = {A: pool.clock()}
    pool.state_file.write_text(json.dumps(state))
    assert pool.current() == B


def test_key_file_validation(pool: LocalPool, tmp_path: Path) -> None:
    assert pool.key_file(A) == tmp_path / "keys" / f"{A}.json"
    (tmp_path / "keys" / f"{B}.json").write_text(
        json.dumps({"type": "service_account", "client_email": "other@x"})
    )
    with pytest.raises(PoolError, match="not a service-account key"):
        pool.key_file(B)
    (tmp_path / "keys" / f"{C}.json").write_bytes(b"\x00GITCRYPT\x00garbage")
    with pytest.raises(PoolError, match="unreadable"):
        pool.key_file(C)
    with pytest.raises(PoolError, match="missing"):
        pool.key_file("nobody@x")


def test_describe(pool: LocalPool) -> None:
    pool.current()
    d = pool.describe()
    assert [k["current"] for k in d["keys"]] == [True, False, False]
    assert all(k["file_ok"] for k in d["keys"])


def test_quota_message_is_case_sensitive() -> None:
    assert is_quota_error("googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded")
    assert not is_quota_error("User Rate Limit Exceeded")
    assert not is_quota_error(None)


async def test_rotating_an_unused_pool_moves_past_its_first_key(tmp_path: Path) -> None:
    pool = LocalPool("p", PoolConfig(keys_dir=tmp_path, keys=KEYS), tmp_path / "state.json")
    write_keys(tmp_path, KEYS)
    assert await pool.advance() == KEYS[1]
    assert pool.epoch == 1
