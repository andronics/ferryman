"""Upload tuning: the search against simulated links, the probe loop against
the fake rclone, and the tuner's guards and profile."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from ferryman import tuning
from ferryman.agent import Agent
from ferryman.config import Tuning, parse_size
from ferryman.rc import RcClient
from ferryman.tuning import Outcome, Probe, Profile, TuneBusy, TuneError, classify, search
from tests.conftest import FakeRclone

M = 1 << 20
QUOTA = "googleapi: Error 403: User rate limit exceeded., userRateLimitExceeded"
CHUNKS = [8 * M, 16 * M, 32 * M, 64 * M, 128 * M, 256 * M]
STEPS = [1, 2, 4, 8, 12, 16]


def link(bandwidth: float, wait: float, cap: float, limited_from: int = 0) -> tuning.Measure:
    """A stream sends a chunk at `bandwidth`, then waits `wait` seconds for
    its acknowledgement; all streams together top out at `cap`."""

    async def measure(chunk: int, transfers: int) -> Probe:
        per = chunk / (chunk / bandwidth + wait)
        rate = min(per * transfers, cap)
        return Probe(chunk, transfers, rate=rate, per_stream=rate / transfers, runs=[rate],
                     rate_limited=bool(limited_from) and transfers >= limited_from)  # fmt: skip

    return measure


def test_parse_size() -> None:
    assert parse_size("128M") == 128 * M
    assert parse_size("2G") == 2 << 30
    assert parse_size("64Mi") == 64 * M
    assert parse_size(4096) == 4096
    with pytest.raises(ValueError):
        parse_size("lots")


def test_tuning_config_rejects_bad_grids() -> None:
    with pytest.raises(ValueError):
        Tuning(chunk_sizes=["64M", "8M"])
    with pytest.raises(ValueError):
        Tuning(transfers=[2, 4])


async def test_latency_bound_link_finds_the_knee_then_the_plateau() -> None:
    # 45 MiB/s per stream on the wire, 0.2s per chunk ack, ~150 MiB/s uplink:
    # the 13 Oct workstation.
    o = await search(link(45 * M, 0.2, 150 * M), CHUNKS, STEPS, 2 << 30)
    a = [p for p in o.probes if p.transfers == 1]
    assert [p.chunk for p in a] == [8 * M, 16 * M, 32 * M, 64 * M, 128 * M]
    assert o.stopped == "plateau"
    # 64M is the knee (128M adds under 10%); four streams saturate the link.
    assert (o.chosen.chunk, o.chosen.transfers) == (64 * M, 4)


async def test_memory_cap_bounds_streams() -> None:
    o = await search(link(45 * M, 0.2, 10_000 * M), CHUNKS, STEPS, 256 * M)
    assert all(p.memory <= 256 * M for p in o.probes)
    assert o.stopped == "memory"


async def test_rate_limits_stop_the_climb_and_are_never_chosen() -> None:
    o = await search(link(45 * M, 0.2, 10_000 * M, limited_from=8), CHUNKS, STEPS, 4 << 30)
    assert o.stopped == "rate-limited"
    assert not o.chosen.rate_limited and o.chosen.transfers == 4


async def test_cheapest_setting_within_five_percent_wins() -> None:
    async def measure(chunk: int, transfers: int) -> Probe:
        rate = {1: 40.0, 2: 80.0, 4: 100.0, 8: 104.0}.get(transfers, 104.5) * M
        return Probe(chunk, transfers, rate=rate, runs=[rate])

    o = await search(measure, [8 * M], STEPS, 4 << 30)
    assert o.chosen.transfers == 4  # 8 adds 4%: not worth twice the buffers


async def test_out_of_budget_keeps_what_was_measured() -> None:
    inner = link(45 * M, 0.2, 150 * M)
    calls = 0

    async def measure(chunk: int, transfers: int) -> Probe:
        nonlocal calls
        calls += 1
        if calls > 3:
            raise tuning._OutOfBudget
        return await inner(chunk, transfers)

    o = await search(measure, CHUNKS, STEPS, 2 << 30)
    assert o.stopped == "budget" and len(o.probes) == 3 and o.chosen.chunk == 32 * M


def test_classify() -> None:
    p = Probe(64 * M, 4, rate=150 * M, runs=[150 * M], cpu=0.5, nic_tx=152 * M)
    assert classify(Outcome([p], p, "plateau"), 156 * M, 8)[0] == "nic"
    assert classify(Outcome([p], p, "plateau"), 1250 * M, 8)[0] == "path"
    assert classify(Outcome([p], p, "plateau"), None, 8)[0] == "path"
    assert classify(Outcome([p], p, "max"), None, 8)[0] == "transfers"
    assert classify(Outcome([p], p, "rate-limited"), None, 8)[0] == "api"
    busy = Probe(64 * M, 4, rate=150 * M, cpu=3.8)
    assert classify(Outcome([busy], busy, "plateau"), None, 4)[0] == "cpu"


def test_noisy_probe() -> None:
    assert Probe(M, 1, runs=[10.0, 7.0]).noisy
    assert not Probe(M, 1, runs=[10.0, 9.0]).noisy


def make_profile(**kw: object) -> Profile:
    base: dict[str, object] = dict(
        backend="drive", host="h", at=1.0, unit="media-tv", key="k", rclone="v1.75.1",
        chunk_size=64 * M, transfers=4, rate=150.0 * M, limit="path", detail="d",
        stopped="plateau", chunk_gain=1.8, noisy=False, bytes_used=1 << 30, probes=[],
    )  # fmt: skip
    base.update(kw)
    return Profile(**base)  # type: ignore[arg-type]


def test_profile_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "tuning/drive.json"
    make_profile().save(path)
    assert Profile.load(path) == make_profile()
    path.write_text("{not json")
    assert Profile.load(path) is None
    assert Profile.load(tmp_path / "missing.json") is None


# --------------------------------------------------------- against the fake


def fast(agent: Agent) -> None:
    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(
        warmup=0.05, window=0.25, repeats=2, budget="64G", probe_bytes="1G",
        chunk_sizes=["8M", "32M", "128M"], transfers=[1, 2, 4, 8],
    )})  # fmt: skip
    agent.tuner.sample_interval = 0.02


async def test_probes_measure_rate_and_stop_their_jobs(
    agent: Agent, fake_rclone: FakeRclone
) -> None:
    fast(agent)
    # 20 MB/s per stream at 8M, 40 at 32M or more; 100 MB/s across streams.
    fake_rclone.tune_rate = lambda c, t: min((20e6 if c < 32 * M else 40e6) * t, 100e6)
    sup = agent.units["media-tv"]
    rc = RcClient(agent.layout.rc_socket("media-tv"))
    used = [0]
    measure = agent.tuner._measurer(rc, os.getpid(), sup, used)
    o = await search(measure, [8 * M, 32 * M, 128 * M], [1, 2, 4, 8], 4 << 30)

    assert [(p.chunk, p.transfers) for p in o.probes] == [
        (8 * M, 1), (32 * M, 1), (128 * M, 1), (32 * M, 2), (32 * M, 4), (32 * M, 8)]  # fmt: skip
    assert (o.chosen.chunk, o.chosen.transfers) == (32 * M, 4)
    assert o.chosen.rate == pytest.approx(100e6, rel=0.25)
    assert o.chosen.per_stream == pytest.approx(o.chosen.rate / 4)
    assert all(len(p.runs) == 2 for p in o.probes)
    assert used[0] == sum(p.bytes for p in o.probes) > 0
    copies = [p for e, p in fake_rclone.calls if e == "sync/copy"]
    scratch = f".ferryman-tune/{agent.layout.hostname}"
    assert copies[0]["dstFs"] == f"tune,chunk_size={8 * M}B:{scratch}/01"
    assert copies[0]["_config"] == {"Transfers": 1, "Checkers": 1}
    assert all(j["finished"] for j in fake_rclone.jobs.values())


async def test_quota_hit_aborts_the_tune(agent: Agent, fake_rclone: FakeRclone) -> None:
    fast(agent)
    fake_rclone.tune_error = QUOTA
    sup = agent.units["media-tv"]
    measure = agent.tuner._measurer(RcClient(agent.layout.rc_socket("media-tv")),
                                    os.getpid(), sup, [0])  # fmt: skip
    with pytest.raises(TuneError, match="upload cap hit"):
        await search(measure, [8 * M], [1], 4 << 30)


async def test_runaway_probe_memory_aborts_the_tune(
    agent: Agent, fake_rclone: FakeRclone, monkeypatch: pytest.MonkeyPatch
) -> None:
    fast(agent)
    monkeypatch.setattr(tuning, "proc_rss", lambda pid: 29 << 30)
    measure = agent.tuner._measurer(RcClient(agent.layout.rc_socket("media-tv")),
                                    os.getpid(), agent.units["media-tv"], [0])  # fmt: skip
    with pytest.raises(TuneError, match=r"over the 4\.5G limit"):
        await search(measure, [8 * M], [1], 4 << 30)
    assert all(j["finished"] for j in fake_rclone.jobs.values())  # the job was stopped


async def test_budget_stops_probing(agent: Agent, fake_rclone: FakeRclone) -> None:
    fast(agent)
    agent.cfg = agent.cfg.model_copy(update={"tuning": agent.cfg.tuning.model_copy(
        update={"budget": "10M", "probe_bytes": "4M"})})  # fmt: skip
    fake_rclone.tune_rate = lambda c, t: 40e6 * t
    sup = agent.units["media-tv"]
    used = [0]
    measure = agent.tuner._measurer(RcClient(agent.layout.rc_socket("media-tv")),
                                    os.getpid(), sup, used)  # fmt: skip
    o = await search(measure, [8 * M, 32 * M, 128 * M], [1, 2, 4], 4 << 30)
    assert o.stopped == "budget"
    assert used[0] <= 10 * M + 4 * M  # a run may overshoot its cap by one sample


def test_prepare_source_links_one_random_file(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    fast(agent)
    monkeypatch.setattr(tuning, "SOURCE_BYTES", 2 * M)
    agent.tuner._prepare_source()
    src = agent.layout.tune_source
    data = src / "random.bin"
    assert data.stat().st_size == 2 * M
    assert len(list((src / "x8").iterdir())) == 32
    assert (src / "x8/f31.bin").stat().st_ino == data.stat().st_ino
    agent.tuner._prepare_source()  # idempotent


# ------------------------------------------------------------- guards, run


async def test_guards(agent: Agent) -> None:
    with pytest.raises(KeyError):
        agent.tuner.check("nope")
    agent.units["media-tv"].state = agent.units["media-tv"].state.__class__("starting")
    with pytest.raises(TuneError, match="no ready drive unit"):
        agent.tuner.check()


async def test_refuses_while_a_transfer_runs(agent: Agent) -> None:
    tr = agent.transfers["media-tv"]
    async with tr.lock:
        with pytest.raises(TuneBusy, match="transfer media-tv is running"):
            agent.tuner.check()


async def test_dry_run_reports_the_plan(agent: Agent) -> None:
    r = await agent.tuner.run(dry_run=True)
    plan = r["plan"]
    assert plan["unit"] == "media-tv" and plan["key"]
    # 6 chunk sizes fit 2G, plus 5 more transfer steps, twice each.
    assert plan["max_probe_runs"] == 22
    assert plan["max_bytes"] == parse_size("32G")
    assert not agent.layout.tune_dir.exists()


async def test_run_saves_profile_and_reports(agent: Agent, monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_tune(sup: object) -> Profile:
        await asyncio.sleep(0)
        return make_profile()

    monkeypatch.setattr(agent.tuner, "_tune", fake_tune)
    r = await agent.tuner.run()
    assert r["ok"] and r["profile"]["transfers"] == 4
    saved = json.loads(agent.layout.tune_profile("drive").read_text())
    assert saved["chunk_size"] == 64 * M
    assert agent.status()["tuning"]["profile"]["limit"] == "path"
    assert agent.notifier.recent(5)[-1]["kind"] == "tune.done"


async def test_failed_run_is_reported(agent: Agent, monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_tune(sup: object) -> Profile:
        raise TuneError("probe rclone exited with 1")

    monkeypatch.setattr(agent.tuner, "_tune", fake_tune)
    r = await agent.tuner.run()
    assert not r["ok"] and "exited" in r["error"]
    assert agent.tuner.profile is None
    assert agent.notifier.recent(5)[-1]["kind"] == "tune.failed"
