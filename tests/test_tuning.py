"""Upload tuning: the search against simulated links, the probe loop against
the fake rclone, and the tuner's guards and profile."""

from __future__ import annotations

import asyncio
import collections
import json
import os
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from ferryman import tuning
from ferryman.agent import Agent
from ferryman.config import Config, Tuning, parse_size
from ferryman.metrics import agent_metrics
from ferryman.rc import RcClient
from ferryman.tuning import Outcome, Probe, Profile, TuneBusy, TuneError, classify, search
from ferryman.unit import State
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


# --------------------------------------------------------- phase 2: apply


def tune_to(agent: Agent, monkeypatch: pytest.MonkeyPatch, **kw: object) -> None:
    async def fake_tune(sup: object) -> Profile:
        return make_profile(chunk_size=16 * M, transfers=6, **kw)

    monkeypatch.setattr(agent.tuner, "_tune", fake_tune)


async def test_defaults_until_a_profile_is_applied(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    sup = agent.units["media-tv"]
    assert "chunk_size = 128M\n" in sup.render_conf()
    tune_to(agent, monkeypatch)
    r = await agent.tuner.run()  # tuning.apply defaults to suggest
    assert r["applied"] == [] and agent.tuner.active is None
    assert agent.tuner.transfer_settings("media-tv") == {
        "transfers": 8, "checkers": 8, "source": "default"}  # fmt: skip
    assert "chunk_size = 128M\n" in sup.render_conf()


async def test_apply_switches_units_live_and_transfers_next_run(
    agent: Agent, fake_rclone: FakeRclone, monkeypatch: pytest.MonkeyPatch
) -> None:
    tune_to(agent, monkeypatch)
    r = await agent.tuner.run(apply=True)
    assert r["applied"] == [{"unit": "media-tv", "chunk_size": 16 * M, "live": True,
                             "was": None}]  # fmt: skip
    set_call = next(p for e, p in fake_rclone.calls if e == "backend/command")
    # `set` takes plain bytes; the conf needs the B (a bare number is KiB).
    assert set_call == {"command": "set", "fs": "media-tv:", "opt": {"chunk_size": str(16 * M)}}
    conf = agent.layout.rclone_conf("media-tv").read_text()
    assert f"chunk_size = {16 * M}B\n" in conf
    assert agent.layout.tune_active("drive").exists()
    assert agent.tuner.unit_settings("media-tv") == {"chunk_size": 16 * M, "source": "profile"}

    await agent.transfers["media-tv"].run()
    job = next(p for e, p in fake_rclone.calls if e == "sync/move")
    assert (job["_config"]["Transfers"], job["_config"]["Checkers"]) == (6, 6)


async def test_auto_applies_unless_noisy(agent: Agent, monkeypatch: pytest.MonkeyPatch) -> None:
    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(apply="auto")})
    tune_to(agent, monkeypatch, noisy=True)
    assert (await agent.tuner.run())["applied"] == []
    tune_to(agent, monkeypatch)
    assert len((await agent.tuner.run())["applied"]) == 1


async def test_a_suggested_tune_leaves_the_active_profile_in_effect(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    tune_to(agent, monkeypatch)
    await agent.tuner.run(apply=True)

    async def later(sup: object) -> Profile:
        return make_profile(at=2.0, chunk_size=64 * M, transfers=12)

    monkeypatch.setattr(agent.tuner, "_tune", later)
    await agent.tuner.run(apply=False)
    assert agent.tuner.profile is not None and agent.tuner.profile.transfers == 12
    assert agent.tuner.transfer_settings("media-tv")["transfers"] == 6
    assert agent.units["media-tv"].tuned_chunk == 16 * M
    await agent.tuner.apply()
    assert agent.tuner.transfer_settings("media-tv")["transfers"] == 12
    assert agent.units["media-tv"].tuned_chunk == 64 * M


async def test_own_settings_win_over_the_profile(
    make_config: Callable[..., Config], monkeypatch: pytest.MonkeyPatch
) -> None:
    agent = Agent(make_config(
        units={"media-tv": {"mounts": [{"path": "/pool/tv"}], "pool": "main", "drive_id": "0A",
                            "options": {"chunk_size": "256M"}}},
        transfers={"media-tv": {"unit": "media-tv", "transfers": 3}},
    ))  # fmt: skip
    tune_to(agent, monkeypatch)
    agent.units["media-tv"].state = State.READY
    agent.units["media-tv"].key_path = Path("/k.json")
    r = await agent.tuner.run(apply=True)
    assert r["applied"] == []  # nothing tunable: the unit has its own chunk_size
    assert agent.tuner.unit_settings("media-tv") == {"chunk_size": "256M", "source": "config"}
    assert agent.tuner.transfer_settings("media-tv") == {
        "transfers": 3, "checkers": 6, "source": "config"}  # fmt: skip
    assert "chunk_size = 256M\n" in agent.units["media-tv"].render_conf()


async def test_active_profile_survives_a_restart(
    agent: Agent, make_config: Callable[..., Config], monkeypatch: pytest.MonkeyPatch
) -> None:
    tune_to(agent, monkeypatch)
    await agent.tuner.run(apply=True)
    again = Agent(agent.cfg)
    assert again.units["media-tv"].tuned_chunk == 16 * M
    assert again.tuner.transfer_settings("media-tv")["source"] == "profile"


async def test_apply_without_a_profile_is_refused(agent: Agent) -> None:
    with pytest.raises(TuneError, match="no tuning profile"):
        await agent.tuner.apply()


async def test_tune_metrics(agent: Agent, monkeypatch: pytest.MonkeyPatch) -> None:
    tune_to(agent, monkeypatch)
    await agent.tuner.run(apply=True)
    text = await agent_metrics(agent)
    assert 'ferryman_tune_chunk_size_bytes{backend="drive",profile="active"} 16777216.0' in text
    assert 'ferryman_tune_transfers{backend="drive",profile="latest"} 6.0' in text
    assert 'ferryman_tune_limit{limit="path",backend="drive",profile="active"} 1.0' in text
    assert 'ferryman_tune_running{backend="drive"} 0.0' in text


# ------------------------------------------------- phase 3: drift, schedule

G = 1 << 30


def run_at(rate: float, size: int = 4 * G, files: int = 8) -> dict[str, object]:
    return {"ok": True, "bytes": size, "files": files, "transfer_time": size / rate}


async def test_drift_needs_consecutive_slow_large_runs(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    tune_to(agent, monkeypatch)
    await agent.tuner.run(apply=True)  # rate 150M/s
    obs = agent.tuner.observe
    obs("media-tv", run_at(50 * M))
    obs("media-tv", run_at(50 * M, size=200 * M))  # too small to count
    obs("media-tv", run_at(50 * M, files=400))  # 10M files: API-bound, not counted
    obs("media-tv", run_at(140 * M))  # back to normal: the streak breaks
    obs("media-tv", run_at(50 * M))
    obs("media-tv", run_at(60 * M))
    assert not agent.tuner.drifting
    obs("media-tv", run_at(70 * M))
    assert agent.tuner.drifting
    events = [e for e in agent.notifier.recent(20) if e["kind"].startswith("tune.")]
    assert events[-1]["kind"] == "tune.drift" and "33%, 40%, 47%" in events[-1]["message"]
    obs("media-tv", run_at(50 * M))  # still slow: no second alert
    assert sum(e["kind"] == "tune.drift" for e in agent.notifier.recent(20)) == 1
    obs("media-tv", run_at(120 * M))
    assert not agent.tuner.drifting
    assert agent.notifier.recent(1)[-1]["kind"] == "tune.recovered"


async def test_drift_with_auto_retunes_once_idle(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(apply="auto", drift_runs=1)})
    tune_to(agent, monkeypatch)
    await agent.tuner.run()
    agent.tuner.idle_poll = 0.01

    async def slower(sup: object) -> Profile:
        return make_profile(at=5.0, rate=60.0 * M, chunk_size=16 * M, transfers=4)

    monkeypatch.setattr(agent.tuner, "_tune", slower)
    tr = agent.transfers["media-tv"]
    async with tr.lock:  # a transfer holds the link: the re-tune waits
        agent.tuner.observe("media-tv", run_at(40 * M))
        assert agent.tuner.status()["retune_queued"]
        await asyncio.sleep(0.05)
        assert agent.tuner.active is not None and agent.tuner.active.rate == 150 * M
    async with asyncio.timeout(2):
        while agent.tuner.active is None or agent.tuner.active.rate != 60 * M:
            await asyncio.sleep(0.01)
    assert agent.tuner.recent == collections.deque()


async def test_no_profile_no_drift(agent: Agent) -> None:
    agent.tuner.observe("media-tv", run_at(1 * M))
    assert not agent.tuner.recent


async def test_schedule_runs_tunes_and_skips_busy_links(
    agent: Agent, monkeypatch: pytest.MonkeyPatch
) -> None:
    agent.cfg = agent.cfg.model_copy(update={"tuning": Tuning(schedule="0 3 * * *")})

    class Every:
        def __init__(self, *a: object) -> None:
            pass

        def get_next(self, _: type) -> float:
            return time.time() + 0.02

    monkeypatch.setattr(tuning, "croniter", Every)
    calls: list[int] = []

    async def fake_run(*a: object, **kw: object) -> dict[str, object]:
        calls.append(1)
        if len(calls) == 1:
            raise TuneBusy("transfer media-tv is running")
        return {"ok": True}

    monkeypatch.setattr(agent.tuner, "run", fake_run)
    task = asyncio.create_task(agent.tuner.schedule())
    async with asyncio.timeout(2):
        while len(calls) < 3:
            await asyncio.sleep(0.01)
    task.cancel()
    assert agent.tuner.next_run is not None


def test_bad_schedule_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid cron"):
        Tuning(schedule="every day")
