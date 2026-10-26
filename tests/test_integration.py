"""End to end with a real rclone and FUSE: an `alias` unit over a local
directory stands in for a shared drive."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ferryman import mountutil
from ferryman.agent import Agent
from ferryman.api import serve
from ferryman.cli import app
from ferryman.config import Config
from ferryman.unit import UnitSupervisor

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (shutil.which("rclone") and shutil.which("fusermount3") and Path("/dev/fuse").exists()),
        reason="needs rclone and FUSE",
    ),
]


async def wait_for(cond: Callable[[], bool], timeout: float = 30.0) -> None:
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.1)


async def test_mount_transfer_crash_and_api(
    make_config: Callable[..., Config], tmp_path: Path
) -> None:
    backing = tmp_path / "drive"
    backing.mkdir()
    (backing / "hello.txt").write_text("hello\n")
    (tmp_path / "remote").mkdir()
    cfg = make_config(
        units={"docs": {"backend": "alias", "options": {"remote": str(backing)}, "warm": True,
                        "mounts": [{"path": str(tmp_path / "remote/docs")},
                                   {"path": str(tmp_path / "remote/docs2"),
                                    "allow_other": False}]}},
        transfers={"docs": {"unit": "docs"}},
    )  # fmt: skip
    agent = Agent(cfg)
    sup: UnitSupervisor = agent.units["docs"]
    mount, extra = agent.layout.mount_paths("docs")
    server = await serve(agent)
    try:
        await agent.start()
        await wait_for(lambda: sup.ready)
        assert mountutil.is_mounted(mount) and mountutil.is_mounted(extra)
        assert await asyncio.to_thread((mount / "hello.txt").read_text) == "hello\n"
        await wait_for(lambda: sup.warm is not None and sup.warm["state"] == "done")
        # One process, one VFS: a write through one path shows through the
        # other at once, despite the 1000h dir cache.
        await asyncio.to_thread((mount / "both.txt").write_text, "both\n")
        assert await asyncio.to_thread((extra / "both.txt").read_text) == "both\n"
        assert [m["state"] for m in sup.status()["mounts"]] == ["mounted", "mounted"]

        # A staged file moves up through the unit's own process.
        staged = tmp_path / "local/docs/sub"
        staged.mkdir(parents=True)
        (staged / "a.txt").write_text("a\n")
        result = await agent.transfers["docs"].run()
        assert result["ok"], result
        assert result["files"] == 1
        assert (backing / "sub/a.txt").read_text() == "a\n"
        assert not (tmp_path / "local/docs/sub").exists()  # deleteEmptySrcDirs

        # rclone dies hard: the dead endpoint is detached and the unit comes back.
        old_pid = sup.pid
        assert old_pid
        os.kill(old_pid, signal.SIGKILL)
        await wait_for(lambda: not sup.ready)
        await wait_for(lambda: sup.ready and sup.pid != old_pid)
        assert sup.restarts == 1
        assert await mountutil.responsive(mount) and await mountutil.responsive(extra)
        assert await asyncio.to_thread((extra / "hello.txt").read_text) == "hello\n"
        assert await agent.check_health() == {"docs": "healthy"}

        # An extra mount lost on its own is re-added in place: no restart.
        assert await mountutil.unmount(extra)
        health = await agent.check_health()
        assert health["docs"] == f"healthy; {extra} not mounted: re-mounted", health
        assert sup.restarts == 1 and mountutil.is_mounted(extra)
        assert await asyncio.to_thread((extra / "hello.txt").read_text) == "hello\n"
        kinds = [e["kind"] for e in agent.notifier.recent(10)]
        assert "mount.lost" in kinds and kinds[-1] == "mount.recovered"

        # The CLI against the live API (in a thread: it is a blocking client).
        def cli(*args: str) -> str:
            r = CliRunner().invoke(app, ["--socket", str(agent.layout.api_socket), *args])
            assert r.exit_code == 0, r.output
            return r.output

        status = json.loads(await asyncio.to_thread(cli, "--json", "status"))
        assert status["units"][0]["state"] == "ready"
        assert "0 file(s)" in await asyncio.to_thread(cli, "run", "docs")
        assert "0 file(s)" in await asyncio.to_thread(cli, "sync", "docs")  # the old name
        assert "docs" in await asyncio.to_thread(cli, "throttle", "docs", "10M")
        assert "docs" in await asyncio.to_thread(cli, "stats")
    finally:
        await agent.stop()
        await server.stop()
    assert not mountutil.is_mounted(mount) and not mountutil.is_mounted(extra)
    assert not agent.layout.api_socket.exists()


async def test_stop_while_starting_is_prompt(
    make_config: Callable[..., Config], tmp_path: Path
) -> None:
    backing = tmp_path / "drive"
    backing.mkdir()
    (tmp_path / "remote").mkdir()
    cfg = make_config(
        units={
            "docs": {"backend": "alias", "options": {"remote": str(backing)},
                     "mounts": [{"path": str(tmp_path / "remote/docs")}]},
            "rcd": {"backend": "alias", "options": {"remote": str(backing)}},
        },
        transfers={},
    )  # fmt: skip
    agent = Agent(cfg)
    await agent.start()
    await asyncio.sleep(0.2)  # rclone spawned, mount not up yet
    async with asyncio.timeout(5):
        await agent.stop()
    assert all(sup.state.value == "stopped" for sup in agent.units.values())
    assert not mountutil.is_mounted(agent.layout.mount_paths("docs")[0])


async def test_backup_sync_through_an_rcd_unit(
    make_config: Callable[..., Config], tmp_path: Path
) -> None:
    """A `sync` backup through a transfer-only unit: {hostname} in dest,
    excluded files stay behind, and the mirror deletes what the source lost."""
    backing, dump = tmp_path / "drive", tmp_path / "dump"
    backing.mkdir()
    dump.mkdir()
    (dump / "vzdump-100.vma.zst").write_text("100\n")
    (dump / "vzdump-101.vma.zst.tmp").write_text("partial\n")
    cfg = make_config(
        hostname="pve-01",
        units={"backups": {"backend": "alias", "options": {"remote": str(backing)}}},
        transfers={"dump": {"unit": "backups", "mode": "sync", "source": str(dump),
                            "dest": "proxmox/{hostname}", "exclude": ["*.tmp"]}},
    )  # fmt: skip
    agent = Agent(cfg)
    agent.job_poll_interval = 0.1
    try:
        await agent.start()
        await wait_for(lambda: agent.units["backups"].ready)
        result = await agent.transfers["dump"].run()
        assert result["ok"] and result["files"] == 1, result
        target = backing / "proxmox/pve-01"
        assert sorted(p.name for p in target.iterdir()) == ["vzdump-100.vma.zst"]
        assert (dump / "vzdump-101.vma.zst.tmp").exists()  # sync never touches the source

        (dump / "vzdump-100.vma.zst").unlink()
        (dump / "vzdump-102.vma.zst").write_text("102\n")
        result = await agent.transfers["dump"].run()
        assert result["ok"], result
        assert sorted(p.name for p in target.iterdir()) == ["vzdump-102.vma.zst"]
    finally:
        await agent.stop()
