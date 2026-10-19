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
    cfg = make_config(
        units={"docs": {"mountpoint": "docs", "backend": "alias",
                        "options": {"remote": str(backing)}, "warm": True}},
        transfers={"docs": {"unit": "docs"}},
    )  # fmt: skip
    agent = Agent(cfg)
    sup: UnitSupervisor = agent.units["docs"]
    mount = agent.layout.mount_path("docs")
    server = await serve(agent)
    try:
        await agent.start()
        await wait_for(lambda: sup.ready)
        assert mountutil.is_mounted(mount)
        assert await asyncio.to_thread((mount / "hello.txt").read_text) == "hello\n"

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
        assert await mountutil.responsive(mount)
        assert await asyncio.to_thread((mount / "hello.txt").read_text) == "hello\n"
        assert await agent.check_health() == {"docs": "healthy"}

        # The CLI against the live API (in a thread: it is a blocking client).
        def cli(*args: str) -> str:
            r = CliRunner().invoke(app, ["--socket", str(agent.layout.api_socket), *args])
            assert r.exit_code == 0, r.output
            return r.output

        status = json.loads(await asyncio.to_thread(cli, "--json", "status"))
        assert status["units"][0]["state"] == "ready"
        assert "0 file(s)" in await asyncio.to_thread(cli, "sync", "docs")
        assert "docs" in await asyncio.to_thread(cli, "throttle", "docs", "10M")
        assert "docs" in await asyncio.to_thread(cli, "stats")
    finally:
        await agent.stop()
        await server.stop()
    assert not mountutil.is_mounted(mount)
    assert not agent.layout.api_socket.exists()


async def test_stop_while_starting_is_prompt(
    make_config: Callable[..., Config], tmp_path: Path
) -> None:
    backing = tmp_path / "drive"
    backing.mkdir()
    cfg = make_config(
        units={
            "docs": {"mountpoint": "docs", "backend": "alias", "options": {"remote": str(backing)}},
            "rcd": {"mountpoint": "rcd", "backend": "alias", "mount": False,
                    "options": {"remote": str(backing)}},
        },
        transfers={},
    )  # fmt: skip
    agent = Agent(cfg)
    await agent.start()
    await asyncio.sleep(0.2)  # rclone spawned, mount not up yet
    async with asyncio.timeout(5):
        await agent.stop()
    assert all(sup.state.value == "stopped" for sup in agent.units.values())
    assert not mountutil.is_mounted(agent.layout.mount_path("docs"))
