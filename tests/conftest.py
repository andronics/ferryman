from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from ferryman.config import Config

KEYS = ["sa-01@p.iam.gserviceaccount.com", "sa-02@p.iam.gserviceaccount.com",
        "sa-03@p.iam.gserviceaccount.com"]  # fmt: skip


def write_keys(keys_dir: Path, keys: list[str]) -> None:
    keys_dir.mkdir(parents=True, exist_ok=True)
    for k in keys:
        (keys_dir / f"{k}.json").write_text(
            json.dumps({"type": "service_account", "client_email": k})
        )


@pytest.fixture
def make_config(tmp_path: Path) -> Callable[..., Config]:
    def make(**overrides: Any) -> Config:
        write_keys(tmp_path / "keys", KEYS)
        data: dict[str, Any] = {
            "paths": {
                "runtime": str(tmp_path / "run"),
                "cache": str(tmp_path / "cache"),
                "state": str(tmp_path / "state"),
                "logs": str(tmp_path / "logs"),
            },
            "roots": {"local": str(tmp_path / "local"), "remote": str(tmp_path / "remote")},
            "pools": {"main": {"keys_dir": str(tmp_path / "keys"), "keys": KEYS}},
            "units": {"media-tv": {"mountpoint": "media/tv", "pool": "main", "drive_id": "0A"}},
            "transfers": {"media-tv": {"unit": "media-tv"}},
        }
        data.update(overrides)
        return Config.model_validate(data)

    return make


class FakeRclone:
    """Enough of rclone's rc API to drive transfers, rotation and health.

    `job_outcomes` is consumed one per submitted job: None for success, or
    an error string for failure. `refresh_results` likewise gives each async
    vfs/refresh job its output's result map."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.job_outcomes: list[str | None] = []
        self.refresh_results: list[dict[str, str]] = []
        self.jobs: dict[int, dict[str, Any]] = {}
        self.last_error: str | None = None

    def endpoints(self) -> list[str]:
        return [e for e, _ in self.calls]

    def app(self) -> Starlette:
        async def handle(request: Request) -> JSONResponse:
            endpoint = request.path_params["path"]
            raw = await request.body()
            params: dict[str, Any] = json.loads(raw) if raw else {}
            self.calls.append((endpoint, params))
            return JSONResponse(self.respond(endpoint, params))

        return Starlette(routes=[Route("/{path:path}", handle, methods=["POST"])])

    def respond(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        if endpoint.startswith("sync/"):
            jobid = len(self.jobs) + 1
            outcome = self.job_outcomes.pop(0) if self.job_outcomes else None
            self.jobs[jobid] = {"finished": True, "success": outcome is None,
                                "error": outcome or "", "group": params.get("_group")}  # fmt: skip
            if outcome:
                self.last_error = outcome
            return {"jobid": jobid}
        if endpoint == "vfs/refresh" and params.get("_async"):
            jobid = len(self.jobs) + 1
            result = self.refresh_results.pop(0) if self.refresh_results else {"": "OK"}
            self.jobs[jobid] = {"finished": True, "success": True, "error": "",
                                "output": {"result": result}}  # fmt: skip
            return {"jobid": jobid}
        if endpoint == "job/status":
            return self.jobs[int(params["jobid"])]
        if endpoint == "core/stats":
            return {"transfers": 3, "bytes": 1024, "errors": 0, "lastError": self.last_error}
        if endpoint == "core/stats-reset":
            self.last_error = None
            return {}
        return {}


@contextlib.asynccontextmanager
async def serve_unix(app: Starlette, path: Path) -> AsyncIterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    server = uvicorn.Server(uvicorn.Config(app, uds=str(path), log_level="warning", lifespan="off"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    try:
        yield
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def fake_rclone() -> Iterator[FakeRclone]:
    yield FakeRclone()
