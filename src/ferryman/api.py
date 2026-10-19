"""HTTP/JSON control API, served on a unix socket for the CLI."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .pool import PoolError
from .transfer import TransferBusy

if TYPE_CHECKING:
    from .agent import Agent

log = logging.getLogger(__name__)

Handler = Callable[[Request], Awaitable[Any]]


def _json(handler: Handler) -> Callable[[Request], Awaitable[JSONResponse]]:
    async def wrapped(request: Request) -> JSONResponse:
        try:
            return JSONResponse(await handler(request))
        except KeyError as e:
            return JSONResponse({"error": f"not found: {e.args[0]}"}, status_code=404)
        except TransferBusy as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        except PoolError as e:
            return JSONResponse({"error": str(e)}, status_code=409)

    return wrapped


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    data: dict[str, Any] = await request.json()
    return data


def build_app(agent: Agent) -> Starlette:
    async def healthz(_: Request) -> Any:
        return {"ok": True}

    async def status(_: Request) -> Any:
        return agent.status()

    async def stats(request: Request) -> Any:
        return await agent.stats(request.query_params.get("pattern"))

    async def jobs(request: Request) -> Any:
        return await agent.jobs(request.query_params.get("pattern"))

    async def restart(request: Request) -> Any:
        sup = agent.units[request.path_params["name"]]
        await sup.restart()
        return sup.status()

    async def run_transfer(request: Request) -> Any:
        tr = agent.transfers[request.path_params["name"]]
        body = await _body(request)
        if tr.running:
            raise TransferBusy(f"transfer {tr.name} is already running")
        # The run belongs to the agent, not the request: a client that
        # disconnects does not abort it, and shutdown cancels it cleanly.
        task = agent.spawn(tr.run(bool(body.get("dry_run"))), f"run:{tr.name}")
        if body.get("wait", True):
            return await asyncio.shield(task)
        return {"started": True, "transfer": tr.name}

    async def pools(_: Request) -> Any:
        out = []
        for p in agent.pools.values():
            try:
                out.append(await p.info())
            except PoolError as e:
                out.append({"pool": p.name, "error": str(e)})
        return out

    async def rotate(request: Request) -> Any:
        name = request.path_params["name"]
        key = await agent.rotate(name)
        return {"pool": name, "current": key}

    async def throttle(request: Request) -> Any:
        body = await _body(request)
        return await agent.throttle(body.get("pattern") or "", str(body["rate"]))

    async def health(_: Request) -> Any:
        return await agent.check_health()

    routes = [
        Route("/healthz", _json(healthz)),
        Route("/v1/status", _json(status)),
        Route("/v1/stats", _json(stats)),
        Route("/v1/jobs", _json(jobs)),
        Route("/v1/units/{name}/restart", _json(restart), methods=["POST"]),
        Route("/v1/transfers/{name}/run", _json(run_transfer), methods=["POST"]),
        Route("/v1/pools", _json(pools)),
        Route("/v1/pools/{name}/rotate", _json(rotate), methods=["POST"]),
        Route("/v1/throttle", _json(throttle), methods=["POST"]),
        Route("/v1/health", _json(health), methods=["POST"]),
    ]
    return Starlette(routes=routes)


class _Server(uvicorn.Server):
    # The agent owns SIGTERM/SIGINT; uvicorn must not install its own.
    def install_signal_handlers(self) -> None:
        pass

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class ApiServer:
    def __init__(self, server: _Server, task: asyncio.Task[None], path: Path) -> None:
        self.server, self.task, self.path = server, task, path

    async def stop(self) -> None:
        self.server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await self.task
        self.path.unlink(missing_ok=True)


def _socket_in_use(path: Path) -> bool:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        try:
            s.connect(str(path))
            return True
        except OSError:
            return False


async def serve(agent: Agent) -> ApiServer:
    path = agent.layout.api_socket
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if _socket_in_use(path):
            raise RuntimeError(f"another ferryman agent is listening on {path}")
        path.unlink()
    config = uvicorn.Config(build_app(agent), uds=str(path), log_level="warning", lifespan="off")
    server = _Server(config)
    task = asyncio.create_task(server.serve(), name="api")
    while not server.started:
        if task.done():
            task.result()
            raise RuntimeError("API server exited during startup")
        await asyncio.sleep(0.05)
    os.chmod(path, agent.cfg.api.socket_mode)
    log.info("API listening on %s", path)
    return ApiServer(server, task, path)
