"""HTTP/JSON control API, served on a unix socket for the CLI, and an optional
TCP listener that serves only health and metrics (no control)."""

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
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from .metrics import CONTENT_TYPE, agent_metrics
from .pool import PoolError
from .transfer import TransferBusy
from .tuning import TuneError

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
        except (PoolError, TuneError) as e:
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
        await asyncio.shield(agent.restart(sup))
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

    async def tune_status(_: Request) -> Any:
        return agent.tuner.status()

    async def tune(request: Request) -> Any:
        body = await _body(request)
        unit, dry_run = body.get("unit"), bool(body.get("dry_run"))
        if dry_run:
            return await agent.tuner.run(unit, dry_run=True)
        # Checked here too, so a refusal answers the request instead of
        # failing a task nobody waits for.
        agent.tuner.check(unit)
        task = agent.spawn(agent.tuner.run(unit), "tune")
        if body.get("wait", True):
            return await asyncio.shield(task)
        return {"started": True}

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

    async def events(request: Request) -> Any:
        return agent.notifier.recent(int(request.query_params.get("limit", "50")))

    routes = [
        *_probes(agent),
        Route("/v1/events", _json(events)),
        Route("/v1/status", _json(status)),
        Route("/v1/stats", _json(stats)),
        Route("/v1/jobs", _json(jobs)),
        Route("/v1/units/{name}/restart", _json(restart), methods=["POST"]),
        Route("/v1/transfers/{name}/run", _json(run_transfer), methods=["POST"]),
        Route("/v1/tune", _json(tune_status)),
        Route("/v1/tune", _json(tune), methods=["POST"]),
        Route("/v1/pools", _json(pools)),
        Route("/v1/pools/{name}/rotate", _json(rotate), methods=["POST"]),
        Route("/v1/throttle", _json(throttle), methods=["POST"]),
        Route("/v1/health", _json(health), methods=["POST"]),
    ]
    return Starlette(routes=routes)


def _probes(agent: Agent) -> list[Route]:
    async def healthz(_: Request) -> Response:
        return JSONResponse({"ok": True})

    async def readyz(_: Request) -> Response:
        # Ready once every unit is: what a pod's readiness probe should wait
        # for before the apps that read its mounts start.
        waiting = {n: s.state.value for n, s in agent.units.items() if not s.ready}
        return JSONResponse({"ready": not waiting, "waiting": waiting},
                            status_code=503 if waiting else 200)  # fmt: skip

    async def metrics(_: Request) -> Response:
        return PlainTextResponse(await agent_metrics(agent), media_type=CONTENT_TYPE)

    return [Route("/healthz", healthz), Route("/readyz", readyz), Route("/metrics", metrics)]


def build_tcp_app(agent: Agent) -> Starlette:
    return Starlette(routes=_probes(agent))


class _Server(uvicorn.Server):
    # The agent owns SIGTERM/SIGINT; uvicorn must not install its own.
    def install_signal_handlers(self) -> None:
        pass

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class ApiServer:
    def __init__(self, servers: list[tuple[_Server, asyncio.Task[None]]], path: Path) -> None:
        self.servers, self.path = servers, path

    async def stop(self) -> None:
        for server, _ in self.servers:
            server.should_exit = True
        for _, task in self.servers:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.path.unlink(missing_ok=True)


async def _start(config: uvicorn.Config, name: str) -> tuple[_Server, asyncio.Task[None]]:
    server = _Server(config)
    task = asyncio.create_task(server.serve(), name=name)
    while not server.started:
        if task.done():
            task.result()
            raise RuntimeError(f"{name} server exited during startup")
        await asyncio.sleep(0.05)
    return server, task


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
    servers = [await _start(config, "api")]
    os.chmod(path, agent.cfg.api.socket_mode)
    log.info("API listening on %s", path)
    if (listen := agent.cfg.api.listen) is not None:
        tcp = uvicorn.Config(
            build_tcp_app(agent),
            host=listen.host,
            port=listen.port,
            log_level="warning",
            access_log=False,
            lifespan="off",
        )
        try:
            servers.append(await _start(tcp, "probes"))
        except BaseException:
            await ApiServer(servers, path).stop()
            raise
        log.info("health and metrics on %s:%d", listen.host, listen.port)
    return ApiServer(servers, path)
