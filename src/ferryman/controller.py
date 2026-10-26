"""The controller: owns the service-account keys and every pool's rotation
state, and leases the current key to agents.

It speaks plain HTTP on a TCP port and expects to sit behind Traefik, which
terminates TLS. Authentication is its own regardless: each agent sends a
bearer token whose sha256 is in the config, and may only touch the pools
listed for it.

Agents report their status to it every few seconds and collect commands
(restart a unit, run a transfer) from it with long polls, which is what its
web UI at /ui/ shows and drives. The UI has no login of its own: Traefik puts
it behind forward-auth (authentik), and the controller trusts the user
header that sets.

It also arbitrates transfer claims. Two agents *moving* (or copying) into the
same place on a drive is harmless - each only adds its own files - but a
`sync` mirrors its source and deletes whatever else is there, so a sync
whose destination overlaps another agent's transfer would delete that
agent's uploads. Those claims are refused.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import hashlib
import hmac
import itertools
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from importlib import resources
from pathlib import PurePosixPath
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from starlette.routing import Route

from .config import ControllerConfig, parse_size
from .metrics import CONTENT_TYPE, controller_metrics
from .pool import LocalPool, NoOtherKey, PoolError, PoolExhausted
from .probe import probe

log = logging.getLogger(__name__)
# Longest an uplink's tune lock lasts if its agent never releases it.
TUNE_LOCK_MAX = 7200.0
# How often the probe loop looks for exhausted keys due a probe.
PROBE_TICK = 60.0


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Forbidden(Exception):
    pass


def overlaps(a: str, b: str) -> bool:
    """Whether two paths inside a drive are the same or one contains the other."""
    pa, pb = PurePosixPath("/", a).parts, PurePosixPath("/", b).parts
    n = min(len(pa), len(pb))
    return pa[:n] == pb[:n]


class Controller:
    def __init__(self, cfg: ControllerConfig) -> None:
        self.cfg = cfg
        self.pools = {
            name: LocalPool(name, p, cfg.state / "pools" / f"{name}.json")
            for name, p in cfg.pools.items()
        }
        self._changed = {name: asyncio.Condition() for name in cfg.pools}
        self.claims_file = cfg.state / "claims.json"
        # agent -> {"at": when, "report": what it last said}; in memory only,
        # agents re-report within seconds of a controller restart.
        self.reports: dict[str, dict[str, Any]] = {}
        # Commands for agents, newest last; pending ones wait for the agent's
        # next long poll.
        self.commands: collections.deque[dict[str, Any]] = collections.deque(maxlen=100)
        self._command_ids = itertools.count(1)
        self._commands_changed = {name: asyncio.Condition() for name in cfg.agents}
        # uplink -> {"agent", "until"}: agents behind one uplink tune one at a
        # time. In memory: a controller restart frees them, which costs at
        # most one overlapping tune.
        self.tune_locks: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------ tune locks

    def tune_lock(self, agent: str, uplink: str, ttl: float) -> dict[str, Any]:
        now = time.time()
        held = self.tune_locks.get(uplink)
        if held and held["until"] > now and held["agent"] != agent:
            return {"granted": False, "holder": held["agent"], "until": held["until"]}
        until = now + min(max(ttl, 60.0), TUNE_LOCK_MAX)
        self.tune_locks[uplink] = {"agent": agent, "until": until}
        log.info("uplink %s: %s is tuning", uplink, agent)
        return {"granted": True, "holder": agent, "until": until}

    def tune_unlock(self, agent: str, uplink: str) -> dict[str, Any]:
        held = self.tune_locks.get(uplink)
        if held and held["agent"] == agent:
            del self.tune_locks[uplink]
        return {"ok": True}

    # ---------------------------------------------------------------- auth

    def agent_for(self, authorization: str | None) -> str | None:
        if not authorization or not authorization.startswith("Bearer "):
            return None
        digest = token_hash(authorization.removeprefix("Bearer ").strip())
        found = None
        for name, agent in self.cfg.agents.items():
            # Compare against every entry so timing does not reveal which matched.
            if hmac.compare_digest(digest, agent.token_sha256):
                found = name
        return found

    def pool_for(self, agent: str, pool: str) -> LocalPool:
        if pool not in self.pools:
            raise KeyError(pool)
        if pool not in self.cfg.agents[agent].pools:
            raise Forbidden(f"agent {agent} may not use pool {pool}")
        return self.pools[pool]

    # --------------------------------------------------------------- leases

    def lease(self, pool: LocalPool) -> dict[str, Any]:
        key = pool.current()
        creds = json.loads(pool.key_file(key).read_text())
        return {"pool": pool.name, "key": key, "epoch": pool.epoch, "size": pool.size,
                "credentials": creds}  # fmt: skip

    async def exhausted(self, agent: str, pool: LocalPool, expected: str | None) -> dict[str, Any]:
        before = pool.epoch
        try:
            await pool.advance(expected)
        finally:
            # Even an all-exhausted pool changed state (the key got marked).
            async with self._changed[pool.name]:
                self._changed[pool.name].notify_all()
        if pool.epoch != before:
            why = f"a cap hit reported by {agent}" if expected else f"request of {agent}"
            log.warning("pool %s: rotated on %s", pool.name, why)
        return self.lease(pool)

    async def watch(self, pool: LocalPool, epoch: int, timeout: float) -> dict[str, Any]:
        cond = self._changed[pool.name]
        async with cond:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(cond.wait_for(lambda: pool.epoch != epoch), timeout)
        return self.lease(pool)

    # --------------------------------------------------------------- probes

    async def probe_loop(self) -> None:
        """Probe each pool's exhausted keys as they fall due, one at a time."""
        if not any(p.cfg.probe for p in self.pools.values()):
            return
        while True:
            for pool in self.pools.values():
                for key, marked in pool.probe_due():
                    try:
                        await self.probe_key(pool, key, marked)
                    except Exception:
                        log.exception("pool %s: probing %s failed", pool.name, key)
            await asyncio.sleep(PROBE_TICK)

    async def probe_key(self, pool: LocalPool, key: str, marked: float) -> str:
        cfg = pool.cfg.probe
        assert cfg is not None
        r = await probe(self.cfg.rclone.binary, pool.key_file(key), cfg.drive_id,
                        parse_size(cfg.size), self.cfg.state / "probe",
                        key.split("@", 1)[0])  # fmt: skip
        if r.result == "error":
            log.warning("pool %s: probe of %s inconclusive: %s", pool.name, key, r.message)
        else:
            log.info("pool %s: probe of %s: %s", pool.name, key, r.result)
        if await pool.record_probe(key, marked, r.result, r.message):
            # The pool had no key and now has one: wake the agents' watches.
            async with self._changed[pool.name]:
                self._changed[pool.name].notify_all()
        return r.result

    # ------------------------------------------------------ reports, commands

    def report(self, agent: str, body: dict[str, Any]) -> None:
        self.reports[agent] = {"at": time.time(), "report": body}

    async def queue(self, agent: str, action: str, target: str, by: str,
                    **args: Any) -> dict[str, Any]:  # fmt: skip
        if agent not in self.cfg.agents:
            raise KeyError(agent)
        cmd = {"id": next(self._command_ids), "agent": agent, "action": action,
               "target": target, "args": args, "by": by, "state": "pending",
               "created": time.time(), "result": None}  # fmt: skip
        self.commands.append(cmd)
        log.info("%s: %s %s on %s (command %d)", by, action, target, agent, cmd["id"])
        async with self._commands_changed[agent]:
            self._commands_changed[agent].notify_all()
        return cmd

    def _pending(self, agent: str) -> list[dict[str, Any]]:
        return [c for c in self.commands if c["agent"] == agent and c["state"] == "pending"]

    async def take(self, agent: str, timeout: float) -> list[dict[str, Any]]:
        """An agent's pending commands, waiting up to `timeout` for one."""
        cond = self._commands_changed[agent]
        async with cond:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(cond.wait_for(lambda: bool(self._pending(agent))), timeout)
        cmds = self._pending(agent)
        for c in cmds:
            c["state"] = "sent"
        return [{k: c[k] for k in ("id", "action", "target", "args")} for c in cmds]

    def finish(self, agent: str, cmd_id: int, result: dict[str, Any]) -> None:
        for c in self.commands:
            if c["id"] == cmd_id and c["agent"] == agent:
                c["state"] = "done" if result.get("ok") else "failed"
                c["result"] = result
                c["finished"] = time.time()
                return
        raise KeyError(f"command {cmd_id}")

    def state(self) -> dict[str, Any]:
        """Everything the UI shows."""
        return {
            "now": time.time(),
            "agents": [
                {"agent": name, "pools": a.pools, **(self.reports.get(name) or {})}
                for name, a in self.cfg.agents.items()
            ],
            "pools": [p.describe() for p in self.pools.values()],
            "claims": self._read_claims(),
            "commands": list(self.commands)[::-1],
            "tune_locks": {u: h for u, h in self.tune_locks.items() if h["until"] > time.time()},
        }

    # --------------------------------------------------------------- claims

    def _read_claims(self) -> dict[str, list[dict[str, str]]]:
        try:
            data: dict[str, list[dict[str, str]]] = json.loads(self.claims_file.read_text())
            return data
        except FileNotFoundError:
            return {}

    def claim(self, agent: str, wanted: list[dict[str, str]]) -> dict[str, Any]:
        """Replace an agent's claims with `wanted`, minus any that conflict with
        another agent's. Returns the conflicts."""
        claims = self._read_claims()
        accepted, conflicts = [], []
        for c in wanted:
            clash = next(
                (
                    (owner, o)
                    for owner, theirs in claims.items()
                    if owner != agent
                    for o in theirs
                    if o["target"] == c["target"]
                    and overlaps(o["dest"], c["dest"])
                    and "sync" in (o["mode"], c["mode"])
                ),
                None,
            )
            if clash:
                owner, o = clash
                conflicts.append({"transfer": c["transfer"], "owner": owner,
                                  "owner_transfer": o["transfer"]})  # fmt: skip
                log.warning("agent %s: transfer %s refused, overlaps %s's %s",
                            agent, c["transfer"], owner, o["transfer"])  # fmt: skip
            else:
                accepted.append({k: c[k] for k in ("transfer", "target", "dest", "mode")})
        claims[agent] = accepted
        self.claims_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.claims_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(claims, indent=2, sort_keys=True) + "\n")
        os.replace(tmp, self.claims_file)
        return {"accepted": [c["transfer"] for c in accepted], "conflicts": conflicts}


# -------------------------------------------------------------------- HTTP

Handler = Callable[[Request, str], Awaitable[Any]]


def build_app(co: Controller) -> Starlette:
    def authed(handler: Handler) -> Callable[[Request], Awaitable[JSONResponse]]:
        async def wrapped(request: Request) -> JSONResponse:
            agent = co.agent_for(request.headers.get("authorization"))
            if agent is None:
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            try:
                return JSONResponse(await handler(request, agent))
            except Forbidden as e:
                return JSONResponse({"error": str(e)}, status_code=403)
            except KeyError as e:
                return JSONResponse({"error": f"not found: {e.args[0]}"}, status_code=404)
            except PoolExhausted as e:
                return JSONResponse({"error": str(e), "frees_at": e.frees_at}, status_code=409)
            except NoOtherKey as e:
                return JSONResponse({"error": str(e)}, status_code=409)
            except PoolError as e:
                log.error("%s", e)
                return JSONResponse({"error": str(e)}, status_code=500)

        return wrapped

    async def healthz(_: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    async def pools(_: Request, agent: str) -> Any:
        return [co.pools[p].describe() for p in co.cfg.agents[agent].pools]

    async def pool(request: Request, agent: str) -> Any:
        return co.pool_for(agent, request.path_params["pool"]).describe()

    async def lease(request: Request, agent: str) -> Any:
        return co.lease(co.pool_for(agent, request.path_params["pool"]))

    async def exhausted(request: Request, agent: str) -> Any:
        p = co.pool_for(agent, request.path_params["pool"])
        body = await request.json() if await request.body() else {}
        return await co.exhausted(agent, p, body.get("expected"))

    async def watch(request: Request, agent: str) -> Any:
        p = co.pool_for(agent, request.path_params["pool"])
        epoch = int(request.query_params.get("epoch", "-1"))
        return await co.watch(p, epoch, co.cfg.watch_timeout)

    async def put_claims(request: Request, agent: str) -> Any:
        wanted = await request.json()
        for c in wanted:
            if not all(isinstance(c.get(k), str) for k in ("transfer", "target", "dest", "mode")):
                raise KeyError("claim fields transfer/target/dest/mode")
        return co.claim(agent, wanted)

    async def get_claims(_: Request, agent: str) -> Any:
        return co._read_claims()

    async def put_report(request: Request, agent: str) -> Any:
        co.report(agent, await request.json())
        return {"ok": True}

    async def tune_lock(request: Request, agent: str) -> Any:
        body = await request.json() if await request.body() else {}
        return co.tune_lock(agent, request.path_params["uplink"], float(body.get("ttl", 3600)))

    async def tune_unlock(request: Request, agent: str) -> Any:
        return co.tune_unlock(agent, request.path_params["uplink"])

    async def get_commands(_: Request, agent: str) -> Any:
        return await co.take(agent, co.cfg.watch_timeout)

    async def post_command(request: Request, agent: str) -> Any:
        co.finish(agent, int(request.path_params["id"]), await request.json())
        return {"ok": True}

    async def metrics(_: Request) -> Response:
        return PlainTextResponse(controller_metrics(co), media_type=CONTENT_TYPE)

    @contextlib.asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        task = asyncio.create_task(co.probe_loop())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route("/healthz", healthz),
            Route("/metrics", metrics),
            Route("/v1/report", authed(put_report), methods=["PUT"]),
            Route("/v1/tune-lock/{uplink}", authed(tune_lock), methods=["POST"]),
            Route("/v1/tune-lock/{uplink}", authed(tune_unlock), methods=["DELETE"]),
            Route("/v1/commands", authed(get_commands)),
            Route("/v1/commands/{id:int}", authed(post_command), methods=["POST"]),
            Route("/v1/pools", authed(pools)),
            Route("/v1/pools/{pool}", authed(pool)),
            Route("/v1/pools/{pool}/lease", authed(lease)),
            Route("/v1/pools/{pool}/exhausted", authed(exhausted), methods=["POST"]),
            Route("/v1/pools/{pool}/watch", authed(watch)),
            Route("/v1/claims", authed(get_claims)),
            Route("/v1/claims", authed(put_claims), methods=["PUT"]),
            *(_ui_routes(co) if co.cfg.ui.enabled else []),
        ],
    )


# ---------------------------------------------------------------------- UI

UiHandler = Callable[[Request, str], Awaitable[Any]]


def _ui_routes(co: Controller) -> list[Route]:
    header = co.cfg.ui.user_header

    def user_of(request: Request) -> str | None:
        if header is None:
            return "ui"
        return request.headers.get(header) or None

    def guarded(handler: UiHandler) -> Callable[[Request], Awaitable[Response]]:
        async def wrapped(request: Request) -> Response:
            user = user_of(request)
            if user is None:
                return JSONResponse({"error": f"no {header} header: is the UI behind "
                                     "the auth proxy?"}, status_code=403)  # fmt: skip
            # A cross-site form cannot set a custom header; fetch() from the
            # page does. That is the CSRF guard for the forward-auth cookie.
            if request.method == "POST" and request.headers.get("x-ferryman-ui") != "1":
                return JSONResponse({"error": "missing X-Ferryman-UI header"}, status_code=403)
            try:
                return JSONResponse(await handler(request, user))
            except KeyError as e:
                return JSONResponse({"error": f"not found: {e.args[0]}"}, status_code=404)
            except PoolError as e:
                return JSONResponse({"error": str(e)}, status_code=409)

        return wrapped

    page = resources.files("ferryman").joinpath("ui.html").read_text()
    # Open tabs compare this with the one they loaded and reload when the
    # controller starts serving a different page (an upgrade).
    page_id = hashlib.sha256(page.encode()).hexdigest()[:16]

    async def index(request: Request) -> Response:
        if user_of(request) is None:
            return PlainTextResponse(f"no {header} header: is the UI behind the auth proxy?",
                                     status_code=403)  # fmt: skip
        return HTMLResponse(page, headers={"Cache-Control": "no-cache"})

    async def root(_: Request) -> Response:
        return RedirectResponse("/ui/")

    async def state(_: Request, user: str) -> Any:
        return {"user": user, "page": page_id, **co.state()}

    def _agent_has(agent: str, kind: str, name: str) -> None:
        rep = co.reports.get(agent)
        if rep is None:
            raise KeyError(agent)
        names = [x[kind[:-1]] for x in rep["report"].get("status", {}).get(kind, [])]
        if name not in names:
            raise KeyError(f"{agent}/{name}")

    async def restart(request: Request, user: str) -> Any:
        a, u = request.path_params["agent"], request.path_params["name"]
        _agent_has(a, "units", u)
        return await co.queue(a, "restart", u, user)

    async def run_transfer(request: Request, user: str) -> Any:
        a, t = request.path_params["agent"], request.path_params["name"]
        _agent_has(a, "transfers", t)
        body = await request.json() if await request.body() else {}
        return await co.queue(a, "run", t, user, dry_run=bool(body.get("dry_run")))

    async def tune(request: Request, user: str) -> Any:
        a = request.path_params["agent"]
        if a not in co.reports:
            raise KeyError(a)
        body = await request.json() if await request.body() else {}
        apply = body.get("apply")
        return await co.queue(a, "tune", "drive", user, apply=apply)

    async def rotate(request: Request, user: str) -> Any:
        pool = co.pools[request.path_params["pool"]]
        lease = await co.exhausted(f"{user} (ui)", pool, None)
        return {"pool": pool.name, "current": lease["key"], "epoch": lease["epoch"]}

    return [
        Route("/", root),
        Route("/ui/", index),
        Route("/ui/api/state", guarded(state)),
        Route("/ui/api/agents/{agent}/units/{name}/restart", guarded(restart), methods=["POST"]),
        Route(
            "/ui/api/agents/{agent}/transfers/{name}/run", guarded(run_transfer), methods=["POST"]
        ),
        Route("/ui/api/agents/{agent}/tune", guarded(tune), methods=["POST"]),
        Route("/ui/api/pools/{pool}/rotate", guarded(rotate), methods=["POST"]),
    ]


def run(cfg: ControllerConfig) -> None:
    co = Controller(cfg)
    for name, p in co.pools.items():
        for key in p.cfg.keys:
            try:
                p.key_file(key)
            except PoolError as e:
                log.error("%s", e)
        try:
            log.info("pool %s: %d key(s), current %s", name, p.size, p.current())
        except PoolError as e:
            log.error("%s", e)
    uvicorn.run(
        build_app(co),
        host=cfg.listen.host,
        port=cfg.listen.port,
        proxy_headers=True,
        forwarded_allow_ips=cfg.listen.forwarded_allow_ips,
        log_level="warning",
        access_log=False,
        # Agents' long polls would otherwise hold shutdown for watch_timeout;
        # they just retry against the next instance.
        timeout_graceful_shutdown=2,
    )
