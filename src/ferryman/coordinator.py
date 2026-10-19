"""The coordinator: owns the service-account keys and every pool's rotation
state, and leases the current key to agents.

It speaks plain HTTP on a TCP port and expects to sit behind Traefik, which
terminates TLS. Authentication is its own regardless: each agent sends a
bearer token whose sha256 is in the config, and may only touch the pools
listed for it.

It also arbitrates transfer claims. Two agents *moving* (or copying) into the
same place on a drive is harmless - each only adds its own files - but a
`sync` mirrors its source and deletes whatever else is there, so a sync
whose destination overlaps another agent's transfer would delete that
agent's uploads. Those claims are refused.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import PurePosixPath
from typing import Any

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .config import CoordinatorConfig
from .pool import LocalPool, PoolError, PoolExhausted

log = logging.getLogger(__name__)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Forbidden(Exception):
    pass


def overlaps(a: str, b: str) -> bool:
    """Whether two paths inside a drive are the same or one contains the other."""
    pa, pb = PurePosixPath("/", a).parts, PurePosixPath("/", b).parts
    n = min(len(pa), len(pb))
    return pa[:n] == pb[:n]


class Coordinator:
    def __init__(self, cfg: CoordinatorConfig) -> None:
        self.cfg = cfg
        self.pools = {
            name: LocalPool(name, p, cfg.state / "pools" / f"{name}.json")
            for name, p in cfg.pools.items()
        }
        self._changed = {name: asyncio.Condition() for name in cfg.pools}
        self.claims_file = cfg.state / "claims.json"

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
            log.warning("pool %s: rotated on a cap hit reported by %s", pool.name, agent)
        return self.lease(pool)

    async def watch(self, pool: LocalPool, epoch: int, timeout: float) -> dict[str, Any]:
        cond = self._changed[pool.name]
        async with cond:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(cond.wait_for(lambda: pool.epoch != epoch), timeout)
        return self.lease(pool)

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


def build_app(co: Coordinator) -> Starlette:
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

    return Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/v1/pools", authed(pools)),
            Route("/v1/pools/{pool}", authed(pool)),
            Route("/v1/pools/{pool}/lease", authed(lease)),
            Route("/v1/pools/{pool}/exhausted", authed(exhausted), methods=["POST"]),
            Route("/v1/pools/{pool}/watch", authed(watch)),
            Route("/v1/claims", authed(get_claims)),
            Route("/v1/claims", authed(put_claims), methods=["PUT"]),
        ]
    )


def run(cfg: CoordinatorConfig) -> None:
    co = Coordinator(cfg)
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
