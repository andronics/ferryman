"""The controller: holds the config every agent runs, owns the
service-account keys and every pool's rotation state, and leases the
current key to agents.

Each agent fetches its slice of the config (its bound units, its transfers,
its settings) with a long poll on /v2/config, and is woken when the slice
changes: the controller re-reads its file on SIGHUP and whenever the file
changes. A config that fails to load leaves the previous one in force.

It speaks plain HTTP on a TCP port and expects to sit behind Traefik, which
terminates TLS. Authentication is its own regardless: each agent sends a
bearer token whose sha256 is in the config, and may only lease the pools of
the units bound to it.

Agents report their status to it every few seconds and collect commands
(restart a unit, run a transfer) from it with long polls, which is what its
web UI at /ui/ shows and drives. The UI has no login of its own: Traefik puts
it behind forward-auth (authentik), and the controller trusts the user
header that sets.
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
import signal
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from importlib import resources
from pathlib import Path
from typing import Any

import uvicorn
import yaml
from pydantic import ValidationError
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

from .alerts import Alerts
from .config import ControllerConfig, parse_size
from .events import EventStore
from .metrics import CONTENT_TYPE, controller_metrics
from .pool import LocalPool, NoOtherKey, PoolError, PoolExhausted
from .probe import probe

log = logging.getLogger(__name__)
# Longest an uplink's tune lock lasts if its agent never releases it.
TUNE_LOCK_MAX = 7200.0
# How often the probe loop looks for exhausted keys due a probe.
PROBE_TICK = 60.0
# How often the config file is checked for changes (a Kubernetes Secret
# update replaces it in place; SIGHUP reloads at once).
CONFIG_TICK = 10.0
# How often agents' silence and pools' exhaustion are looked at.
WATCH_TICK = 30.0


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class Forbidden(Exception):
    pass


class Controller:
    def __init__(self, cfg: ControllerConfig, path: Path | None = None) -> None:
        self.cfg = cfg
        # The file cfg came from, re-read on change; None: never reloaded.
        self.path = path
        self._file_id = _file_id(path)
        # Why the file's current contents aren't in force, if they aren't.
        self.config_error: str | None = None
        self._config_changed = asyncio.Condition()
        self.pools: dict[str, LocalPool] = {}
        self._changed: dict[str, asyncio.Condition] = {}
        self._commands_changed: dict[str, asyncio.Condition] = {}
        self._slices: dict[str, dict[str, Any]] = {}
        self._apply(cfg)
        self.events = EventStore(cfg.state, cfg.events.retention_days * 86400.0)
        # Only the controller notifies: its rules over every event.
        self.alerts = Alerts(cfg.notify, cfg.state, bound=self.bound_to)
        self.events.listeners.append(self.alerts.on_event)
        self.started = time.time()
        # Problems the controller has raised and not yet seen end.
        self._silent: set[str] = set()
        self._exhausted: set[str] = set()
        self._rejected: dict[str, Any] = {}
        # agent -> {"at": when, "report": what it last said}; in memory only,
        # agents re-report within seconds of a controller restart.
        self.reports: dict[str, dict[str, Any]] = {}
        # Commands for agents, newest last; pending ones wait for the agent's
        # next long poll.
        self.commands: collections.deque[dict[str, Any]] = collections.deque(maxlen=100)
        self._command_ids = itertools.count(1)
        # uplink -> {"agent", "until"}: agents behind one uplink tune one at a
        # time. In memory: a controller restart frees them, which costs at
        # most one overlapping tune.
        self.tune_locks: dict[str, dict[str, Any]] = {}
        # pool -> the index of the drive its last probe used (drive: rotate).
        self._probe_turn: dict[str, int] = {}

    # ---------------------------------------------------------------- config

    def _apply(self, cfg: ControllerConfig) -> None:
        """Put cfg in force. Pool state lives in files, so pools rebuilt from
        a new config carry on where they were."""
        self.cfg = cfg
        self.pools = {name: LocalPool(name, p, cfg.state / "pools" / f"{name}.json")
                      for name, p in cfg.pools.items()}  # fmt: skip
        for name in cfg.pools:
            self._changed.setdefault(name, asyncio.Condition())
        for name in cfg.agents:
            self._commands_changed.setdefault(name, asyncio.Condition())
        self._slices = {name: cfg.slice(name) for name in cfg.agents}

    def bound_to(self, unit: str) -> set[str]:
        """The agents a unit is bound on."""
        return {name for name, a in self.cfg.agents.items() if unit in a.units}

    async def reload(self, cfg: ControllerConfig) -> list[str]:
        """Put a new config in force and wake every agent whose slice changed.
        Returns those agents."""
        before = {a: s["version"] for a, s in self._slices.items()}
        self._apply(cfg)
        self.alerts.update(cfg.notify)
        self.config_error = None
        changed = [a for a, s in self._slices.items() if before.get(a) != s["version"]]
        async with self._config_changed:
            self._config_changed.notify_all()
        for name in cfg.pools:  # a pool's keys may have changed
            async with self._changed[name]:
                self._changed[name].notify_all()
        log.info("config reloaded: %d agent(s), %d changed", len(cfg.agents), len(changed))
        return changed

    async def check_config(self, force: bool = False) -> None:
        """Reload if the file changed (or `force`). A file that fails to load
        leaves the previous config in force, with the error kept."""
        if self.path is None:
            return
        file_id = _file_id(self.path)
        if not force and file_id == self._file_id:
            return
        self._file_id = file_id
        try:
            cfg = ControllerConfig.load(self.path)
        except (OSError, yaml.YAMLError, ValidationError, ValueError) as e:
            self.config_error = f"{self.path}: {e}"
            log.error("config not reloaded, the previous one stays in force: %s", e)
            why = f"not reloaded, the previous config stays in force: {e}"
            self.events.emit("config.invalid", str(self.path), why, "error")
            return
        was_invalid = self.config_error is not None
        await self.reload(cfg)
        if was_invalid:
            self.events.emit("config.reloaded", str(self.path), "valid again and in force",
                             "info", resolves=True)  # fmt: skip

    async def config_loop(self) -> None:
        while True:
            await asyncio.sleep(CONFIG_TICK)
            try:
                await self.check_config()
            except Exception:
                log.exception("config check failed")

    async def slice_for(self, agent: str, version: str | None, timeout: float) -> dict[str, Any]:
        """The agent's slice: at once when its version isn't `version`, else
        when it changes or after `timeout` (then just {"version": version})."""
        async with self._config_changed:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._config_changed.wait_for(
                    lambda: agent not in self._slices
                    or self._slices[agent]["version"] != version), timeout)  # fmt: skip
        if agent not in self._slices:
            raise KeyError(agent)  # removed from the config while it waited
        slc = self._slices[agent]
        return slc if slc["version"] != version else {"version": version}

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
        if pool not in self.cfg.agent_pools(agent):
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
        except PoolExhausted as e:
            if pool.name not in self._exhausted:
                self._exhausted.add(pool.name)
                self.events.emit("pool.exhausted", pool.name, str(e), "error")
            raise
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

    def probe_drive(self, pool: str) -> str:
        """The drive the pool's next probe uploads to (see Probe.drive)."""
        drives = self.cfg.probe_drives(pool)
        probe = self.cfg.pools[pool].probe
        if probe is None or probe.drive == "first" or len(drives) == 1:
            return drives[0]
        turn = self._probe_turn.get(pool, -1) + 1
        self._probe_turn[pool] = turn
        return drives[turn % len(drives)]

    async def probe_key(self, pool: LocalPool, key: str, marked: float) -> str:
        cfg = pool.cfg.probe
        assert cfg is not None
        r = await probe(self.cfg.rclone.binary, pool.key_file(key), self.probe_drive(pool.name),
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
        if r.result == "ok" and key in pool.available():
            self.events.emit("probe.recovered", pool.name, f"{key} can upload again", "info")
            self.check_pools()
        return r.result

    # ---------------------------------------------------- the controller's own

    def check_pools(self) -> None:
        """An exhausted pool that has a key again (its window ended, or a
        probe found one) has recovered."""
        for name in list(self._exhausted):
            pool = self.pools.get(name)
            if pool is None or pool.available():
                self._exhausted.discard(name)
                self.events.emit("pool.recovered", name, "a key can upload again", "info",
                                 resolves=True)  # fmt: skip

    def check_agents(self) -> None:
        """agent.silent for an agent that hasn't reported for silent_after
        (since the controller started, for one never heard from)."""
        now, after = time.time(), self.cfg.events.silent_after
        for name in self.cfg.agents:
            rep = self.reports.get(name)
            last = rep["at"] if rep else self.started
            if now - last > after and name not in self._silent:
                self._silent.add(name)
                seen = f"last report {time.strftime('%F %T', time.localtime(last))}" if rep \
                    else "no report since the controller started"  # fmt: skip
                self.events.emit("agent.silent", name, f"quiet for {now - last:.0f}s ({seen})")
        self._silent &= set(self.cfg.agents)

    async def watch_loop(self) -> None:
        while True:
            await asyncio.sleep(WATCH_TICK)
            try:
                self.check_agents()
                self.check_pools()
            except Exception:
                log.exception("watch failed")

    # ------------------------------------------------------ reports, commands

    def report(self, agent: str, body: dict[str, Any]) -> None:
        self.reports[agent] = {"at": time.time(), "report": body}
        if agent in self._silent:
            self._silent.discard(agent)
            self.events.emit("agent.recovered", agent, "reporting again", "info", resolves=True)
        cfg = body.get("config") or {}
        if cfg.get("state") == "rejected":
            seen = (cfg.get("offered"), cfg.get("error"))
            if self._rejected.get(agent) != seen:
                self._rejected[agent] = seen
                self.events.emit("agent.config_rejected", agent,
                                 f"version {cfg.get('offered')} refused, {cfg.get('version')} "
                                 f"stays in force: {cfg.get('error')}", "error")  # fmt: skip
        elif agent in self._rejected and cfg.get("state") in ("applied", "pending"):
            del self._rejected[agent]
            now_in = f"version {cfg.get('version')} in force"
            self.events.emit("agent.config_applied", agent, now_in, "info", resolves=True)

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
                {
                    "agent": name,
                    "pools": self.cfg.agent_pools(name),
                    **(self.reports.get(name) or {}),
                }
                for name in self.cfg.agents
            ],
            "pools": [p.describe() for p in self.pools.values()],
            "config_error": self.config_error,
            "events": self.events.latest(200),
            "alerts": self.alerts.describe(),
            "commands": list(self.commands)[::-1],
            "tune_locks": {u: h for u, h in self.tune_locks.items() if h["until"] > time.time()},
        }


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
        return [co.pools[p].describe() for p in co.cfg.agent_pools(agent)]

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

    async def get_config(request: Request, agent: str) -> Any:
        return await co.slice_for(agent, request.query_params.get("version"),
                                  co.cfg.watch_timeout)  # fmt: skip

    async def gone(_: Request) -> JSONResponse:
        return JSONResponse({"error": "this controller speaks /v2 (ferryman 0.3): upgrade "
                             "the agent"}, status_code=410)  # fmt: skip

    async def post_events(request: Request, agent: str) -> Any:
        body = await request.json()
        acked = co.events.ingest(agent, str(body["outbox"]), body["events"])
        co.alerts.end_batch(agent)
        return {"acked": acked}

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
        tasks = [
            asyncio.create_task(co.probe_loop()),
            asyncio.create_task(co.config_loop()),
            asyncio.create_task(co.watch_loop()),
            asyncio.create_task(co.alerts.run()),
        ]
        loop = asyncio.get_running_loop()

        def hup() -> None:
            tasks.append(asyncio.create_task(co.check_config(force=True)))

        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal.SIGHUP, hup)
        try:
            yield
        finally:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(signal.SIGHUP)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route("/healthz", healthz),
            Route("/metrics", metrics),
            Route("/v2/config", authed(get_config)),
            Route("/v2/report", authed(put_report), methods=["PUT"]),
            Route("/v2/events", authed(post_events), methods=["POST"]),
            Route("/v2/tune-lock/{uplink}", authed(tune_lock), methods=["POST"]),
            Route("/v2/tune-lock/{uplink}", authed(tune_unlock), methods=["DELETE"]),
            Route("/v2/commands", authed(get_commands)),
            Route("/v2/commands/{id:int}", authed(post_command), methods=["POST"]),
            Route("/v2/pools", authed(pools)),
            Route("/v2/pools/{pool}", authed(pool)),
            Route("/v2/pools/{pool}/lease", authed(lease)),
            Route("/v2/pools/{pool}/exhausted", authed(exhausted), methods=["POST"]),
            Route("/v2/pools/{pool}/watch", authed(watch)),
            # 0.2 agents: a clear answer instead of a 404.
            Route("/v1/{rest:path}", gone, methods=["GET", "POST", "PUT", "DELETE"]),
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


def _file_id(path: Path | None) -> tuple[int, int, int] | None:
    """What changes when a file is rewritten or replaced (a Secret's
    atomic symlink swap gives a new inode)."""
    if path is None:
        return None
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_ino, st.st_mtime_ns, st.st_size)


def run(cfg: ControllerConfig, path: Path | None = None) -> None:
    co = Controller(cfg, path)
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
