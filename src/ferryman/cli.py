"""`ferryman agent` runs the daemon; every other command talks to it over its
unix socket (except `verify`, which checks the config offline)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Annotated, Any, NoReturn

import httpx
import typer

from .config import Config, Layout
from .pool import LocalPool, PoolError
from .transfer import describe, iec

app = typer.Typer(no_args_is_help=True, add_completion=False, help=__doc__)

DEFAULT_CONFIG = Path(os.environ.get("FERRYMAN_CONFIG", "/etc/ferryman/config.yaml"))


class _Ctx:
    def __init__(self, config: Path, socket: Path | None, as_json: bool) -> None:
        self.config_path, self._socket, self.as_json = config, socket, as_json

    def config(self) -> Config:
        try:
            return Config.load(self.config_path)
        except FileNotFoundError:
            _die(f"config not found: {self.config_path}")
        except ValueError as e:
            _die(f"invalid config {self.config_path}:\n{e}")

    @property
    def socket(self) -> Path:
        if self._socket:
            return self._socket
        if env := os.environ.get("FERRYMAN_SOCKET"):
            return Path(env)
        return Layout(self.config()).api_socket

    def request(self, method: str, path: str, **kw: Any) -> Any:
        transport = httpx.HTTPTransport(uds=str(self.socket))
        try:
            with httpx.Client(transport=transport, base_url="http://ferryman", timeout=None) as c:
                r = c.request(method, path, **kw)
        except httpx.TransportError as e:
            _die(f"cannot reach the agent on {self.socket}: {e}")
        body = r.json()
        if r.status_code != 200:
            _die(body.get("error", f"HTTP {r.status_code}"))
        return body

    def show(self, data: Any, render: Any) -> None:
        if self.as_json:
            typer.echo(json.dumps(data, indent=2))
        else:
            render(data)


def _die(msg: str) -> NoReturn:
    typer.secho(msg, fg="red", err=True)
    raise typer.Exit(1)


def _ago(ts: float | None) -> str:
    if not ts:
        return "-"
    s = int(time.time() - ts)
    future, s = s < 0, abs(s)
    span = f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"
    return f"in {span}" if future else f"{span} ago"


@app.callback()
def _root(
    ctx: typer.Context,
    config: Annotated[Path, typer.Option("--config", "-c", help="Config file")] = DEFAULT_CONFIG,
    socket: Annotated[Path | None, typer.Option(help="Agent API socket")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Raw JSON output")] = False,
) -> None:
    ctx.obj = _Ctx(config, socket, as_json)


@app.command()
def agent(
    ctx: typer.Context,
    log_level: Annotated[str, typer.Option(help="Python log level")] = "INFO",
) -> None:
    """Run the daemon."""
    from .agent import Agent

    logging.basicConfig(
        level=log_level.upper(),
        stream=sys.stderr,
        # Under systemd the journal adds its own timestamps.
        format="%(levelname)s %(name)s: %(message)s"
        if os.environ.get("JOURNAL_STREAM")
        else "%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(Agent(ctx.obj.config()).run())


@app.command()
def verify(ctx: typer.Context) -> None:
    """Check the config, every pool's key files and the rclone binary (offline)."""
    cfg = ctx.obj.config()
    layout, failed = Layout(cfg), 0
    rclone = shutil.which(cfg.rclone.binary)
    if rclone:
        out = subprocess.run([rclone, "version"], capture_output=True, text=True, check=False)
        typer.echo(f"[PASS] rclone: {out.stdout.splitlines()[0] if out.stdout else rclone}")
    else:
        typer.echo(f"[FAIL] rclone: {cfg.rclone.binary} not found")
        failed += 1
    for name, p in cfg.pools.items():
        pool = LocalPool(name, p, layout.pool_state(name))
        errors = []
        for key in p.keys:
            try:
                pool.key_file(key)
            except PoolError as e:
                errors.append(str(e))
        typer.echo(f"[{'FAIL' if errors else 'PASS'}] pool {name}: {len(p.keys)} key(s)")
        for err in errors:
            typer.echo(f"  - {err}")
        failed += bool(errors)
    typer.echo(f"[PASS] {len(cfg.units)} unit(s), {len(cfg.transfers)} transfer(s)")
    raise typer.Exit(1 if failed else 0)


@app.command()
def status(ctx: typer.Context) -> None:
    """Units and transfers."""

    def render(d: dict[str, Any]) -> None:
        typer.echo(f"{'UNIT':24} {'STATE':9} {'SINCE':>12} {'RESTARTS':>8}  LAST ERROR")
        for u in d["units"]:
            typer.echo(
                f"{u['unit']:24} {u['state']:9} {_ago(u['since']):>12} {u['restarts']:>8}  "
                f"{(u['last_error'] or '')[:60]}"
            )
        if d["transfers"]:
            typer.echo(f"\n{'TRANSFER':24} {'MODE':5} {'STATE':8} {'NEXT':>12}  LAST RUN")
            for t in d["transfers"]:
                last = t["last"]
                if last is None:
                    summary = "-"
                elif last["ok"]:
                    summary = f"{_ago(last['finished'])}: {describe(last)}"
                else:
                    summary = f"{_ago(last['finished'])}: FAILED {last['error'][:50]}"
                state = "running" if t["running"] else "idle"
                typer.echo(
                    f"{t['transfer']:24} {t['mode']:5} {state:8} "
                    f"{_ago(t['next_run']):>12}  {summary}"
                )

    ctx.obj.show(ctx.obj.request("GET", "/v1/status"), render)


@app.command()
def stats(ctx: typer.Context, pattern: Annotated[str, typer.Argument()] = "") -> None:
    """Live transfer stats per unit."""

    def render(rows: list[dict[str, Any]]) -> None:
        typer.echo(f"{'UNIT':24} {'SPEED':>10} {'BYTES':>9} {'FILES':>6} {'ERRORS':>6}  LAST ERROR")
        for r in rows:
            if "error" in r:
                typer.echo(f"{r['unit']:24} ({r['error']})")
                continue
            typer.echo(
                f"{r['unit']:24} {iec(r['speed']) + '/s':>10} {iec(r['bytes']):>9} "
                f"{r['files']:>6} {r['errors']:>6}  {(r['last_error'] or '')[:60]}"
            )

    ctx.obj.show(ctx.obj.request("GET", "/v1/stats", params={"pattern": pattern}), render)


@app.command()
def jobs(ctx: typer.Context, pattern: Annotated[str, typer.Argument()] = "") -> None:
    """rc jobs running inside each unit's process."""

    def render(rows: list[dict[str, Any]]) -> None:
        if not rows:
            typer.echo("(no running jobs)")
            return
        typer.echo(f"{'UNIT':24} {'JOB':>6} {'GROUP':28} RUNNING FOR")
        for r in rows:
            if "error" in r:
                typer.echo(f"{r['unit']:24} ({r['error']})")
                continue
            typer.echo(
                f"{r['unit']:24} {r['jobid']:>6} {r['group'] or '-':28} {r['duration'] or 0:.0f}s"
            )

    ctx.obj.show(ctx.obj.request("GET", "/v1/jobs", params={"pattern": pattern}), render)


@app.command()
def sync(
    ctx: typer.Context,
    transfer: Annotated[str, typer.Argument(help="Transfer name")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Submit with DryRun set")] = False,
    wait: Annotated[bool, typer.Option(help="Wait for the result")] = True,
) -> None:
    """Run a transfer now."""
    body = {"dry_run": dry_run, "wait": wait}
    result = ctx.obj.request("POST", f"/v1/transfers/{transfer}/run", json=body)

    def render(r: dict[str, Any]) -> None:
        if "started" in r and "ok" not in r:
            typer.echo(f"transfer {transfer} started")
        elif r["ok"]:
            typer.echo(f"transfer {transfer}: {describe(r)}")
        else:
            _die(f"transfer {transfer} failed: {r['error']}")

    ctx.obj.show(result, render)


@app.command()
def restart(ctx: typer.Context, unit: str) -> None:
    """Restart a unit's rclone process."""
    r = ctx.obj.request("POST", f"/v1/units/{unit}/restart")
    ctx.obj.show(r, lambda u: typer.echo(f"unit {u['unit']}: {u['state']}"))


@app.command()
def pool(ctx: typer.Context) -> None:
    """Key pools: current key, exhausted keys, key-file health."""

    def render(pools: list[dict[str, Any]]) -> None:
        for p in pools:
            typer.echo(f"{p['pool']} (window {p['quota_window']}s)")
            for k in p["keys"]:
                state = (
                    "exhausted, frees "
                    + time.strftime("%F %T", time.localtime(k["exhausted_until"]))
                    if k["exhausted_until"]
                    else "available"
                )
                typer.echo(
                    f"  {'*' if k['current'] else ' '} {k['key']:66} "
                    f"{'ok' if k['file_ok'] else 'BAD':4} {state}"
                )
            typer.echo("")

    ctx.obj.show(ctx.obj.request("GET", "/v1/pools"), render)


@app.command()
def rotate(ctx: typer.Context, pool_name: Annotated[str, typer.Argument(metavar="POOL")]) -> None:
    """Advance a pool to its next key and move running units onto it."""
    r = ctx.obj.request("POST", f"/v1/pools/{pool_name}/rotate")
    ctx.obj.show(r, lambda d: typer.echo(f"pool {d['pool']} now on {d['current']}"))


@app.command()
def throttle(ctx: typer.Context, pattern: str, rate: str) -> None:
    """Set ("10M", "5M:1M") or clear ("off") a bandwidth limit until restart."""
    rows = ctx.obj.request("POST", "/v1/throttle", json={"pattern": pattern, "rate": rate})

    def render(rs: list[dict[str, Any]]) -> None:
        if not rs:
            _die(f"no ready unit matches {pattern}")
        for r in rs:
            typer.echo(f"{r['unit']}: {r.get('rate') or r.get('error')}")

    ctx.obj.show(rows, render)


@app.command()
def health(ctx: typer.Context) -> None:
    """Run the health check now."""
    r = ctx.obj.request("POST", "/v1/health")

    def render(d: dict[str, str]) -> None:
        for unit, state in d.items():
            typer.echo(f"{unit:24} {state}")

    ctx.obj.show(r, render)


def main() -> None:
    app()
