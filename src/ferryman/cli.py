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
DEFAULT_COORDINATOR_CONFIG = Path(
    os.environ.get("FERRYMAN_COORDINATOR_CONFIG", "/etc/ferryman/coordinator.yaml")
)


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


def _span(seconds: float) -> str:
    s = int(abs(seconds))
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def _ago(ts: float | None) -> str:
    if not ts:
        return "-"
    s = time.time() - ts
    return f"in {_span(s)}" if s < 0 else f"{_span(s)} ago"


def _warm(w: dict[str, Any] | None) -> str:
    """The last dir-cache warm-up, e.g. "done 1m05s, 2h03m ago"."""
    if not w:
        return "-"
    if w["state"] == "running":
        return f"running {_span(time.time() - w['started'])}"
    return f"{w['state']} {_span(w['finished'] - w['started'])}, {_ago(w['finished'])}"


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

    cfg = ctx.obj.config()
    if problems := Layout(cfg).check_paths():
        _die("\n".join(problems))
    logging.basicConfig(
        level=log_level.upper(),
        stream=sys.stderr,
        # Under systemd the journal adds its own timestamps.
        format="%(levelname)s %(name)s: %(message)s"
        if os.environ.get("JOURNAL_STREAM")
        else "%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Every rc call is an HTTP request; their per-connection logs drown the
    # agent's own at DEBUG.
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    asyncio.run(Agent(cfg).run())


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
    if cfg.coordinator:
        tf = cfg.coordinator.token_file
        ok = os.access(tf, os.R_OK) and bool(tf.read_text().strip())
        typer.echo(f"[{'PASS' if ok else 'FAIL'}] coordinator {cfg.coordinator.url}: token {tf}")
        failed += not ok
    for name, p in cfg.pools.items():
        if p.coordinator:
            typer.echo(f"[PASS] pool {name}: served by the coordinator")
            continue
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
    if (mail := cfg.notify.mail) is not None:
        ok = shutil.which(mail.sendmail) is not None
        typer.echo(f"[{'PASS' if ok else 'FAIL'}] notify: mail to {', '.join(mail.to)} "
                   f"via {mail.sendmail}")  # fmt: skip
        failed += not ok
    for problem in layout.check_paths():
        typer.echo(f"[FAIL] {problem}")
        failed += 1
    typer.echo(f"[PASS] {len(cfg.units)} unit(s), {len(cfg.transfers)} transfer(s)")
    raise typer.Exit(1 if failed else 0)


@app.command()
def status(ctx: typer.Context) -> None:
    """Units and transfers."""

    def render(d: dict[str, Any]) -> None:
        typer.echo(
            f"{'UNIT':24} {'STATE':9} {'SINCE':>12} {'RESTARTS':>8}  {'WARM':28}  LAST ERROR"
        )
        for u in d["units"]:
            typer.echo(
                f"{u['unit']:24} {u['state']:9} {_ago(u['since']):>12} {u['restarts']:>8}  "
                f"{_warm(u.get('warm')):28}  {(u['last_error'] or '')[:60]}"
            )
        mounts = [(u["unit"], m) for u in d["units"] for m in u.get("mounts") or []]
        if mounts:
            typer.echo(f"\n{'MOUNT':48} {'UNIT':24} STATE")
            for unit, m in mounts:
                state = f"{m['state']} ({m.get('kind', '?')}{', ro' if m.get('read_only') else ''})"
                typer.echo(f"{m['path']:48} {unit:24} {state}")
        serves = [(u["unit"], sv) for u in d["units"] for sv in u.get("serves") or []]
        if serves:
            typer.echo(f"\n{'SERVE':8} {'ADDRESS':39} {'UNIT':24} STATE")
            for unit, sv in serves:
                typer.echo(f"{sv['type']:8} {sv['addr']:39} {unit:24} {sv['state']}")
        # Read settings only where they differ from ferryman's built-ins.
        tuned = [(u["unit"], {k: v for k, v in (u.get("read") or {}).items()
                              if v["source"] != "built-in"}) for u in d["units"]]  # fmt: skip
        if any(r for _, r in tuned):
            typer.echo(f"\n{'UNIT':24} READ SETTINGS (not built-in)")
            for name, r in tuned:
                if r:
                    typer.echo(f"{name:24} " + ", ".join(
                        f"{k}={v['value']} ({v['source']})" for k, v in r.items()))  # fmt: skip
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
def run(
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


# `remote sync` is what the workstation tool called it.
app.command("sync", hidden=True)(run)


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
            if "error" in p:
                typer.echo(f"{p['pool']}: {p['error']}\n")
                continue
            typer.echo(f"{p['pool']} (window {p['quota_window']}s, epoch {p['epoch']})")
            for k in p["keys"]:
                state = (
                    "exhausted, frees "
                    + time.strftime("%F %T", time.localtime(k["exhausted_until"]))
                    if k["exhausted_until"]
                    else "available"
                    if k["file_ok"]
                    else "skipped"
                )
                pr = k.get("probe")
                if (
                    pr
                    and k["exhausted_until"]
                    and pr["at"] >= k["exhausted_until"] - p["quota_window"]
                ):
                    when = time.strftime("%F %T", time.localtime(pr["at"]))
                    said = "still capped" if pr["result"] == "capped" else pr["message"]
                    state += f"; probed {when}: {said}"
                typer.echo(
                    f"  {'*' if k['current'] else ' '} {k['key']:66} "
                    f"{'ok' if k['file_ok'] else 'BAD':4} {state}"
                )
            typer.echo("")

    ctx.obj.show(ctx.obj.request("GET", "/v1/pools"), render)


@app.command()
def rotate(ctx: typer.Context, pool_name: Annotated[str, typer.Argument(metavar="POOL")]) -> None:
    """Move a pool to its next usable key (the current one keeps its quota)
    and move running units onto it."""
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


def _profile(p: dict[str, Any]) -> None:
    typer.echo(f"{'CHUNK':>7} {'XFERS':>5} {'RATE':>11} {'PER STREAM':>11} {'RUNS':>18} "
               f"{'CPU':>5} {'RSS':>7}")  # fmt: skip
    for r in p["probes"]:
        runs = "/".join(f"{iec(x)}" for x in r["runs"])
        mark = "  <- chosen" if (r["chunk"], r["transfers"]) == (p["chunk_size"], p["transfers"]) \
            else ""  # fmt: skip
        flags = " rate-limited" if r["rate_limited"] else " noisy" if r["noisy"] else ""
        cpu = f"{r['cpu']:.2f}" if r["cpu"] is not None else "-"
        rss = iec(r["rss"]) if r["rss"] is not None else "-"
        typer.echo(f"{iec(r['chunk']):>7} {r['transfers']:>5} {iec(r['rate']) + '/s':>11} "
                   f"{iec(r['per_stream']) + '/s':>11} {runs:>18} {cpu:>5} {rss:>7}"
                   f"{flags}{mark}")  # fmt: skip
    when = time.strftime("%F %T", time.localtime(p["at"]))
    typer.echo(
        f"\nchosen: {iec(p['chunk_size'])} chunks x {p['transfers']} transfers, "
        f"{iec(p['rate'])}/s (needs {iec(p['chunk_size'] * p['transfers'])} of buffers)"
    )
    typer.echo(f"limit:  {p['limit']}: {p['detail']}")
    typer.echo(f"chunks: one stream ran {p['chunk_gain']:.2f}x faster at the chosen chunk size "
               "than at the smallest")  # fmt: skip
    if p["noisy"]:
        typer.echo("noisy:  repeats of a probe differed by more than 20%; worth re-running")
    typer.echo(f"ran:    {when} via {p['unit']} on {p['key']}, rclone {p['rclone']}, "
               f"{iec(p['bytes_used'])} uploaded")  # fmt: skip


def _applied(rows: list[dict[str, Any]]) -> None:
    if not rows:
        typer.echo("applied: transfers use the profile from their next run (no drive units "
                   "without their own chunk_size)")  # fmt: skip
    for r in rows:
        how = (
            f"live (was {r['was']})"
            if r["live"]
            else f"not live: {r['error']}; restarting"
            if "error" in r
            else "from its next start"
        )
        typer.echo(f"applied {iec(r['chunk_size'])} chunks to {r['unit']}: {how}")  # fmt: skip


@app.command()
def tune(
    ctx: typer.Context,
    unit: Annotated[str | None, typer.Option(help="Drive unit whose drive and key to use")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show the probe plan only")] = False,
    show: Annotated[bool, typer.Option("--show", help="Show the profiles and settings")] = False,
    apply: Annotated[
        bool | None,
        typer.Option("--apply/--suggest", help="Put the result in effect (default: tuning.apply)"),
    ] = None,
    use_latest: Annotated[
        bool, typer.Option("--use-latest", help="Apply the latest profile without probing")
    ] = False,
    wait: Annotated[bool, typer.Option(help="Wait for the result")] = True,
) -> None:
    """Measure Drive upload throughput and pick the chunk size and transfers.

    Probes upload random data (counted against the key's daily cap) into a
    scratch folder on the unit's drive, which is trashed afterwards. Units'
    and transfers' own chunk_size / transfers settings always win."""
    if use_latest:
        r = ctx.obj.request("POST", "/v1/tune/apply")
        ctx.obj.show(r, lambda d: _applied(d["applied"]))
        return
    if show:
        st = ctx.obj.request("GET", "/v1/tune")

        def render_status(s: dict[str, Any]) -> None:
            if s["running"]:
                typer.echo(f"tune running: {s['progress'] or 'starting'}\n")
            if s["last"] and not s["last"]["ok"]:
                typer.echo(f"last tune failed: {s['last']['error']}\n")
            if s["profile"]:
                _profile(s["profile"])
            elif not s["running"]:
                typer.echo("no tuning profile yet (run `ferryman tune`)")
            active, latest = s["active"], s["profile"]
            if latest and active and active["at"] == latest["at"]:
                typer.echo("in effect: this profile")
            elif active:
                when = time.strftime("%F %T", time.localtime(active["at"]))
                was = f"{iec(active['chunk_size'])} chunks x {active['transfers']}"
                typer.echo(f"in effect: the profile from {when} ({was}); `--use-latest` to switch")
            elif latest:
                typer.echo("in effect: built-in defaults; `--use-latest` to apply this profile")
            typer.echo("\nUNIT                     CHUNK     FROM")
            for name, u in s["units"].items():
                size = u["chunk_size"] if isinstance(u["chunk_size"], str) else iec(u["chunk_size"])
                typer.echo(f"{name:24} {size:>6}     {u['source']}")
            typer.echo("\nTRANSFER                 XFERS  CHECKERS  FROM")
            for name, t in s["transfers"].items():
                typer.echo(f"{name:24} {t['transfers']:>5}  {t['checkers']:>8}  {t['source']}")

        ctx.obj.show(st, render_status)
        return
    body = {"unit": unit, "dry_run": dry_run, "wait": wait, "apply": apply}
    if not dry_run and wait and not ctx.obj.as_json:
        typer.echo("tuning; this takes a few minutes (`ferryman tune --show` for progress)",
                   err=True)  # fmt: skip
    result = ctx.obj.request("POST", "/v1/tune", json=body)

    def render(r: dict[str, Any]) -> None:
        if r.get("dry_run"):
            p = r["plan"]
            typer.echo(f"via unit {p['unit']} on {p['key']}")
            typer.echo(f"chunk sizes: {', '.join(iec(c) for c in p['chunk_sizes'])}")
            typer.echo(f"transfers:   {', '.join(map(str, p['transfers']))}")
            typer.echo(
                f"at most {p['max_probe_runs']} probe runs ({p['repeats']} per setting), "
                f"{iec(p['max_bytes'])} uploaded, {p['max_seconds'] / 60:.0f} min"
            )
        elif r.get("started"):
            typer.echo("tune started")
        elif r["ok"]:
            _profile(r["profile"])
            if r["applied"]:
                typer.echo("")
                _applied(r["applied"])
            else:
                typer.echo("\nnot applied (suggest mode); `ferryman tune --use-latest` applies it")
        else:
            _die(f"tune failed: {r['error']}")

    ctx.obj.show(result, render)


@app.command()
def health(ctx: typer.Context) -> None:
    """Run the health check now."""
    r = ctx.obj.request("POST", "/v1/health")

    def render(d: dict[str, str]) -> None:
        for unit, state in d.items():
            typer.echo(f"{unit:24} {state}")

    ctx.obj.show(r, render)


@app.command()
def events(ctx: typer.Context, limit: Annotated[int, typer.Option(help="How many")] = 50) -> None:
    """Recent events: failures, recoveries, rotations (newest last)."""

    def render(rows: list[dict[str, Any]]) -> None:
        if not rows:
            typer.echo("(no events)")
        for e in rows:
            when = time.strftime("%F %T", time.localtime(e["at"]))
            typer.echo(f"{when}  {e['severity'].upper():7}  {e['kind']} {e['subject']}: "
                       f"{e['message']}")  # fmt: skip

    ctx.obj.show(ctx.obj.request("GET", "/v1/events", params={"limit": limit}), render)


@app.command()
def coordinator(
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Coordinator config file")
    ] = DEFAULT_COORDINATOR_CONFIG,
    log_level: Annotated[str, typer.Option(help="Python log level")] = "INFO",
) -> None:
    """Run the coordinator (plain HTTP; put Traefik in front for TLS)."""
    from .config import CoordinatorConfig
    from .coordinator import run

    logging.basicConfig(level=log_level.upper(), stream=sys.stderr,
                        format="%(levelname)s %(name)s: %(message)s")  # fmt: skip
    try:
        cfg = CoordinatorConfig.load(config)
    except FileNotFoundError:
        _die(f"config not found: {config}")
    except ValueError as e:
        _die(f"invalid config {config}:\n{e}")
    run(cfg)


@app.command()
def token() -> None:
    """Make an agent token: the token for the agent's token_file, and its
    sha256 for the coordinator's config."""
    import secrets

    from .coordinator import token_hash

    t = secrets.token_urlsafe(32)
    typer.echo(f"token:        {t}")
    typer.echo(f"token_sha256: {token_hash(t)}")


def main() -> None:
    app()
