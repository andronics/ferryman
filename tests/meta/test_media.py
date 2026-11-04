"""Tests for meta/media, the media centre's module for `meta` (brain record 0019).

No network: every app URL points at a fake HTTP server on 127.0.0.1 serving hand-written fixtures,
and `pass`, `tessera` and gpg are fake scripts on a PATH that holds nothing else, so no test
ever reaches a real password store or a real key. A small fake core checks the manifest against
record 0019 §2 and runs each verb the way the core does (§3).
"""

from __future__ import annotations

import copy
import json
import os
import re
import stat
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "meta" / "media"
FIXTURES = json.loads((Path(__file__).parent / "fixtures" / "media.json").read_text())

APP_NAMES = [
    "bazarr",
    "lazylibrarian",
    "lidarr",
    "prowlarr",
    "qbittorrent",
    "radarr",
    "readarr",
    "sonarr",
]
# Fake credentials: secret-looking on purpose, so a leak is easy to spot.
FAKE_KEY = "fakeapikey0123456789abcdef0123ab"
FAKE_USER = "fake-webui-user"
FAKE_PASSWORD = "Fake-Pa55word-zq81"
FAKE_COOKIE = "fakesessioncookie0123456789"
SECRETS = [FAKE_KEY, FAKE_PASSWORD, FAKE_COOKIE]
# What a queue must never show: hashes, paths, tracker URLs, passkeys.
FORBIDDEN_IN_QUEUE = [
    "0123456789abcdef0123456789abcdef",
    "/pool/",
    "tracker.",
    "PASSKEY",
    "ABCDEF0123",
]
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
ANSI = re.compile(r"\x1b\[")


# ------------------------------------------------------------------ fake apps


@dataclass
class FakeApps:
    fixtures: dict[str, Any]
    requests: list[tuple[str, str, dict[str, str], str]] = field(default_factory=list)
    redirect: set[str] = field(default_factory=set)
    slow: set[str] = field(default_factory=set)
    html: set[str] = field(default_factory=set)


def make_handler(state: FakeApps) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def _send(self, code: int, body: Any, headers: dict[str, str] | None = None) -> None:
            raw = body if isinstance(body, bytes) else json.dumps(body).encode()
            if isinstance(body, str):
                raw = body.encode()
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _handle(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode() if length else ""
            url = urllib.parse.urlsplit(self.path)
            app, _, rest = url.path.lstrip("/").partition("/")
            state.requests.append((method, self.path, dict(self.headers), body))
            if app in state.slow:
                time.sleep(30)
                return
            if app in state.redirect:
                self._send(302, b"", {"Location": "https://auth.example.test/authorize?x=1"})
                return
            if app in state.html:
                self._send(200, b"<html>not the app</html>", {"Content-Type": "text/html"})
                return
            key = f"{app}/{rest}"
            if app == "qbittorrent":
                if rest == "api/v2/auth/login":
                    form = urllib.parse.parse_qs(body)
                    if form.get("username") == [FAKE_USER] and form.get("password") == [
                        FAKE_PASSWORD
                    ]:
                        cookie = f"SID={FAKE_COOKIE}; HttpOnly; path=/"
                        self._send(200, b"Ok.", {"Set-Cookie": cookie})
                    else:
                        self._send(200, b"Fails.")
                    return
                if self.headers.get("Cookie") != f"SID={FAKE_COOKIE}":
                    self._send(403, b"Forbidden")
                    return
            elif app == "lazylibrarian":
                form = urllib.parse.parse_qs(body)
                if form.get("apikey") != [FAKE_KEY]:
                    self._send(200, {"Success": False, "Error": "Invalid key"})
                    return
                key = f"lazylibrarian/api:{form.get('cmd', [''])[0]}"
            elif self.headers.get("X-Api-Key") != FAKE_KEY:
                self._send(401, {"error": "Unauthorized"})
                return
            if key not in state.fixtures:
                self._send(404, {"error": "not found"})
                return
            self._send(200, state.fixtures[key])

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

    return Handler


@pytest.fixture
def apps() -> Iterator[tuple[FakeApps, str]]:
    state = FakeApps(copy.deepcopy(FIXTURES))
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(state))
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def healthy(state: FakeApps) -> None:
    state.fixtures["sonarr/api/v3/health"] = []
    for t in state.fixtures["qbittorrent/api/v2/torrents/info"]:
        if t["state"] == "stalledDL":
            t["state"] = "downloading"


# ------------------------------------------------------------------ fake pass and running


def _script(bin_dir: Path, name: str, lines: list[str]) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    script = bin_dir / name
    script.write_text("\n".join(["#!/bin/sh", *lines]) + "\n")
    script.chmod(0o755)


def _entries(with_credentials: bool, missing: str) -> list[str]:
    """A shell `case` on "$2" printing each fake entry; `missing` runs for any other path."""
    lines = ['case "$2" in']
    if with_credentials:
        lines += [
            f"  services/qbittorrent/webui-username) echo '{FAKE_USER}' ;;",
            f"  services/qbittorrent/webui-password) echo '{FAKE_PASSWORD}' ;;",
            f"  services/*/api-key) echo '{FAKE_KEY}' ;;",
        ]
    return [*lines, f"  *) {missing} ;;", "esac"]


def fake_pass(bin_dir: Path, with_credentials: bool = True, gpg_fails: bool = False) -> None:
    """pass, logging each call with the gpg options it was given. gpg_fails: gpg refuses to
    decrypt (as it does with --pinentry-mode=error while the key is locked)."""
    lines = [
        'echo "pass $* opts=$PASSWORD_STORE_GPG_OPTS" >> "$FAKE_LOG"',
        '[ "$1" = show ] || exit 2',
    ]
    if gpg_fails:
        lines += ['echo "gpg: decryption failed: No secret key" >&2', "exit 2"]
    lines += _entries(
        with_credentials, 'echo "Error: $2 is not in the password store." >&2; exit 1'
    )
    _script(bin_dir, "pass", lines)


def fake_tessera(bin_dir: Path, lock: str, with_credentials: bool = True) -> None:
    """tessera: `status --json` reports `lock` and exits 1, as the real one does with a warning;
    `show` refuses with exit 1 (queued) while locked, 5 for no such entry. Every call logged."""
    lines = [
        'echo "tessera $*" >> "$FAKE_LOG"',
        'case "$1" in',
        f'  status) echo \'{{"state": "warn", "lock": "{lock}"}}\'; exit 1 ;;',
        "  show) ;;",
        "  *) exit 2 ;;",
        "esac",
    ]
    if lock == "locked":
        lines += ['echo "tessera: the store is locked; queued as r1" >&2', "exit 1"]
    lines += _entries(with_credentials, 'echo "tessera: no such entry" >&2; exit 5')
    _script(bin_dir, "tessera", lines)


FAKE_GRIP = "0123456789ABCDEF0123456789ABCDEF01234567"


def fake_gpg(bin_dir: Path, store: Path, cached: bool) -> None:
    """gpg and gpg-connect-agent for the check without tessera: one key with an encryption
    subkey, and an agent whose KEYINFO says whether it holds the passphrase."""
    store.mkdir(parents=True, exist_ok=True)
    (store / ".gpg-id").write_text("fake@example.test\n")
    _script(
        bin_dir,
        "gpg",
        [
            'echo "gpg $*" >> "$FAKE_LOG"',
            "echo 'sec:u:255:22:AAAAAAAAAAAAAAAA:1:::u:::scESC:::+:::23::0:'",
            "echo 'grp:::::::::BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB:'",
            "echo 'ssb:u:255:18:CCCCCCCCCCCCCCCC:1::::::e:::+:::23:'",
            f"echo 'grp:::::::::{FAKE_GRIP}:'",
        ],
    )
    flag = "1" if cached else "-"
    _script(
        bin_dir,
        "gpg-connect-agent",
        [
            'echo "gpg-connect-agent $*" >> "$FAKE_LOG"',
            f"echo 'S KEYINFO {FAKE_GRIP} D - - {flag} P - - -'",
            "echo OK",
        ],
    )


@dataclass
class Run:
    code: int
    stdout: str
    stderr: str
    seconds: float


def run(
    tmp_path: Path,
    base: str | None,
    *args: str,
    with_credentials: bool = True,
    env_extra: dict[str, str] | None = None,
    timeout: float = 40,
    tessera: str | None = None,
    gpg_cached: bool | None = None,
    decrypt_fails: bool = False,
) -> Run:
    """Run the module as the core would: exec-style, stdin closed, the META_* environment set.

    PATH holds only the fakes: `pass` always; `tessera` reporting lock state `tessera` when given;
    gpg with an agent holding the passphrase or not when `gpg_cached` is given. Without gpg the
    lock state is unknown and the read goes on; `decrypt_fails` makes pass's gpg refuse.
    Every fake logs its calls to tmp_path/calls.log."""
    bin_dir = tmp_path / "bin"
    if bin_dir.exists():
        for old in bin_dir.iterdir():
            old.unlink()
    fake_pass(bin_dir, with_credentials, gpg_fails=decrypt_fails or gpg_cached is False)
    if tessera is not None:
        fake_tessera(bin_dir, tessera, with_credentials)
    store = tmp_path / "store"
    if gpg_cached is not None:
        fake_gpg(bin_dir, store, gpg_cached)
    env = {
        "PATH": str(bin_dir),
        "HOME": str(tmp_path),
        "PASSWORD_STORE_DIR": str(store),
        "FAKE_LOG": str(tmp_path / "calls.log"),
        "META": "1",
        "META_CENTRE": "media",
        "META_VERB": args[0] if args and not args[0].startswith("-") else "",
        "META_TIMEOUT": "20",
        "META_CACHE_DIR": str(tmp_path / "cache"),
    }
    if "--json" in args:
        env["META_JSON"] = "1"
    for name in APP_NAMES:
        # Never a real name: a closed local port when no fake server is given.
        env[f"MEDIA_META_{name.upper()}_URL"] = f"{base}/{name}" if base else "http://127.0.0.1:9"
    env.update(env_extra or {})
    start = time.monotonic()
    proc = subprocess.run(
        [sys.executable, str(MODULE), *args],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        check=False,
    )
    return Run(proc.returncode, proc.stdout, proc.stderr, time.monotonic() - start)


def assert_plain(r: Run) -> None:
    for line in r.stdout.splitlines() + r.stderr.splitlines():
        assert len(line) <= 100, line
        assert not ANSI.search(line), line
    for line in r.stderr.splitlines():
        if not line.startswith("usage:"):
            assert line.startswith("meta media:"), line


def assert_snake(value: Any) -> None:
    if isinstance(value, dict):
        for k, v in value.items():
            assert SNAKE.match(k), k
            assert_snake(v)
    elif isinstance(value, list):
        for v in value:
            assert_snake(v)


def assert_no_secrets(r: Run) -> None:
    for secret in SECRETS:
        assert secret not in r.stdout
        assert secret not in r.stderr


def one_json(r: Run) -> Any:
    value = json.loads(r.stdout)  # exactly one value: json.loads rejects trailing data
    assert_snake(value)
    return value


# ------------------------------------------------------------------ the fake core: manifest


def validate_manifest(m: Any) -> list[str]:
    """Record 0019 §2's rules for a manifest, as the core will apply them."""
    errors: list[str] = []
    if not isinstance(m, dict):
        return ["not an object"]
    if m.get("meta") != 1:
        errors.append("meta is not 1")
    for key in ("centre", "owner", "summary"):
        if not isinstance(m.get(key), str) or not m[key]:
            errors.append(f"{key} missing")
    verbs = m.get("verbs")
    if not isinstance(verbs, dict) or "status" not in verbs:
        return [*errors, "verbs missing or without status"]
    results = m.get("results", [])
    if not isinstance(results, list) or not all(
        isinstance(r, int) and not isinstance(r, bool) and r in (1, 5) for r in results
    ):
        errors.append("results must be exit codes that are answers (1, 5)")
    for name, v in verbs.items():
        if not isinstance(v, dict) or not isinstance(v.get("summary"), str):
            errors.append(f"{name}: no summary")
            continue
        cache = v.get("cache", 0)
        if not isinstance(cache, int) or isinstance(cache, bool) or cache < 0:
            errors.append(f"{name}: cache not a non-negative int")
        if v.get("writes") or v.get("interactive"):
            errors.append(f"{name}: writes/interactive not allowed for this module")
        args = v.get("args", [])
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            errors.append(f"{name}: args not a list of names")
        complete = v.get("complete")
        if complete is not None:
            if isinstance(complete, str):
                verb, _, arg = complete.partition(" ")
                if verb not in verbs or not arg:
                    errors.append(f"{name}: complete names no verb of this module")
            elif not (isinstance(complete, list) and all(isinstance(c, str) for c in complete)):
                errors.append(f"{name}: complete not a list or a verb")
    shapes = {"status": [], "list": ["what"]}
    for name, shape in shapes.items():
        if name in verbs and verbs[name].get("args", []) != shape:
            errors.append(f"{name}: shared verb args must be {shape}")
    if "show" in verbs and len(verbs["show"].get("args", [])) != 1:
        errors.append("show: shared verb takes one thing")
    return errors


def test_manifest_is_valid(tmp_path: Path) -> None:
    r = run(tmp_path, None, "--meta-manifest")
    assert r.code == 0
    m = json.loads(r.stdout)
    assert validate_manifest(m) == []
    assert m["centre"] == "media"
    assert m["owner"] == "media-coordinator"
    assert m["results"] == [1, 5]
    assert {k: v.get("cache") for k, v in m["verbs"].items()} == {
        "status": 60,
        "list": 900,
        "show": 60,
        "queue": 30,
    }
    assert m["verbs"]["list"]["complete"] == ["apps"]
    assert m["verbs"]["show"]["complete"] == "list apps"


def test_manifest_validator_catches_planted_defects() -> None:
    good = json.loads(
        subprocess.run(
            [sys.executable, str(MODULE), "--meta-manifest"], capture_output=True, check=True
        ).stdout
    )
    planted: dict[str, Callable[[dict[str, Any]], Any]] = {
        "meta is not 1": lambda m: m.update(meta=2),
        "writes/interactive": lambda m: m["verbs"]["queue"].update(writes=True),
        "cache not": lambda m: m["verbs"]["status"].update(cache="60"),
        "shared verb args": lambda m: m["verbs"]["list"].update(args=["kind"]),
        "complete names no verb": lambda m: m["verbs"]["show"].update(complete="nosuch apps"),
    }
    for message, plant in planted.items():
        m = copy.deepcopy(good)
        plant(m)
        assert any(message in e for e in validate_manifest(m)), message


def test_module_is_executable_stdlib_only() -> None:
    assert MODULE.read_text().startswith("#!/usr/bin/env python3\n")
    assert MODULE.stat().st_mode & 0o777 == 0o755
    assert os.access(MODULE, os.X_OK)
    stdlib = sys.stdlib_module_names | {"__future__"}
    for line in MODULE.read_text().splitlines():
        m = re.match(r"^(?:from|import)\s+([A-Za-z_][\w]*)", line)
        if m:
            assert m.group(1) in stdlib, line


# ------------------------------------------------------------------ verbs


def test_status_plain_warns(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "status")
    assert r.code == 1, r.stderr
    assert_plain(r)
    lines = r.stdout.splitlines()
    assert lines[0].startswith("warn: 8 of 8 apps reachable")
    by_name = {line.split()[1]: line for line in lines[1:]}
    assert sorted(by_name) == APP_NAMES
    assert by_name["sonarr"].startswith("warn")
    assert "queue 2" in by_name["sonarr"] and "1 health warning" in by_name["sonarr"]
    assert "1 stalled" in by_name["qbittorrent"] and "down 1.2 MB/s" in by_name["qbittorrent"]
    assert "last search unknown" in by_name["lazylibrarian"]
    assert by_name["radarr"].startswith("ok") and "queue 1" in by_name["radarr"]


def test_status_all_ok_exits_0(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    healthy(state)
    r = run(tmp_path, base, "status")
    assert r.code == 0, r.stdout
    assert r.stdout.startswith("ok: 8 of 8 apps reachable")


def test_status_json(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "status", "--json")
    assert r.code == 1
    data = one_json(r)
    assert set(data) == {"summary", "state", "checks"}
    assert data["state"] == "warn"
    assert {c["name"] for c in data["checks"]} == set(APP_NAMES)
    for c in data["checks"]:
        assert set(c) >= {"name", "state", "detail"}
        assert c["state"] in ("ok", "warn", "fail", "unknown")


def test_status_json_from_env(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "status", env_extra={"META_JSON": "1"})
    assert one_json(r)["state"] == "warn"


def test_status_one_dead_app_is_a_fail(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    healthy(state)
    state.redirect.add("radarr")
    state.html.add("bazarr")
    r = run(tmp_path, base, "status", env_extra={"MEDIA_META_LIDARR_URL": "http://127.0.0.1:9"})
    assert r.code == 1
    assert_plain(r)
    by_name = {line.split()[1]: line for line in r.stdout.splitlines()[1:]}
    assert (
        by_name["radarr"].startswith("fail") and "sign-in at auth.example.test" in by_name["radarr"]
    )
    assert by_name["lidarr"].startswith("fail") and "connection refused" in by_name["lidarr"]
    assert by_name["bazarr"].startswith("fail") and "not with the app's API" in by_name["bazarr"]
    assert by_name["sonarr"].startswith("ok")


def test_status_slow_app_cannot_eat_the_budget(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    state.slow.add("sonarr")
    r = run(tmp_path, base, "status", env_extra={"META_TIMEOUT": "4"})
    assert r.seconds < 8, r.seconds
    by_name = {line.split()[1]: line for line in r.stdout.splitlines()[1:]}
    assert by_name["sonarr"].startswith("fail")
    assert by_name["radarr"].startswith("ok")


def test_status_nothing_reachable_exits_4(tmp_path: Path) -> None:
    r = run(tmp_path, None, "status")
    assert r.code == 4
    assert r.stdout.startswith("fail: 0 of 8 apps reachable")
    assert "meta media: no media app answered" in r.stderr


def test_missing_credentials_are_unknown(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    r = run(tmp_path, base, "status", with_credentials=False)
    assert r.code == 1
    by_name = {line.split()[1]: line for line in r.stdout.splitlines()[1:]}
    assert by_name["sonarr"].startswith("unknown")
    assert "no credential at services/sonarr/api-key" in by_name["sonarr"]
    assert "no credential at services/qbittorrent/webui-username" in by_name["qbittorrent"]
    assert state.requests == []  # nothing is asked of an app without its credential


def test_off_skips_an_app(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    healthy(state)
    r = run(tmp_path, base, "status", env_extra={"MEDIA_META_READARR_URL": "off"})
    assert r.code == 0
    assert r.stdout.startswith("ok: 7 of 7 apps reachable")
    assert "readarr" not in r.stdout


def test_list_apps(tmp_path: Path) -> None:
    r = run(tmp_path, None, "list", "apps")
    assert r.code == 0
    assert r.stdout.splitlines() == APP_NAMES
    data = one_json(run(tmp_path, None, "list", "apps", "--json"))
    assert [d["name"] for d in data] == APP_NAMES


def test_list_other_is_usage(tmp_path: Path) -> None:
    r = run(tmp_path, None, "list", "services")
    assert r.code == 2
    assert_plain(r)


def test_show_app(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "show", "sonarr")
    assert r.code == 0
    assert_plain(r)
    kv = dict(line.split(": ", 1) for line in r.stdout.split("\n\n")[0].splitlines())
    assert kv["name"] == "sonarr"
    assert kv["url host"] == "127.0.0.1:" + base.rsplit(":", 1)[1]
    assert kv["version"] == "4.0.15.2941"
    assert kv["reachable"] == "yes"
    assert kv["queue"] == "2"
    assert kv["health warnings"] == "1"
    data = one_json(run(tmp_path, base, "show", "qbittorrent", "--json"))
    assert data["name"] == "qbittorrent" and data["reachable"] is True
    assert data["down_speed"] == 1_250_000 and data["stalled"] == 1


def test_show_unknown_app_is_5(tmp_path: Path) -> None:
    r = run(tmp_path, None, "show", "nosuchapp")
    assert r.code == 5
    assert_plain(r)


def test_show_unreachable_app_is_4(tmp_path: Path) -> None:
    r = run(tmp_path, None, "show", "radarr")
    assert r.code == 4
    assert "reachable: no" in r.stdout


def test_queue(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "queue")
    assert r.code == 0, r.stderr
    assert_plain(r)
    lines = r.stdout.splitlines()
    assert lines[0].split() == ["SOURCE", "DONE", "STATE", "SIZE", "NAME"]
    assert len(lines) == 1 + 2 + 3  # qbittorrent's 2 incomplete, radarr's 1, sonarr's 2
    for bad in FORBIDDEN_IN_QUEUE:
        assert bad not in r.stdout
    data = one_json(run(tmp_path, base, "queue", "--json"))
    assert isinstance(data, list) and len(data) == 5
    for item in data:
        assert set(item) == {"source", "name", "progress", "state", "size"}
        assert isinstance(item["size"], int) and 0 <= item["progress"] <= 1
    assert {"source": "radarr", "name": "Example Film 2024 2160p"}.items() <= data[2].items()
    for bad in FORBIDDEN_IN_QUEUE:
        assert bad not in json.dumps(data)


def test_queue_partial_and_none(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    state.redirect.add("sonarr")
    r = run(tmp_path, base, "queue")
    assert r.code == 1
    assert "meta media: sonarr: sent to sign-in" in r.stderr
    assert run(tmp_path, None, "queue").code == 4


def test_help(tmp_path: Path) -> None:
    r = run(tmp_path, None, "--help")
    assert r.code == 0 and "status" in r.stdout and "MEDIA_META_<APP>_URL" in r.stdout
    for verb in ("status", "list", "show", "queue"):
        r = run(tmp_path, None, verb, "--help")
        assert r.code == 0 and "usage: meta media " + verb in r.stdout
        assert_plain(r)


def test_unknown_verb_is_usage(tmp_path: Path) -> None:
    assert run(tmp_path, None, "shwo").code == 2
    assert run(tmp_path, None).code == 2


# ------------------------------------------------------------------ secrets


def test_no_credential_ever_printed(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    runs = [
        run(tmp_path, base, "status"),
        run(tmp_path, base, "status", "--json"),
        run(tmp_path, base, "show", "qbittorrent"),
        run(tmp_path, base, "show", "lazylibrarian", "--json"),
        run(tmp_path, base, "queue"),
        run(tmp_path, base, "queue", "--json"),
    ]
    state.redirect.update(APP_NAMES)
    runs += [run(tmp_path, base, "status"), run(tmp_path, base, "show", "sonarr")]
    for r in runs:
        assert_no_secrets(r)
    assert state.requests
    for _method, path, _headers, _body in state.requests:
        for secret in SECRETS:
            assert secret not in path  # never in a URL
        assert "apikey" not in path.lower()


def test_credentials_sent_by_header_or_body(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    run(tmp_path, base, "status")
    seen = {p.split("/")[1] for _m, p, _h, _b in state.requests}
    assert seen == set(APP_NAMES)
    for method, path, headers, body in state.requests:
        app = path.split("/")[1]
        if app == "qbittorrent" and path.endswith("/auth/login"):
            assert method == "POST" and FAKE_PASSWORD in body
        elif app == "lazylibrarian":
            assert method == "POST" and FAKE_KEY in body and "getVersion" in body
        elif app != "qbittorrent":
            assert method == "GET" and headers.get("X-Api-Key") == FAKE_KEY


SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|password|token|secret)\s*[=:]\s*['\"][^'\"\s]{8,}"),
    re.compile(r"\b[0-9a-f]{32}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]


def test_source_and_output_have_no_secret_patterns(
    apps: tuple[FakeApps, str], tmp_path: Path
) -> None:
    _, base = apps
    texts = [MODULE.read_text()]
    for args in (("status",), ("status", "--json"), ("queue", "--json"), ("show", "sonarr")):
        r = run(tmp_path, base, *args)
        texts += [r.stdout, r.stderr]
    for text in texts:
        for pattern in SECRET_PATTERNS:
            assert not pattern.search(text), pattern.pattern


def test_credentials_never_in_argv_or_env() -> None:
    source = MODULE.read_text()
    # One place runs programs; reads are only `tessera show <path>` or `pass show <path>`.
    assert re.findall(r"subprocess\.\w+\(", source) == ["subprocess.run("]
    assert '[tessera, "show", path]' in source
    assert '["pass", "show", path]' in source
    assert "os.environ[" not in source and "putenv" not in source
    assert stat.S_IMODE(MODULE.stat().st_mode) == 0o755


# ------------------------------------------------------------------ a locked key


def calls(tmp_path: Path) -> list[str]:
    log = tmp_path / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def reads(tmp_path: Path) -> list[str]:
    return [c for c in calls(tmp_path) if c.startswith(("pass show", "tessera show"))]


def assert_all_key_locked(r: Run) -> None:
    lines = r.stdout.splitlines()
    assert lines[0].startswith("unknown: key locked")
    assert len(lines) == 1 + len(APP_NAMES)
    for line in lines[1:]:
        state, name, detail = line.split(None, 2)
        assert (state, detail) == ("unknown", "key locked"), line
        assert name in APP_NAMES


def test_locked_key_reads_nothing(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    """tessera says locked: no `tessera show` or `pass show` at all, no app asked, every app
    "key locked" as unknown, and exit 1, a result (record 0019), never 4 or a failure."""
    state, base = apps
    r = run(tmp_path, base, "status", tessera="locked")
    assert r.code == 1, r.stderr
    assert_all_key_locked(r)
    assert "tessera status --json" in calls(tmp_path)
    assert reads(tmp_path) == []
    assert state.requests == []
    assert r.stderr == ""
    assert_plain(r)


def test_locked_key_json(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    _, base = apps
    r = run(tmp_path, base, "status", "--json", tessera="locked")
    assert r.code == 1
    doc = one_json(r)
    assert doc["state"] == "unknown"
    assert {c["name"] for c in doc["checks"]} == set(APP_NAMES)
    assert all(c["state"] == "unknown" and c["detail"] == "key locked" for c in doc["checks"])
    assert reads(tmp_path) == []


def test_locked_key_show_and_queue(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    r = run(tmp_path, base, "show", "sonarr", tessera="locked")
    assert r.code == 0
    assert "state: unknown" in r.stdout and "key locked" in r.stdout
    q = run(tmp_path, base, "queue", tessera="locked")
    assert q.code == 1  # a result: the queue is unknown, not a failure (4)
    assert q.stdout.startswith("key locked:")
    assert "meta media: sonarr: key locked" in q.stderr
    assert_plain(q)
    assert reads(tmp_path) == []
    assert state.requests == []


def test_locked_key_without_tessera_reads_nothing(
    apps: tuple[FakeApps, str], tmp_path: Path
) -> None:
    """No tessera: the module asks gpg-agent itself, never decrypting, and reads nothing."""
    state, base = apps
    r = run(tmp_path, base, "status", gpg_cached=False)
    assert r.code == 1
    assert_all_key_locked(r)
    assert any(c.startswith("gpg-connect-agent --no-autostart KEYINFO") for c in calls(tmp_path))
    assert reads(tmp_path) == []
    assert state.requests == []


def test_unlocked_key_reads_through_tessera(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    healthy(state)
    r = run(tmp_path, base, "status", tessera="unlocked")
    assert r.code == 0, r.stdout
    got = reads(tmp_path)
    assert got and all(c.startswith("tessera show services/") for c in got)
    assert_no_secrets(r)


def test_pass_fallback_never_prompts(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    """No tessera, lock state unlocked: pass runs with gpg told to fail rather than prompt."""
    state, base = apps
    healthy(state)
    r = run(tmp_path, base, "status", gpg_cached=True)
    assert r.code == 0, r.stdout
    got = reads(tmp_path)
    assert got
    assert all(c.endswith("opts=--pinentry-mode=error --batch") for c in got)


def test_refused_decrypt_is_key_locked(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    """The lock state can't be told (no gpg here) and gpg then refuses to decrypt: each app is
    "key locked" and unknown, not a failure, and no app is asked."""
    state, base = apps
    r = run(tmp_path, base, "status", decrypt_fails=True)
    assert r.code == 1
    assert_all_key_locked(r)
    assert reads(tmp_path)  # tried, since the state was unknown, and refused without a prompt
    assert state.requests == []


def test_tessera_missing_entry_is_no_credential(apps: tuple[FakeApps, str], tmp_path: Path) -> None:
    state, base = apps
    r = run(tmp_path, base, "status", tessera="unlocked", with_credentials=False)
    assert r.code == 1
    by_name = {line.split()[1]: line for line in r.stdout.splitlines()[1:]}
    assert by_name["sonarr"].startswith("unknown")
    assert "no credential at services/sonarr/api-key" in by_name["sonarr"]
    assert state.requests == []
