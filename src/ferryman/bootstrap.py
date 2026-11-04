"""Starting an agent: its own file (AgentBootstrap) plus its slice of the
controller's config.

The agent asks the controller for its slice. If the controller can't be
reached, the agent starts from the slice it last applied, cached in its
state directory, so a restart during an outage still brings its mounts
back (the leased key is cached beside it, see ControllerPool). With neither,
it waits for the controller.

A slice is checked on the host before it is used: placeholders, paths
(system trees, ferryman's own, nesting, transfer sources, secret files) and
ports against the agent's own API. One that fails is rejected and the agent
keeps the previous one.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import AgentBootstrap, Config, Layout, effective_config
from .controller_pool import ControllerClient, ControllerError

log = logging.getLogger(__name__)

FETCH_TIMEOUT = 10.0
RETRY_MIN, RETRY_MAX = 5.0, 60.0


class Rejected(ValueError):
    """A slice this host can't run."""


def slice_file(b: AgentBootstrap) -> Path:
    return b.paths.state / "controller" / "slice.json"


def load_cached(b: AgentBootstrap) -> dict[str, Any] | None:
    try:
        data: dict[str, Any] = json.loads(slice_file(b).read_text())
        return data
    except (FileNotFoundError, ValueError):
        return None


def save_cached(b: AgentBootstrap, slc: dict[str, Any]) -> None:
    path = slice_file(b)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(slc, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def build(b: AgentBootstrap, slc: dict[str, Any]) -> Config:
    """The flat config the agent runs, after the checks only the host can make."""
    try:
        cfg = effective_config(b, slc)
    except ValueError as e:  # a pydantic ValidationError is one too
        raise Rejected(str(e)) from None
    if problems := Layout(cfg).check_paths():
        raise Rejected("; ".join(problems))
    return cfg


async def fetch(client: ControllerClient, version: str | None = None,
                timeout: float = FETCH_TIMEOUT) -> dict[str, Any]:  # fmt: skip
    params = {"version": version} if version else {}
    body: dict[str, Any] = await client.request("GET", "/v2/config", params=params,
                                                timeout=timeout)  # fmt: skip
    return body


@dataclass
class Start:
    cfg: Config
    slice: dict[str, Any]
    source: str  # "controller", or "cache" when the controller couldn't be used
    rejected: str | None = None  # why the controller's newer slice wasn't used


async def start_config(b: AgentBootstrap, client: ControllerClient,
                       retry: float = RETRY_MIN) -> Start:  # fmt: skip
    """The config to start with. Raises Rejected only when the controller's
    slice fails here and there is no previous one to fall back on."""
    while True:
        try:
            slc = await fetch(client)
        except ControllerError as e:
            if (cached := load_cached(b)) is not None:
                log.warning("%s: starting from the config cached at %s (version %s)",
                            e, slice_file(b), cached.get("version"))  # fmt: skip
                return Start(build(b, cached), cached, "cache")
            log.warning("%s, and no cached config: retrying in %.0fs", e, retry)
            await asyncio.sleep(retry)
            retry = min(retry * 2, RETRY_MAX)
            continue
        try:
            cfg = build(b, slc)
        except Rejected as e:
            if (cached := load_cached(b)) is None:
                raise
            log.error("config version %s rejected, starting from the cached one: %s",
                      slc.get("version"), e)  # fmt: skip
            return Start(build(b, cached), cached, "cache", rejected=str(e))
        save_cached(b, slc)
        return Start(cfg, slc, "controller")
