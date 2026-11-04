"""Placeholders in mount paths and in a transfer's `source` and `dest`.

Fixed for the agent's lifetime (mount paths take all but {transfer}):
  {hostname}    the agent's hostname (config `hostname:`, $FERRYMAN_HOSTNAME, or the system's)
  {transfer}    the transfer's name
  {unit}        its unit's name
  {env:NAME}    the environment variable NAME (in a pod: the node name, the namespace)

Per run, in the host's local time like the cron schedules (dest only):
  {now:FORMAT}  strftime, e.g. {now:%Y/%m}
  {date}        2026-10-25
  {weekday}     mon .. sun, whatever the locale

A `sync` mirrors its source into its dest, so a dest that changes every run
uploads a full copy each time and never removes the old ones. A sync may
therefore only use time placeholders that repeat weekly ({weekday}, or
{now:...} with only %a %A %u %w): `dumps/{weekday}` keeps seven rolling
mirrors with no pruning.
"""

from __future__ import annotations

import os
import re
import string
from collections.abc import Mapping
from datetime import datetime

FIXED = frozenset({"hostname", "transfer", "unit", "env"})
MOUNT = frozenset({"hostname", "unit", "env"})
TIME = frozenset({"now", "date", "weekday"})
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
# strftime directives whose value repeats every week.
WEEKLY_DIRECTIVES = frozenset({"%a", "%A", "%u", "%w"})
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

Part = tuple[str, str | None, str]  # literal text, placeholder name, format spec


class PlaceholderError(ValueError):
    pass


def parse(template: str, allowed: frozenset[str]) -> list[Part]:
    """Split a template into literal text and placeholders, checking each."""
    try:
        raw = list(string.Formatter().parse(template))
    except ValueError as e:
        raise PlaceholderError(f"{template!r}: {e}") from None
    parts: list[Part] = []
    for literal, name, spec, conversion in raw:
        if name is None:
            parts.append((literal, None, ""))
            continue
        spec = spec or ""
        where = f"{template!r}: {{{name}{':' + spec if spec else ''}}}"
        if name not in allowed:
            names = ", ".join(f"{{{n}}}" for n in sorted(allowed))
            raise PlaceholderError(f"{where} is not a placeholder here (allowed: {names})")
        if conversion or "{" in spec:
            raise PlaceholderError(f"{where}: conversions and nested fields are not supported")
        if name == "env" and not _ENV_NAME.match(spec):
            raise PlaceholderError(f"{where}: write {{env:NAME}} with a variable name")
        if name == "now" and not spec:
            raise PlaceholderError(f"{where}: write {{now:FORMAT}}, e.g. {{now:%Y-%m}}")
        if name not in ("env", "now") and spec:
            raise PlaceholderError(f"{where}: {{{name}}} takes no format")
        parts.append((literal, name, spec))
    return parts


def has_time(template: str) -> bool:
    return any(name in TIME for _, name, _ in parse(template, FIXED | TIME))


def repeats_weekly(template: str) -> bool:
    """Whether every time placeholder in `template` comes round again each week."""
    for _, name, spec in parse(template, FIXED | TIME):
        if name == "date":
            return False
        if name == "now":
            directives = set(re.findall(r"%.", spec.replace("%%", "")))
            if not directives <= WEEKLY_DIRECTIVES:
                return False
    return True


def _value(name: str, spec: str, values: Mapping[str, str], now: datetime | None) -> str:
    if name == "env":
        try:
            return os.environ[spec]
        except KeyError:
            raise PlaceholderError(f"{{env:{spec}}}: ${spec} is not set") from None
    if name in TIME:
        now = now or datetime.now().astimezone()
        if name == "now":
            return now.strftime(spec)
        if name == "date":
            return now.strftime("%Y-%m-%d")
        return WEEKDAYS[now.weekday()]
    return values[name]


def expand(template: str, values: Mapping[str, str], now: datetime | None = None) -> str:
    return "".join(
        literal + (_value(name, spec, values, now) if name else "")
        for literal, name, spec in parse(template, FIXED | TIME)
    )


def fixed_prefix(template: str, values: Mapping[str, str], stop: frozenset[str] = TIME) -> str:
    """The part of a dest that is the same on every run: everything before
    the first time placeholder, cut back to a whole directory. With none,
    the whole expansion. `stop` adds placeholders whose value isn't known
    here (the controller can't see an agent's environment: TIME | {"env"})."""
    out = []
    for literal, name, spec in parse(template, FIXED | TIME):
        out.append(literal)
        if name in stop:
            head = "".join(out)
            return head[: head.rfind("/") + 1].rstrip("/")
        if name:
            out.append(_value(name, spec, values, None))
    return "".join(out)
