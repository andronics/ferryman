"""Name selection for CLI/API commands: globs (`media-*`), comma lists
(`media-tv,media-movies`), exact names, or empty for everything."""

from __future__ import annotations

from collections.abc import Iterable
from fnmatch import fnmatchcase


def match(pattern: str | None, names: Iterable[str]) -> list[str]:
    names = list(names)
    if not pattern:
        return names
    globs = [p.strip() for p in pattern.split(",") if p.strip()]
    return [n for n in names if any(fnmatchcase(n, g) for g in globs)]
