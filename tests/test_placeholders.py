"""Placeholders in transfer sources and dests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest

from ferryman import placeholders as ph
from ferryman.config import Config, Layout

VALUES = {"hostname": "pve-01", "transfer": "dump", "unit": "backups"}
SUNDAY = datetime(2026, 10, 25, 3, 30)


def test_expand() -> None:
    assert ph.expand("b/{hostname}/{unit}/{transfer}", VALUES) == "b/pve-01/backups/dump"
    assert ph.expand("{date}/{weekday}/{now:%Y/%m}", VALUES, SUNDAY) == "2026-10-25/sun/2026/10"
    assert ph.expand("{now:%%Y-%a}", VALUES, SUNDAY) == "%Y-Sun"


def test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NODE_NAME", "talos-w2")
    assert ph.expand("k8s/{env:NODE_NAME}", VALUES) == "k8s/talos-w2"
    monkeypatch.delenv("NODE_NAME")
    with pytest.raises(ph.PlaceholderError, match=r"\$NODE_NAME is not set"):
        ph.expand("k8s/{env:NODE_NAME}", VALUES)


@pytest.mark.parametrize(
    ("template", "error"),
    [
        ("{host}", "not a placeholder"),
        ("{0}", "not a placeholder"),
        ("{hostname.upper}", "not a placeholder"),
        ("{hostname!r}", "conversions"),
        ("{hostname:>10}", "takes no format"),
        ("{env}", "variable name"),
        ("{env:1BAD}", "variable name"),
        ("{now}", "strftime|FORMAT"),
        ("{now:{date}}", "nested"),
        ("{hostname", "expected '}'"),
    ],
)
def test_bad_templates(template: str, error: str) -> None:
    with pytest.raises(ph.PlaceholderError, match=error):
        ph.parse(template, ph.FIXED | ph.TIME)


def test_time_is_dest_only() -> None:
    with pytest.raises(ph.PlaceholderError, match="not a placeholder here"):
        ph.parse("/srv/{date}", ph.FIXED)


def test_repeats_weekly() -> None:
    assert ph.repeats_weekly("dumps/{hostname}")
    assert ph.repeats_weekly("dumps/{weekday}")
    assert ph.repeats_weekly("dumps/{now:%a}-{now:%u}/x%%y")
    assert not ph.repeats_weekly("dumps/{date}")
    assert not ph.repeats_weekly("dumps/{now:%a-%H}")
    assert not ph.repeats_weekly("dumps/{now:%Y}")


def test_fixed_prefix() -> None:
    assert ph.fixed_prefix("dumps/{hostname}", VALUES) == "dumps/pve-01"
    assert ph.fixed_prefix("dumps/{hostname}/{weekday}", VALUES) == "dumps/pve-01"
    # A time placeholder mid-name claims the whole directory it sits in.
    assert ph.fixed_prefix("dumps/{hostname}/day-{weekday}/x", VALUES) == "dumps/pve-01"
    assert ph.fixed_prefix("{date}", VALUES) == ""


def test_config_rules(make_config: Callable[..., Config], tmp_path: Path) -> None:
    def transfer(**t: object) -> Config:
        return make_config(hostname="pve-01", transfers={"dump": {"unit": "media-tv", **t}})

    cfg = transfer(mode="sync", source="/srv/{hostname}/{transfer}", dest="d/{weekday}")
    layout = Layout(cfg)
    assert layout.transfer_source("dump") == Path("/srv/pve-01/dump")
    assert layout.transfer_dest("dump", SUNDAY) == "d/sun"
    assert layout.claim_dest("dump") == "d"
    assert Layout(transfer(source="{unit}")).transfer_source("dump") == tmp_path / "local/media-tv"

    transfer(mode="copy", dest="archive/{now:%Y/%m}")  # dated archives are fine for copy
    with pytest.raises(ValueError, match="full copy each time"):
        transfer(mode="sync", dest="d/{date}")
    with pytest.raises(ValueError, match="not a placeholder here"):
        transfer(source="/srv/{date}")


def test_check_placeholders_reports_unset_env(
    make_config: Callable[..., Config], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NODE_NAME", raising=False)
    cfg = make_config(transfers={"dump": {"unit": "media-tv", "dest": "{env:NODE_NAME}"}})
    assert Layout(cfg).check_placeholders() == [
        "transfer dump: {env:NODE_NAME}: $NODE_NAME is not set"
    ]
    monkeypatch.setenv("NODE_NAME", "talos-w2")
    assert Layout(cfg).check_placeholders() == []
