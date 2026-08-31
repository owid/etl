"""The chart's own configuration, which the public endpoints do not expose.

A large share of what gets reported in ``#we-need-to-correct-it`` is configuration rather than
data: a manual ``maxTime`` left behind so the chart stops a year before the data does, an empty
default entity selection, a tab that renders nothing. None of that is in ``.metadata.json``, and
there is no public config endpoint — so it comes from ``chart_configs`` in the grapher database.

Optional by design: without a database the critic still reviews the render, the metadata and the
values, and simply says nothing about configuration.
"""

from __future__ import annotations

import json
from typing import Any

# Config keys worth showing the model. Everything else is styling.
KEYS = (
    "tab",
    "chartTypes",
    "minTime",
    "maxTime",
    "timelineMinTime",
    "timelineMaxTime",
    "stackMode",
    "hasMapTab",
    "yAxis",
    "xAxis",
    "title",
    "subtitle",
    "note",
)


def fetch(slug: str) -> dict[str, Any] | None:
    """The live config for a slug, or None if the database is unreachable or the slug is unknown."""
    try:
        from etl.db import read_sql

        df = read_sql(
            """
            SELECT cc.config
            FROM chart_configs cc
            JOIN charts c ON c.configId = cc.id
            WHERE cc.slug = %(slug)s
            LIMIT 1
            """,
            params={"slug": slug},
        )
    except Exception:  # noqa: BLE001 — no database is a normal way to run this
        return None
    if df.empty:
        return None
    raw = df.config.iloc[0]
    return json.loads(raw) if isinstance(raw, str) else raw


def summarize(config: dict[str, Any]) -> list[str]:
    """Describe the configuration in the same flat style as the rest of the bundle."""
    lines = ["\nchart configuration (what this chart is set to show):"]
    for key in KEYS:
        value = config.get(key)
        if value in (None, "", [], {}):
            continue
        if key in ("title", "subtitle", "note"):
            # These are chart-level overrides of the indicator's own text, which is worth
            # distinguishing: an override is edited by hand and can drift from the data.
            lines.append(f"  {key} (set on the chart, overriding the indicator): {value}")
        else:
            lines.append(f"  {key}: {json.dumps(value)}")
    selected = config.get("selectedEntityNames")
    if isinstance(selected, list):
        shown = ", ".join(selected[:8]) + (f", … ({len(selected)} total)" if len(selected) > 8 else "")
        lines.append(
            f"  default entities shown: {shown if selected else 'NONE — the chart opens with nothing selected'}"
        )
    return lines
