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
    "title",
    "subtitle",
    "note",
)


class ConfigUnavailable(Exception):
    """The config lookup itself failed, as opposed to the slug having no chart config.

    The two were indistinguishable, and the consequence was quiet: in ``--sample`` and
    ``--changed-since`` — which have a database by definition — a permissions problem or a schema
    change would remove the whole chart-config channel from every review while the run still
    reported success. That channel is what catches stale time bounds, empty entity selections and
    a broken default tab, so losing it silently loses a class of findings, not a detail.
    """


def fetch(slug: str) -> dict[str, Any] | None:
    """The live config for a slug, or None if the slug has no chart config.

    Raises:
        ConfigUnavailable: the lookup could not be performed at all.
    """
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
    except Exception as e:
        raise ConfigUnavailable(str(e)) from e
    if df.empty:
        return None
    raw = df.config.iloc[0]
    return json.loads(raw) if isinstance(raw, str) else raw


# Axis bounds are deliberately NOT shown to the model. `yAxis: {"max": 0, "min": 0}` is
# grapher's auto-scale default, not a zero ceiling or floor, and passing it through produced
# three confident false positives in two sweeps — "the y-axis maximum is set to 0, which
# collapses the y-axis" on wheat-production and cocoa-bean-production, and "the Y-axis is fixed
# to a minimum of 0%, truncating negative growth values" on weekly-growth-covid-deaths — against
# zero true findings. Filtering just the zeros was tried first and was not enough: the model
# reasons about whichever half survives. If axis bounds are ever worth checking, compare them
# against the data range in code, where a sentinel can be handled exactly rather than described.


# Grapher chart types mapped to the tab parameter that shows them.
_TAB_FOR_TYPE = {
    "LineChart": "line",
    "DiscreteBar": "discrete-bar",
    "SlopeChart": "slope",
    "StackedArea": "line",
    "StackedBar": "line",
    "Marimekko": "marimekko",
    "ScatterPlot": "scatter",
}


def other_tab(config: dict[str, Any]) -> str | None:
    """The ``tab=`` parameter for a second view of the same chart, or None if there isn't one.

    Charts that open on the map hide the time dimension entirely, and a series that misbehaves
    over time is invisible there — which is where problems are most often spotted in practice.
    So when a chart defaults to the map, look at its time-series tab as well, and vice versa.
    """
    default = (config.get("tab") or "").lower()
    types = [t for t in (config.get("chartTypes") or []) if t in _TAB_FOR_TYPE]

    if default == "map":
        # chartTypes is often absent, in which case grapher's default applies: a line chart.
        return f"tab={_TAB_FOR_TYPE[types[0]]}" if types else "tab=line"
    if config.get("hasMapTab") and default in ("", "chart", "line", "discrete-bar", "slope"):
        return "tab=map"
    return None


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
