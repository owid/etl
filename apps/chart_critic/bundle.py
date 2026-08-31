"""Build the review bundle for a published chart: its render, its metadata, its numbers.

Everything here comes from the public grapher endpoints, so the bundle is exactly what a
reader could see. Three deliberate details, each learned the hard way:

- The endpoints sit behind bot protection: without a browser-like ``User-Agent`` they answer
  ``403`` for ``.metadata.json`` and ``.csv`` while the PNG still works, which silently yields
  bundles with no metadata and no data. Hence :data:`HEADERS`, and hard failures rather than
  empty bundles.
- Some datasets (IHME/GBD among them) are non-redistributable and answer ``403`` on the CSV.
  That is not an error — the chart is still worth reviewing on its render and metadata — so it
  is reported as ``data_available=False``.
- A slug carrying pageviews can be gone entirely (deleted, or an explorer rather than a chart),
  in which case every endpoint answers ``404``. That is its own outcome, not a failure.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

from apps.chart_critic import cache

GRAPHER_URL = "https://ourworldindata.org/grapher"

# A browser-like UA. Without it the JSON and CSV endpoints answer 403 (the PNG does not).
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}

# Fields worth showing the model. The long forms (titleLong, citationLong, fullMetadata) add
# tokens without adding judgement.
COLUMN_FIELDS = ("titleShort", "descriptionShort", "unit", "shortUnit", "timespan", "type", "conversionFactor")
CHART_FIELDS = ("title", "subtitle", "note", "xAxisLabel", "yAxisLabel")

# Cap how many indicators of a wide chart get summarised, so a 100-column chart cannot blow up
# the prompt.
MAX_COLUMNS = 10


class ChartGone(Exception):
    """Every endpoint answered 404 — the slug no longer resolves to a chart."""


@dataclass
class Bundle:
    """What the critic is shown for one chart."""

    slug: str
    png: bytes | None = None
    summary: str = ""
    data_available: bool = True
    notes: list[str] = field(default_factory=list)
    from_cache: bool = False

    @property
    def url(self) -> str:
        return f"{GRAPHER_URL}/{self.slug}"


def _fetch(url: str) -> bytes:
    return urlopen(Request(url, headers=HEADERS), timeout=120).read()


def _numeric_summary(df: pd.DataFrame) -> list[str]:
    """Describe the data. Deliberately not a detector — no thresholds, no flags, no candidates.

    Anomaly detection over indicator values belongs in ``apps/anomalist``; this only tells the
    model what is in the table so it can apply judgement to it.
    """
    lines: list[str] = []
    time_col = "Year" if "Year" in df.columns else ("Day" if "Day" in df.columns else df.columns[2])
    data_cols = [
        c for c in df.columns if c not in ("Entity", "Code", time_col) and pd.api.types.is_numeric_dtype(df[c])
    ]
    lines.append(
        f"\ndata: {len(df):,} rows | {df.Entity.nunique()} entities | {time_col} {df[time_col].min()}..{df[time_col].max()}"
    )
    latest = df[time_col].max()
    for col in data_cols[:MAX_COLUMNS]:
        series = df[["Entity", time_col, col]].dropna()
        if series.empty:
            lines.append(f"  {col}: no values")
            continue
        fmt = lambda v: f"{v:,.4g}"  # noqa: E731
        hi, lo = series.loc[series[col].idxmax()], series.loc[series[col].idxmin()]
        lines.append(
            f"  {col}: min {fmt(lo[col])} ({lo.Entity} {lo[time_col]}) | max {fmt(hi[col])} ({hi.Entity} {hi[time_col]})"
        )
        last = series[series[time_col] == latest].sort_values(col, ascending=False)
        if len(last):
            top = ", ".join(f"{r.Entity} {fmt(getattr(r, col))}" for r in last.head(4).itertuples())
            bottom = ", ".join(f"{r.Entity} {fmt(getattr(r, col))}" for r in last.tail(3).itertuples())
            lines.append(f"     {latest} highest: {top} | lowest: {bottom}")
    if len(data_cols) > MAX_COLUMNS:
        lines.append(f"  … and {len(data_cols) - MAX_COLUMNS} further indicators not summarised")
    return lines


def render(slug: str, params: str = "") -> bytes:
    """The chart's PNG for a specific view, e.g. ``params="country=~CAF&time=2000..latest"``."""
    query = f"?{params.lstrip('?')}" if params else ""
    return _fetch(f"{GRAPHER_URL}/{slug}.png{query}")


def build(
    slug: str, with_image: bool = True, use_cache: bool = True, ttl_hours: float = cache.DEFAULT_TTL_HOURS
) -> Bundle:
    """Fetch and assemble the review bundle for one chart slug.

    Raises:
        ChartGone: the slug does not resolve (deleted chart, or an explorer).
        HTTPError: anything else the endpoints return.
    """
    if use_cache:
        cached = cache.read_bundle(slug, ttl_hours=ttl_hours)
        if cached and (cached.get("png") is not None or not with_image):
            return Bundle(
                slug=slug,
                png=cached["png"] if with_image else None,
                summary=cached["summary"],
                data_available=cached["data_available"],
                notes=list(cached["notes"]),
                from_cache=True,
            )

    bundle = Bundle(slug=slug)

    try:
        meta = json.loads(_fetch(f"{GRAPHER_URL}/{slug}.metadata.json?useColumnShortNames=true"))
    except HTTPError as e:
        if e.code == 404:
            raise ChartGone(slug) from e
        raise

    lines: list[str] = []
    chart = meta.get("chart") or {}
    for key in CHART_FIELDS:
        if chart.get(key):
            lines.append(f"{key}: {chart[key]}")
    lines.append("")
    for name, column in (meta.get("columns") or {}).items():
        entry = [f"indicator [{name}]"]
        entry += [f"    {k}: {column[k]}" for k in COLUMN_FIELDS if column.get(k) not in (None, "")]
        lines.append("\n".join(entry))

    try:
        raw = _fetch(f"{GRAPHER_URL}/{slug}.csv?useColumnShortNames=true&csvType=full").decode()
        if raw.lstrip().startswith("{"):
            # The endpoint answers 200 with a JSON error body for non-redistributable data.
            raise ValueError(json.loads(raw).get("error", "data unavailable"))
        df = pd.read_csv(io.StringIO(raw))
        df = df.rename(
            columns={c: c.capitalize() for c in df.columns if c.lower() in ("entity", "code", "year", "day")}
        )
        lines += _numeric_summary(df)
    except (HTTPError, ValueError) as e:
        bundle.data_available = False
        reason = str(e)[:120]
        bundle.notes.append(f"values unavailable: {reason}")
        lines.append(f"\ndata: not available to this reviewer ({reason})")

    bundle.summary = "\n".join(lines)

    if with_image:
        bundle.png = _fetch(f"{GRAPHER_URL}/{slug}.png")

    if use_cache:
        cache.write_bundle(slug, bundle.summary, bundle.png, bundle.notes, bundle.data_available)

    return bundle
