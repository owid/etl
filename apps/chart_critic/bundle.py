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

from apps.chart_critic import cache, chart_config

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
    # True when the render was requested and the service refused it, as opposed to not asked for.
    # Without this distinction a chart whose render 500s looks like an incomplete cache entry and
    # is re-fetched in full on every run — and those are often the charts with the largest CSVs.
    render_failed: bool = False
    # Query params for a second view worth reviewing: the entities holding the highest and
    # lowest value in the series, which is where implausible numbers tend to live and which the
    # default view almost never shows.
    extremes_params: str = ""

    @property
    def url(self) -> str:
        return f"{GRAPHER_URL}/{self.slug}"


def _fetch(url: str) -> bytes:
    return urlopen(Request(url, headers=HEADERS), timeout=120).read()


def _extremes_params(df: pd.DataFrame, time_col: str, col: str) -> str:
    """``country=CODE~CODE`` for the entities at the extremes of one indicator."""
    # Not every CSV has a Code column — a multi-dim view's CSV is entity/year/value only.
    if "Code" not in df.columns:
        return ""
    series = df[["Entity", "Code", time_col, col]].dropna(subset=[col])
    if series.empty:
        return ""
    codes = []
    for idx in (series[col].idxmax(), series[col].idxmin()):
        code = series.loc[idx, "Code"]
        if isinstance(code, str) and code and code not in codes:
            codes.append(code)
    if not codes:
        return ""
    years = series[time_col]
    span = f"&time={years.min()}..latest" if pd.api.types.is_integer_dtype(years) else ""
    return f"country=~{'~'.join(codes)}{span}"


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
            # Index by position, not attribute: itertuples renames any column that is not a valid
            # Python identifier (SDG indicator columns start with a digit), and attribute access
            # then raises — which failed 4 charts of 100 silently before this.
            pairs = list(zip(last.Entity, last[col]))
            top = ", ".join(f"{e} {fmt(v)}" for e, v in pairs[:4])
            bottom = ", ".join(f"{e} {fmt(v)}" for e, v in pairs[-3:])
            lines.append(f"     {latest} highest: {top} | lowest: {bottom}")
    if len(data_cols) > MAX_COLUMNS:
        lines.append(f"  … and {len(data_cols) - MAX_COLUMNS} further indicators not summarised")
    return lines


def render(slug: str, params: str = "") -> bytes:
    """The chart's PNG for a specific view, e.g. ``params="country=~CAF&time=2000..latest"``."""
    query = f"?{params.lstrip('?')}" if params else ""
    return _fetch(f"{GRAPHER_URL}/{slug}.png{query}")


def build(
    slug: str,
    with_image: bool = True,
    use_cache: bool = True,
    ttl_hours: float = cache.DEFAULT_TTL_HOURS,
    with_config: bool = True,
    params: str = "",
) -> Bundle:
    """Fetch and assemble the review bundle for one chart slug, optionally for one view.

    ``params`` must be threaded through **all three** requests, not just the render. A
    multi-dimensional page's ``.metadata.json`` and ``.csv`` answer for the view its dimension
    parameters select, and answer ``500`` when the view is underspecified — so fetching metadata
    for the bare slug while rendering a specific view both fails on some mdims and, where it
    succeeds, describes a different view than the picture shows.

    Raises:
        ChartGone: the slug does not resolve (deleted chart, or an explorer).
        HTTPError: anything else the endpoints return.
    """
    cache_key = f"{slug}?{params}" if params else slug
    if use_cache:
        cached = cache.read_bundle(cache_key, ttl_hours=ttl_hours)
        if cached and (cached.get("png") is not None or cached.get("render_failed") or not with_image):
            return Bundle(
                slug=slug,
                png=cached["png"] if with_image else None,
                summary=cached["summary"],
                data_available=cached["data_available"],
                notes=list(cached["notes"]),
                from_cache=True,
                extremes_params=cached.get("extremes_params", ""),
                render_failed=bool(cached.get("render_failed")),
            )

    bundle = Bundle(slug=slug)

    try:
        query = "useColumnShortNames=true" + (f"&{params.lstrip('?')}" if params else "")
        meta = json.loads(_fetch(f"{GRAPHER_URL}/{slug}.metadata.json?{query}"))
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
        csv_query = "useColumnShortNames=true&csvType=" + ("filtered" if params else "full")
        if params:
            csv_query += f"&{params.lstrip('?')}"
        raw = _fetch(f"{GRAPHER_URL}/{slug}.csv?{csv_query}").decode()
        if raw.lstrip().startswith("{"):
            # The endpoint answers 200 with a JSON error body for non-redistributable data.
            raise ValueError(json.loads(raw).get("error", "data unavailable"))
        df = pd.read_csv(io.StringIO(raw))
        df = df.rename(
            columns={c: c.capitalize() for c in df.columns if c.lower() in ("entity", "code", "year", "day")}
        )
        lines += _numeric_summary(df)
        time_col = "Year" if "Year" in df.columns else ("Day" if "Day" in df.columns else df.columns[2])
        numeric = [
            c for c in df.columns if c not in ("Entity", "Code", time_col) and pd.api.types.is_numeric_dtype(df[c])
        ]
        if numeric:
            bundle.extremes_params = _extremes_params(df, time_col, numeric[0])
    except (HTTPError, ValueError) as e:
        bundle.data_available = False
        reason = str(e)[:120]
        bundle.notes.append(f"values unavailable: {reason}")
        lines.append(f"\ndata: not available to this reviewer ({reason})")

    config = chart_config.fetch(slug) if with_config else None
    if config:
        lines += chart_config.summarize(config)

    bundle.summary = "\n".join(lines)

    if with_image:
        try:
            bundle.png = render(slug, params)
        except HTTPError as e:
            # A render can be unavailable, so it cannot be a hard requirement. This was briefly
            # dramatic: on 2026-08-31 a render-service bug had ~22% of ordinary charts and ~82% of
            # declared mdim views answering 500 while their interactive pages answered 200. It was
            # fixed the next day — re-measured 2026-09-01 at 39/40 ordinary charts and 28/28 mdim
            # views — so treat those numbers as history, not as a property of mdims.
            # The degradation path stays because renders will fail again, and metadata and values
            # are worth reviewing without a picture.
            bundle.png = None
            bundle.render_failed = True
            bundle.notes.append(f"no render available (HTTP {e.code}) — reviewed without the image")

    if use_cache:
        cache.write_bundle(
            cache_key,
            bundle.summary,
            bundle.png,
            bundle.notes,
            bundle.data_available,
            bundle.extremes_params,
            bundle.render_failed,
        )

    return bundle
