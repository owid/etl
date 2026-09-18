"""Build the review bundle for a published chart: its render, its metadata, its numbers.

Everything here comes from the public grapher endpoints, so the bundle is exactly what a
reader could see. Three deliberate details, each learned the hard way:

- The endpoints reject the default ``python-urllib`` User-Agent with ``403`` on
  ``.metadata.json`` and ``.csv`` while the PNG still works, which silently yields bundles with no
  metadata and no data. Requests go through ``etl.http.session``, whose ``owid-etl`` UA is both
  accepted and attributable in our own CDN logs — an earlier version spoofed a browser, which was
  never necessary: any non-default UA gets through.
- Some datasets (IHME/GBD among them) are non-redistributable and answer ``403`` on the CSV.
  That is not an error — the chart is still worth reviewing on its render and metadata — so it
  is reported as ``data_available=False``.
- A slug carrying pageviews can be gone entirely (deleted, or an explorer rather than a chart),
  in which case every endpoint answers ``404``. That is its own outcome, not a failure.
"""

from __future__ import annotations

import io
import json
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from requests import HTTPError

from apps.chart_critic import cache, chart_config
from etl.http import session as http_session

GRAPHER_URL = "https://ourworldindata.org/grapher"

# Fields worth showing the model. The long forms (titleLong, citationLong, fullMetadata) add
# tokens without adding judgement.
#
# ``descriptionKey`` and ``descriptionProcessing`` are here for one specific reason: they are where
# OWID documents the data that *looks* wrong and is not. The critic filed the Central African
# Republic's life expectancy oscillating between 14.7 and 57.4 years as an error (owid/etl#6779);
# it is the UN's deliberate crisis-mortality adjustment, and the investigation ended in
# owid/etl#6813 writing that explanation into the indicator's key facts — for readers, who hit the
# same spike. Showing the model those fields closes the loop: the answer to a false positive is to
# document the data, and the documentation is then what stops it being raised again. The
# alternative, a list of findings to suppress, would be maintained for the critic alone and would
# leave the reader with the unexplained spike.
#
# Measured over eight charts: ~520 extra input tokens each, about $0.0004 a chart. The render
# dominates the bill by an order of magnitude.
COLUMN_FIELDS = (
    "titleShort",
    "descriptionShort",
    "descriptionKey",
    "descriptionProcessing",
    "unit",
    "shortUnit",
    "timespan",
    "type",
    "conversionFactor",
)
CHART_FIELDS = ("title", "subtitle", "note", "xAxisLabel", "yAxisLabel")

# Cap how many indicators of a wide chart get summarised, so a 100-column chart cannot blow up
# the prompt.
MAX_COLUMNS = 10


class DataFetchFailed(Exception):
    """The values could not be fetched for an operational reason, as opposed to a policy one.

    Kept distinct from the non-redistributable case on purpose. A chart whose data we are not
    allowed to serve is still reviewable on its metadata and its image; a chart whose CSV 503s
    is a chart nobody reviewed, and the difference has to reach the exit code.
    """


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
    # ``tab=`` for the chart's other view, when it has one. A map-default chart hides time.
    other_tab_params: str = ""

    @property
    def url(self) -> str:
        return f"{GRAPHER_URL}/{self.slug}"


def _declined_reason(response: Any) -> str | None:
    """The endpoint's own explanation for refusing the data, or None if it did not give one.

    Keyed on the body rather than the status, because the same refusal has been seen as a 403
    and as a 200 carrying an error document. A 403 with no such body is a different animal — a
    CDN block, say — and must not be mistaken for "we are not allowed to share this".
    """
    if response is None or response.status_code != 403:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    reason = payload.get("error") if isinstance(payload, dict) else None
    return str(reason) if reason else None


def _fetch(url: str, attempts: int = 3) -> bytes:
    """Fetch from grapher through the repo's shared session, retrying 5xx with a short backoff.

    Transient 503s from the render service and the CSV endpoint were the only failure category
    left once the bundle bugs were fixed, and a chart lost to one is a chart nobody reviewed.
    """
    for attempt in range(attempts):
        response = http_session.get(url, timeout=120)
        if response.status_code >= 500 and attempt < attempts - 1:
            time.sleep(2 * (attempt + 1))
            continue
        if response.status_code == 404:
            raise ChartGone(url)
        response.raise_for_status()
        return response.content
    raise RuntimeError("unreachable")


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
                other_tab_params=cached.get("other_tab_params", ""),
                render_failed=bool(cached.get("render_failed")),
            )

    bundle = Bundle(slug=slug)

    query = "useColumnShortNames=true" + (f"&{params.lstrip('?')}" if params else "")
    meta = json.loads(_fetch(f"{GRAPHER_URL}/{slug}.metadata.json?{query}"))

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
    except ValueError as e:
        # The endpoint's own "we may not redistribute this" answer. A real and permanent
        # condition (IHME/GBD and a few others), so the chart is still worth reviewing on its
        # metadata and image — this is the one case that degrades rather than fails.
        bundle.data_available = False
        reason = str(e)[:120]
        bundle.notes.append(f"values unavailable: {reason}")
        lines.append(f"\ndata: not available to this reviewer ({reason})")
    except HTTPError as e:
        # Two different 403s live here, and telling them apart is the whole point.
        #
        # The endpoint answers 403 with a JSON body — {"status":403,"error":"This chart contains
        # non-redistributable data…"} — for the IHME/GBD family and a handful of others. That is
        # policy, permanent, and the chart is still worth reviewing on its metadata and image:
        # the "aumber" subtitle typo in the fixture set is on exactly such a chart, and failing
        # it outright cost that fixture its finding.
        #
        # Anything else — a 5xx that outlived its retries, a rate limit, a bare 403 from the CDN,
        # a 404 on the CSV alone — is operational. Treating that as "no data" would let the report
        # and the digest call the chart clean when the model never saw a single value, so the
        # chart fails and the caller marks the run incomplete and exits 2.
        detail = _declined_reason(e.response)
        if detail is None:
            code = e.response.status_code if e.response is not None else "?"
            raise DataFetchFailed(f"CSV endpoint returned HTTP {code}") from e
        bundle.data_available = False
        bundle.notes.append(f"values unavailable: {detail[:120]}")
        lines.append(f"\ndata: not available to this reviewer ({detail[:120]})")

    # Not fatal: ``--slugs`` is documented to work without a database, and the config is one
    # channel of several. But it goes in the notes, so a review that could not see the chart's
    # configuration says so instead of looking like a review that saw everything.
    try:
        config = chart_config.fetch(slug) if with_config else None
    except chart_config.ConfigUnavailable as e:
        config = None
        bundle.notes.append(f"chart configuration unavailable ({str(e)[:80]}) — reviewed without it")
    if config:
        lines += chart_config.summarize(config)
        if not params:
            bundle.other_tab_params = chart_config.other_tab(config) or ""

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
            code = e.response.status_code if e.response is not None else "?"
            bundle.notes.append(f"no render available (HTTP {code}) — reviewed without the image")

    if use_cache:
        cache.write_bundle(
            cache_key,
            bundle.summary,
            bundle.png,
            bundle.notes,
            bundle.data_available,
            bundle.render_failed,
            bundle.other_tab_params,
        )

    return bundle
