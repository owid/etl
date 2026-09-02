"""Daily digest: review the charts that changed yesterday, and say something only if it matters.

Selection is deliberately narrow. Charts whose **configuration** changed are ~17 a day, which is
a sweep that costs cents and finishes in a minute. Charts whose **data** was refreshed are ~105 a
day but 3,138 over a week, because one dataset update touches thousands at once — reviewing those
is available behind ``--include-data-updates`` and is a different job, needing a much larger cap
and much harder deduplication.

Three things keep the digest worth reading, and all three are about restraint:

- **It deduplicates by indicator, not by chart.** One bad column of one ETL step produced the same
  finding on three separate charts; a dataset refresh would produce it on hundreds.
- **It remembers what it has posted.** Without state, day two repeats day one and the channel
  learns to skip it inside a week.
- **It says nothing when there is nothing.** No daily heartbeat. A digest that only speaks when it
  has something is one people keep reading.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from etl.paths import CACHE_DIR

STATE_PATH = CACHE_DIR / "chart_critic" / "digest_state.json"
SLACK_CHANNEL = "C03NV9Z3YSV"  # #we-need-to-correct-it

# What a person reads over coffee. If the sweep found more, the ranking is the deliverable.
MAX_FINDINGS = 5


def changed_slugs(days: int = 1, include_data_updates: bool = False) -> list[str]:
    """Published chart slugs that changed in the last ``days``.

    By default this is configuration edits only. ``include_data_updates`` adds every published
    chart whose underlying dataset was re-run, which is the right signal for "did this update
    break anything" but an order of magnitude larger and extremely uneven day to day.

    **Needs the production database.** ``updatedAt`` on a staging copy reflects when the copy was
    made, not when anyone edited a chart, so run this with ``ENV_FILE=.env.prod.read
    DATA_API_ENV=production DB_NAME=live_grapher`` or it will report that nothing changed.
    """
    from etl.db import read_sql

    config_edits = read_sql(
        """
        SELECT DISTINCT cc.slug
        FROM chart_configs cc
        WHERE cc.updatedAt >= NOW() - INTERVAL %(days)s DAY
          AND cc.slug IS NOT NULL
          AND JSON_EXTRACT(cc.config, '$.isPublished') = TRUE
        """,
        params={"days": days},
    )
    slugs = set(config_edits.slug)

    if include_data_updates:
        data_edits = read_sql(
            """
            SELECT DISTINCT cc.slug
            FROM datasets d
            JOIN variables v         ON v.datasetId = d.id
            JOIN chart_dimensions cd ON cd.variableId = v.id
            JOIN charts c            ON c.id = cd.chartId
            JOIN chart_configs cc    ON cc.id = c.configId
            WHERE d.dataEditedAt >= NOW() - INTERVAL %(days)s DAY
              AND cc.slug IS NOT NULL
              AND JSON_EXTRACT(cc.config, '$.isPublished') = TRUE
            """,
            params={"days": days},
        )
        slugs |= set(data_edits.slug)

    return sorted(slugs)


def _fingerprint(slug: str, issue: dict[str, Any]) -> str:
    """Identify a finding across days, loosely enough to survive the model's rewording."""
    words = sorted({w.rstrip("s") for w in re.findall(r"[a-z0-9]{5,}", issue.get("claim", "").lower())})
    return f"{slug}:{issue.get('kind', '')}:{'-'.join(words[:8])}"


def load_state() -> dict[str, str]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {}


def save_state(state: dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1, sort_keys=True))


def new_findings(results: list[dict[str, Any]], state: dict[str, str]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Findings not posted before, deduplicated by indicator-shaped fingerprint, best first."""
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    order = {"high": 0, "medium": 1, "low": 2}
    ranked = sorted(
        ((r, i) for r in results for i in r["issues"]),
        key=lambda ri: (order.get(ri[1].get("severity", "low"), 3), -ri[0].get("views", 0)),
    )
    for result, issue in ranked:
        key = _fingerprint(result["slug"], issue)
        if key in state or key in seen:
            continue
        seen.add(key)
        out.append((result, issue))
    return out


def format_slack(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    reviewed: int,
    candidates: int,
    incomplete: int = 0,
    window_days: int | None = None,
) -> str:
    """Slack mrkdwn — single asterisks for bold, and the channel's own one-line-per-chart shape."""
    if not findings:
        return ""

    shown = findings[:MAX_FINDINGS]
    # Say what was actually reviewed. A header claiming "changed since yesterday" on a run that
    # reviewed a hand-picked list is the kind of small inaccuracy that costs a digest its trust.
    if window_days == 1:
        scope = "Charts changed since yesterday"
    elif window_days:
        scope = f"Charts changed in the last {window_days} days"
    else:
        scope = "Charts reviewed"
    truncated = "" if reviewed >= candidates else f" of {candidates}"
    header = (
        f"{scope} — reviewed {reviewed}{truncated}, {len(findings)} worth a look"
        + (f" (showing the top {len(shown)})" if len(findings) > len(shown) else "")
        + ":"
    )

    lines = [header]
    for result, issue in shown:
        title = result["slug"]
        url = issue.get("url") or f"https://ourworldindata.org/grapher/{result['slug']}"
        views = f" · {result['views']:,} views/yr" if result.get("views") else ""
        lines.append(f"\n• *{title}*{views}\n  {issue.get('claim', '').rstrip('.')}.\n  {url}")
    lines.append("")
    lines.append(
        "_Each of these is a claim to check rather than a confirmed error — the review is an LLM "
        "reading the chart, its metadata and its values._"
    )
    if incomplete:
        lines.append(f"_{incomplete} chart(s) could not be reviewed, so treat this as incomplete._")
    return "\n".join(lines)


def stamp(findings: list[tuple[dict[str, Any], dict[str, Any]]], state: dict[str, str]) -> dict[str, str]:
    today = datetime.now(timezone.utc).date().isoformat()
    for result, issue in findings[:MAX_FINDINGS]:
        state[_fingerprint(result["slug"], issue)] = today
    return state
