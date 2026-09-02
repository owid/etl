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
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from structlog import get_logger

from apps.chart_critic.critic import claim_tokens, format_views, same_finding
from etl.paths import CACHE_DIR

log = get_logger()

STATE_PATH = CACHE_DIR / "chart_critic" / "digest_state.json"
SLACK_CHANNEL = "C03NV9Z3YSV"  # #we-need-to-correct-it
ADMIN_URL = "https://admin.owid.io"

# Matches #analytics-bites, the other daily owidbot post in this workspace: a severity dot, then
# facts separated by wide middots. Consistency is worth more than a bespoke shape — people learn
# one scanning pattern.
SEVERITY_DOT = {"high": ":red_circle:", "medium": ":large_orange_circle:", "low": ":large_yellow_circle:"}

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


def chart_facts(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """``{slug: {"chart_id": int | None, "indicators": [catalog paths]}}`` for the flagged charts.

    A data-level defect lives in an indicator, not in a chart, so the same claim surfaces on
    every chart sharing that column. This already happened: the UK coal share above 100% was
    found three times on three charts, all from one column of one ETL step. With a per-chart
    fingerprint those are three separate findings, and with only five digest slots they crowd
    out everything else — which is precisely what makes ``--include-data-updates`` unusable
    today, since one dataset refresh touches thousands of charts.

    The same query yields the numeric chart id, which is what the admin edit URL needs — the
    public slug will not do. A multi-dim view has no row here, so it has no id and gets no edit
    link; that is correct rather than missing, since there is no single chart to edit.

    Only flagged charts are looked up, so this is one small query. It fails soft: without a
    database the fingerprint falls back to the slug (which over-reports rather than dropping a
    finding) and the message simply carries no edit link.
    """
    slugs = sorted({r["slug"] for r in results if r.get("issues")})
    if not slugs:
        return {}
    from etl.db import read_sql

    try:
        df = read_sql(
            """
            SELECT cc.slug AS slug, c.id AS chartId, v.catalogPath AS catalogPath
            FROM chart_configs cc
            JOIN charts c            ON c.configId = cc.id
            JOIN chart_dimensions cd ON cd.chartId = c.id
            JOIN variables v         ON v.id = cd.variableId
            WHERE cc.slug IN %(slugs)s
            """,
            params={"slugs": tuple(slugs)},
        )
    except Exception as e:  # noqa: BLE001 — dedup quality and a link, not correctness
        log.warning("chart_critic.chart_facts_failed", error=str(e))
        return {}
    out: dict[str, dict[str, Any]] = {}
    for slug, group in df.groupby("slug"):
        # The whole indicator set, so two charts collapse only when they read the same columns.
        # Keyed on the step path without its version, so tomorrow's re-run of the same step does
        # not re-post yesterday's finding under a new name.
        paths = sorted({re.sub(r"/\d{4}-\d{2}-\d{2}/", "/", p) for p in group["catalogPath"] if isinstance(p, str)})
        out[str(slug)] = {"chart_id": int(group["chartId"].iloc[0]), "indicators": paths}
    return out


def _indicators(facts: dict[str, dict[str, Any]] | None, slug: str) -> list[str]:
    got = (facts or {}).get(slug, {}).get("indicators") or []
    return [str(path) for path in got]


def _fingerprint_keys(slug: str, issue: dict[str, Any], facts: dict[str, dict[str, Any]] | None = None) -> list[str]:
    """Every identity under which this finding should be recognised, across days and charts.

    One key per indicator for a data-level finding, rather than one key for the chart's whole
    indicator set. The set was the obvious thing to hash and it does not work: a defective column
    read alone by chart A and alongside another column by chart B yields ``A`` and ``A|B``, so the
    identical finding still takes two of the five digest slots. Matching on *any* shared indicator
    is what "one defective column is one finding" actually requires.

    The claim's own significant words are in every key, so two different problems on charts that
    happen to share an indicator do not collapse into each other.

    Chart-level findings stay keyed by slug: a wrong subtitle really is specific to that chart
    even when the data behind it is shared.
    """
    words = sorted(claim_tokens(issue.get("claim", "")))
    tail = f"{issue.get('kind', '')}:{'-'.join(words)}"
    indicators = _indicators(facts, slug) if issue.get("kind") == "data" else []
    return [f"{i}:{tail}" for i in indicators] or [f"{slug}:{tail}"]


def _already_posted(keys: list[str], known: Iterable[str]) -> bool:
    """Whether any of these keys names a finding we have already sent.

    Not an exact key match, which is what this used to be. The overlapping review window means
    a chart is looked at on several consecutive days, and the model does not repeat itself
    verbatim — so an exact match would file "the unit is set to doses" and "all three indicators
    are labelled in doses" as two separate findings and post the same thing twice. Subject and
    kind must agree exactly, and the claim's vocabulary has to overlap by the same margin that
    folds repeated passes into one finding within a run.
    """
    for key in keys:
        subject, kind, words = key.split(":", 2)
        tokens = set(words.split("-")) - {""}
        for other in known:
            other_subject, other_kind, other_words = other.split(":", 2)
            if (other_subject, other_kind) != (subject, kind):
                continue
            if same_finding(tokens, set(other_words.split("-")) - {""}):
                return True
    return False


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


def new_findings(
    results: list[dict[str, Any]], state: dict[str, str], facts: dict[str, dict[str, Any]] | None = None
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Findings not posted before, best first.

    Deduplicated by indicator for data-level findings and by chart for chart-level ones — pass
    ``facts`` from :func:`chart_facts` to get the former; without it everything falls back to
    per-chart, which over-reports.
    """
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    order = {"high": 0, "medium": 1, "low": 2}
    ranked = sorted(
        ((r, i) for r in results for i in r["issues"]),
        key=lambda ri: (order.get(ri[1].get("severity", "low"), 3), -ri[0].get("views", 0)),
    )
    for result, issue in ranked:
        keys = _fingerprint_keys(result["slug"], issue, facts)
        # Already-known on either axis: an earlier day (state) or an earlier chart in this run
        # sharing one indicator (seen) — the same defect on a second chart is not news, even
        # though the two charts are not the same chart.
        if _already_posted(keys, state) or _already_posted(keys, seen):
            continue
        seen.update(keys)
        out.append((result, issue))
    return out


def _chart_title(result: dict[str, Any]) -> str:
    """The chart's display title, taken from the bundle summary the model was shown.

    Read out of the summary rather than plumbed through as its own field, so it works for a
    cached bundle too without changing the cache format. Falls back to the slug.
    """
    for line in (result.get("summary") or "").splitlines():
        if line.startswith("title: "):
            return line[len("title: ") :].strip()
    return result["slug"]


def format_slack(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    reviewed: int,
    candidates: int,
    incomplete: int = 0,
    window_days: int | None = None,
    facts: dict[str, dict[str, Any]] | None = None,
    cost: float = 0.0,
) -> str:
    """Slack mrkdwn — single asterisks for bold, in the shape #analytics-bites uses.

    Each finding is a linked bold title, the claim in a sentence, and a footer of severity,
    readership and an admin edit link. Matching the other daily owidbot post is deliberate: it
    is one scanning pattern to learn, and the edit link is what turns a report into an action.
    """
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
        url = issue.get("url") or f"https://ourworldindata.org/grapher/{result['slug']}"
        dot = SEVERITY_DOT.get(issue.get("severity", "low"), ":large_yellow_circle:")
        footer = [f"{dot} {issue.get('severity', 'low')} · {issue.get('kind', 'chart')}-level"]
        if views := format_views(result.get("views")):
            footer.append(views)
        chart_id = (facts or {}).get(result["slug"], {}).get("chart_id")
        if chart_id:
            footer.append(f"<{ADMIN_URL}/admin/charts/{chart_id}/edit|Edit chart>")
        lines.append(
            f"\n*<{url}|{_chart_title(result)}>*\n{issue.get('claim', '').rstrip('.')}.\n{'   ·   '.join(footer)}"
        )
    lines.append("")
    # What the run cost, in the footer. It is the sweep's actual model spend, so a day whose
    # charts were all already reviewed reads as $0.00 — the cache doing its job, not an error.
    # The gating --eval run is a separate invocation and is not counted here.
    spend = f"${cost:,.2f}" if cost >= 0.01 else "<$0.01"
    lines.append(
        "_Posted by `etl chart-critic` — an LLM reading each chart, its metadata and its values. "
        "Each of these is a claim to check rather than a confirmed error. "
        f"Reviewing {reviewed} chart{'s' if reviewed != 1 else ''} cost {spend}._"
    )
    if incomplete:
        lines.append(f"_{incomplete} chart(s) could not be reviewed, so treat this as incomplete._")
    return "\n".join(lines)


def stamp(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    state: dict[str, str],
    facts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Record what was posted. ``facts`` must be the same mapping :func:`new_findings` used —
    a key written per-chart and looked up per-indicator matches nothing, and the digest would
    re-post every finding every day."""
    today = datetime.now(timezone.utc).date().isoformat()
    for result, issue in findings[:MAX_FINDINGS]:
        for key in _fingerprint_keys(result["slug"], issue, facts):
            state[key] = today
    return state
