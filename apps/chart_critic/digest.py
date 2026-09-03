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

import functools
import json
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from slack_sdk.errors import SlackApiError
from structlog import get_logger

from apps.chart_critic.critic import format_views
from etl.paths import CACHE_DIR

if TYPE_CHECKING:
    import pandas as pd

log = get_logger()

STATE_PATH = CACHE_DIR / "chart_critic" / "digest_state.json"
SLACK_CHANNEL = "C03NV9Z3YSV"  # #we-need-to-correct-it
ADMIN_URL = "https://admin.owid.io"

# Accounts that turn up as a chart's last editor without a person behind them.
SERVICE_ACCOUNTS = {"etl@ourworldindata.org"}

# Matches #analytics-bites, the other daily owidbot post in this workspace: a severity dot, then
# facts separated by wide middots. Consistency is worth more than a bespoke shape — people learn
# one scanning pattern.
SEVERITY_DOT = {"high": ":red_circle:", "medium": ":large_orange_circle:", "low": ":large_yellow_circle:"}

# What a person reads over coffee. If the sweep found more, the ranking is the deliverable.
MAX_FINDINGS = 5

# The digest file is the record of what was sent, and a digest is now several messages. They are
# joined by this marker so the file shows the split that the channel will see.
MESSAGE_SEPARATOR = "\n\n----- next message -----\n\n"


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
    """What the digest needs to know about each flagged chart beyond the review itself:
    ``{slug: {"chart_id", "indicators", "editor"}}``.

    A data-level defect lives in an indicator, not in a chart, so the same claim surfaces on
    every chart sharing that column. This already happened: the UK coal share above 100% was
    found three times on three charts, all from one column of one ETL step. With a per-chart
    fingerprint those are three separate findings, and with only five digest slots they crowd
    out everything else — which is precisely what makes ``--include-data-updates`` unusable
    today, since one dataset refresh touches thousands of charts.

    The same query yields the numeric chart id, which is what the admin edit URL needs — the
    public slug will not do. A multi-dim view has no row here, so it has no id and gets no edit
    link; that is correct rather than missing, since there is no single chart to edit.

    It also yields the chart's last editor, who is who a finding gets addressed to — see
    :func:`attach_mentions` for when we may name them.

    Only flagged charts are looked up, so this is one small query. It fails soft: without a
    database the fingerprint falls back to the slug (which over-reports rather than dropping a
    finding) and the message simply carries no edit link and names nobody.
    """
    slugs = sorted({r["slug"] for r in results if r.get("issues")})
    if not slugs:
        return {}
    from etl.db import read_sql

    try:
        df = read_sql(
            """
            SELECT cc.slug        AS slug,
                   c.id           AS chartId,
                   u.fullName     AS editorName,
                   u.email        AS editorEmail,
                   u.isActive     AS editorActive,
                   v.catalogPath  AS catalogPath
            FROM chart_configs cc
            JOIN charts c            ON c.configId = cc.id
            JOIN chart_dimensions cd ON cd.chartId = c.id
            JOIN variables v         ON v.id = cd.variableId
            LEFT JOIN users u        ON u.id = c.lastEditedByUserId
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
        out[str(slug)] = {
            "chart_id": int(group["chartId"].iloc[0]),
            "indicators": paths,
            "editor": _editor(group),
        }
    return out


def _editor(group: pd.DataFrame) -> dict[str, str] | None:
    """The chart's last editor, or ``None`` when the account is gone or has left.

    One chart per slug, so the editor columns are constant across the group's rows.
    """
    name, email, active = group["editorName"].iloc[0], group["editorEmail"].iloc[0], group["editorActive"].iloc[0]
    if not isinstance(name, str) or not active:
        return None
    return {"name": name, "email": str(email) if isinstance(email, str) else ""}


@functools.cache
def _slack_member_id(email: str) -> str | None:
    """Slack member id for an email address, or ``None`` if Slack does not know it.

    Resolved at run time rather than stored: ``users.lookupByEmail`` (the owidbot token carries
    ``users:read.email``) is exact and always current, where a handle written down next to the
    grapher account would go stale silently the first time someone's Slack account changes.
    """
    from etl import config
    from etl.slack_helpers import slack_client

    if not config.SLACK_API_TOKEN:
        return None
    try:
        return str(slack_client.users_lookupByEmail(email=email)["user"]["id"])
    except SlackApiError as e:
        log.warning("chart_critic.slack_lookup_failed", email=email, error=str(e))
        return None


def attach_mentions(facts: dict[str, dict[str, Any]], tag_last_editor: bool) -> None:
    """Resolve each chart's editor to a Slack mention, in place, before any message is built.

    A finding addressed to nobody gets read and forgotten, so a finding names the person who
    last edited the chart — but only when ``tag_last_editor`` says the sweep earns it. That is
    the configuration-edit sweep, where the chart is under review *because* they edited it in
    the last day, and where "you changed this yesterday, this may be off" is a fair thing to
    say. In any other selection the last editor may have changed a colour two years ago, and
    naming them is noise at best.

    Called once from the CLI so the Slack lookups happen in one visible place and the message
    formatting stays a pure function of its inputs. Fails soft to the plain name: an unresolved
    person reads as "last edited by Pablo Rosado", which still says who to ask, rather than as a
    broken mention.
    """
    for chart in facts.values():
        editor = (chart.get("editor") or {}) if tag_last_editor else {}
        name, email = editor.get("name"), editor.get("email")
        # An ETL grapher step upserting a config shows up as a last editor with no person behind
        # it, so it is named neither as a mention nor as a plain name — there is nobody to tell.
        if not name or email in SERVICE_ACCOUNTS:
            chart["editor_mention"] = None
            continue
        member_id = _slack_member_id(email) if email else None
        chart["editor_mention"] = f"<@{member_id}>" if member_id else name


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
    words = sorted({w.rstrip("s") for w in re.findall(r"[a-z0-9]{5,}", issue.get("claim", "").lower())})
    tail = f"{issue.get('kind', '')}:{'-'.join(words[:8])}"
    indicators = _indicators(facts, slug) if issue.get("kind") == "data" else []
    return [f"{i}:{tail}" for i in indicators] or [f"{slug}:{tail}"]


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
        # Any overlap counts as already-known: the same defect on a second chart sharing one
        # indicator is not news, even though the two charts are not the same chart.
        if any(k in state or k in seen for k in keys):
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


def _format_finding(
    result: dict[str, Any], issue: dict[str, Any], facts: dict[str, dict[str, Any]] | None = None
) -> str:
    """One finding as its own message: a linked bold title, the claim in a sentence, and a footer
    of severity, readership, an admin edit link and whoever last edited the chart.

    Self-contained on purpose. It is read next to the lead message but it is also what someone
    quotes, forwards or replies to on its own, so it carries the link and the edit action itself.
    """
    chart = (facts or {}).get(result["slug"]) or {}
    url = issue.get("url") or f"https://ourworldindata.org/grapher/{result['slug']}"
    dot = SEVERITY_DOT.get(issue.get("severity", "low"), ":large_yellow_circle:")
    footer = [f"{dot} {issue.get('severity', 'low')} · {issue.get('kind', 'chart')}-level"]
    if views := format_views(result.get("views")):
        footer.append(views)
    if chart_id := chart.get("chart_id"):
        footer.append(f"<{ADMIN_URL}/admin/charts/{chart_id}/edit|Edit chart>")
    if editor := chart.get("editor_mention"):
        footer.append(f"last edited by {editor}")
    return f"*<{url}|{_chart_title(result)}>*\n{issue.get('claim', '').rstrip('.')}.\n{'   ·   '.join(footer)}"


def format_slack(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    reviewed: int,
    candidates: int,
    incomplete: int = 0,
    window_days: int | None = None,
    facts: dict[str, dict[str, Any]] | None = None,
    cost: float = 0.0,
) -> list[str]:
    """The digest as separate Slack mrkdwn messages — single asterisks for bold, in the shape
    #analytics-bites uses.

    A lead message saying what was swept and what it cost, then **one message per finding**. The
    findings used to be one message, which gave the whole digest a single thread: every reply
    about one chart landed in the same place as the replies about the others, and a fix on one
    could not be acknowledged without noise for the rest. One message each gives every claim its
    own thread, which is where the adjudication belongs. The lead stays separate rather than
    riding on the first finding, so the first finding is not privileged.
    """
    if not findings:
        return []

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
    lead = [
        f"{scope} — reviewed {reviewed}{truncated}, {len(findings)} worth a look"
        + (f" (showing the top {len(shown)})" if len(findings) > len(shown) else "")
        + (", each posted separately below." if len(shown) > 1 else ", posted below."),
        "",
    ]
    # What the run cost, in the footer. It is the sweep's actual model spend, so a day whose
    # charts were all already reviewed reads as $0.00 — the cache doing its job, not an error.
    # The gating --eval run is a separate invocation and is not counted here.
    spend = f"${cost:,.2f}" if cost >= 0.01 else "<$0.01"
    lead.append(
        "_Posted by `etl chart-critic` — an LLM reading each chart, its metadata and its values. "
        "Each of these is a claim to check rather than a confirmed error. "
        f"Reviewing {reviewed} chart{'s' if reviewed != 1 else ''} cost {spend}._"
    )
    if incomplete:
        lead.append(f"_{incomplete} chart(s) could not be reviewed, so treat this as incomplete._")

    return ["\n".join(lead)] + [_format_finding(result, issue, facts) for result, issue in shown]


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
