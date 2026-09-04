"""Daily digest: review the charts that changed yesterday, and say something only if it matters.

Selection is deliberately narrow. Charts whose **configuration** changed are ~17 a day, which is
a sweep that costs cents and finishes in a minute. Charts whose **data** was refreshed are ~105 a
day but 3,138 over a week, because one dataset update touches thousands at once — reviewing those
is available behind ``--include-data-updates`` and is a different job, needing a much larger cap
and much harder deduplication.

Three things keep the digest worth reading, and all three are about restraint:

- **It deduplicates by indicator, not by chart.** One bad column of one ETL step produced the same
  finding on three separate charts; a dataset refresh would produce it on hundreds.
- **It remembers what it has posted**, and remembers it by what the claim says rather than by how
  it was worded — the model rewords freely, and a state file keyed on the sentence let day two
  repeat day one anyway. Without either, the channel learns to skip the digest inside a week.
- **It says nothing when there is nothing.** No daily heartbeat. A digest that only speaks when it
  has something is one people keep reading.
"""

from __future__ import annotations

import functools
import json
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from slack_sdk.errors import SlackClientError
from structlog import get_logger

from apps.chart_critic.critic import claim_tokens, format_views, same_claim
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

# On every finding rather than once in the lead. The lead says what the sweep was; this says what
# the claim is worth, and it is the finding that gets forwarded, quoted and replied to on its own.
# Plain italics, no nested code span: Slack mrkdwn renders nested formatting unreliably.
CAVEAT = "_A chart-critic claim to check, not a confirmed error._"

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


def chart_facts(results: list[dict[str, Any]], editor_window_days: int | None = None) -> dict[str, dict[str, Any]]:
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
    :func:`attach_mentions` for when we may name them. ``editor_window_days`` is the sweep's own
    window, and an editor whose edit falls outside it is reported as unknown: selection is on
    ``chart_configs.updatedAt``, which an ETL upsert or a chart sync bumps without anybody
    editing anything, so "this chart changed" is not on its own evidence about who changed it.

    Only flagged charts are looked up, so this is one small query. It fails soft: without a
    database the fingerprint falls back to the slug (which over-reports rather than dropping a
    finding) and the message simply carries no edit link and names nobody.
    """
    slugs = sorted({r["slug"] for r in results if r.get("issues")})
    if not slugs:
        return {}
    from etl.db import read_sql

    # A literal fragment, and the window itself stays a bound parameter.
    edited_in_window = (
        "AND c.lastEditedAt >= NOW() - INTERVAL %(editor_days)s DAY" if editor_window_days is not None else ""
    )
    try:
        df = read_sql(
            f"""
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
            LEFT JOIN users u        ON u.id = c.lastEditedByUserId {edited_in_window}
            WHERE cc.slug IN %(slugs)s
            """,
            params={"slugs": tuple(slugs), "editor_days": editor_window_days},
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
    # Slack refusing to answer (SlackClientError covers both an API error and a malformed
    # request) and Slack being unreachable (a DNS failure, a timeout, a reset connection — all
    # OSError under the SDK's urllib transport) have the same right answer: fall back to the
    # plain name. A courtesy in a footer must not be able to fail a --digest run.
    except (SlackClientError, OSError) as e:
        log.warning("chart_critic.slack_lookup_failed", email=email, error=str(e))
        return None


def attach_mentions(facts: dict[str, dict[str, Any]], tag_last_editor: bool) -> None:
    """Resolve each chart's editor to a Slack mention, in place, before any message is built.

    A finding addressed to nobody gets read and forgotten, so a finding names the person who
    last edited the chart — but only when ``tag_last_editor`` says the sweep earns it. That is
    the configuration-edit sweep, where the chart is under review *because* of a recent edit and
    "you changed this, it may be off" is a fair thing to say. In any other selection the last
    editor may have changed a colour two years ago, and naming them is noise at best.

    The other half of that rule lives in :func:`chart_facts`, which reports no editor at all
    unless their edit is inside the sweep's window — so a longer ``--changed-since`` widens who
    can be named exactly as far as it widens what the digest says it reviewed, and no further.

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


def _dedup_keys(slug: str, issue: dict[str, Any], facts: dict[str, dict[str, Any]] | None = None) -> list[str]:
    """Every identity under which this finding should be recognised, across days and charts.

    One key per indicator for a data-level finding, rather than one key for the chart's whole
    indicator set. The set was the obvious thing to hash and it does not work: a defective column
    read alone by chart A and alongside another column by chart B yields ``A`` and ``A|B``, so the
    identical finding still takes two of the five digest slots. Matching on *any* shared indicator
    is what "one defective column is one finding" actually requires.

    Chart-level findings stay keyed by slug: a wrong subtitle really is specific to that chart
    even when the data behind it is shared.

    The key deliberately carries no part of the claim. It used to carry the claim's significant
    words, which made the key change whenever the model reworded the same finding — and it rewords
    constantly, so a claim posted yesterday came back as a new key today and was posted again.
    What the claim *says* is compared separately, with :func:`critic.same_claim`, so two different
    problems on charts sharing an indicator still do not collapse into each other.
    """
    kind = str(issue.get("kind", ""))
    indicators = _indicators(facts, slug) if kind == "data" else []
    return [f"{i}:{kind}" for i in indicators] or [f"{slug}:{kind}"]


def load_state() -> dict[str, list[dict[str, Any]]]:
    """What has been posted, as ``{key: [{"words": [...], "date": "YYYY-MM-DD"}, ...]}``.

    The words are :func:`critic.claim_tokens` of the claim, kept rather than a hash of it because
    recognising the same finding tomorrow means comparing what it says, not whether it was worded
    identically.
    """
    if not STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {}
    return _upgrade(raw) if isinstance(raw, dict) else {}


def _upgrade(raw: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Read the older state format as well as the current one.

    The first format put the claim's words *in* the key —
    ``<indicator-or-slug>:<kind>:<eight-words>`` mapped to a date — which is the bug this
    replaced. But the file is the only record of what the channel has already seen, so the old
    keys are read back into the current shape rather than dropped: their words become the claim
    they stood for, and yesterday's findings stay suppressed instead of all being posted once more
    on the day this ships.

    Matching against a migrated entry is weaker than against a current one, because the old key
    kept only the eight alphabetically-first words of five characters or more: the stored words
    are a diluted sample of the claim, so a *reworded* repeat of a pre-migration finding can
    still slip through once. That resolves itself — every finding posted from now on is recorded
    in full — and it is a better floor than dropping the file and re-posting everything in it.
    """
    state: dict[str, list[dict[str, Any]]] = {}
    for key, value in raw.items():
        if isinstance(value, list):
            state.setdefault(key, []).extend(value)
            continue
        head, _, words = str(key).rpartition(":")
        if head:
            state.setdefault(head, []).append({"words": [w for w in words.split("-") if w], "date": str(value)})
    return state


def save_state(state: dict[str, list[dict[str, Any]]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1, sort_keys=True))


def new_findings(
    results: list[dict[str, Any]],
    state: dict[str, list[dict[str, Any]]],
    facts: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Findings not posted before, best first.

    Deduplicated by indicator for data-level findings and by chart for chart-level ones — pass
    ``facts`` from :func:`chart_facts` to get the former; without it everything falls back to
    per-chart, which over-reports.

    "Already posted" is a question about what the claim says, not about how it was worded, so a
    finding is suppressed when its claim overlaps one already recorded under any of its keys —
    the same test :func:`cli._merge` uses to fold the repeat passes of a single run together.
    """
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    # What each key has already seen, from the state file and from this run's own findings.
    seen: dict[str, list[set[str]]] = {
        key: [set(claim.get("words") or []) for claim in claims] for key, claims in state.items()
    }
    order = {"high": 0, "medium": 1, "low": 2}
    ranked = sorted(
        ((r, i) for r in results for i in r["issues"]),
        key=lambda ri: (order.get(ri[1].get("severity", "low"), 3), -ri[0].get("views", 0)),
    )
    for result, issue in ranked:
        keys = _dedup_keys(result["slug"], issue, facts)
        tokens = claim_tokens(issue.get("claim", ""))
        # A match under any one key counts as already-known: the same defect on a second chart
        # sharing one indicator is not news, even though the two charts are not the same chart.
        if any(same_claim(tokens, before) for key in keys for before in seen.get(key, [])):
            continue
        for key in keys:
            seen.setdefault(key, []).append(tokens)
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
    """One finding as its own message: a linked bold title, the claim in a sentence, a footer of
    severity, readership, an admin edit link and whoever last edited the chart, and the caveat.

    Self-contained on purpose. It is read next to the lead message but it is also what someone
    quotes, forwards or replies to on its own, so it carries the link, the edit action and — the
    part that would otherwise be left behind in the lead — what the claim is actually worth.
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
    claim = issue.get("claim", "").rstrip(".")
    return f"*<{url}|{_chart_title(result)}>*\n{claim}.\n{'   ·   '.join(footer)}\n{CAVEAT}"


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
        f"Reviewing {reviewed} chart{'s' if reviewed != 1 else ''} cost {spend}._"
    )
    if incomplete:
        lead.append(f"_{incomplete} chart(s) could not be reviewed, so treat this as incomplete._")

    return ["\n".join(lead)] + [_format_finding(result, issue, facts) for result, issue in shown]


def stamp(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    state: dict[str, list[dict[str, Any]]],
    facts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Record what was posted. ``facts`` must be the same mapping :func:`new_findings` used —
    a claim written per-chart and looked up per-indicator matches nothing, and the digest would
    re-post every finding every day."""
    today = datetime.now(timezone.utc).date().isoformat()
    for result, issue in findings[:MAX_FINDINGS]:
        tokens = claim_tokens(issue.get("claim", ""))
        for key in _dedup_keys(result["slug"], issue, facts):
            claims = state.setdefault(key, [])
            # The same claim can arrive under a key that already holds a reworded version of it,
            # from an earlier day or from another chart sharing the indicator. Recording it again
            # would grow the file without changing any answer.
            if not any(same_claim(tokens, set(claim.get("words") or [])) for claim in claims):
                claims.append({"words": sorted(tokens), "date": today})
    return state
