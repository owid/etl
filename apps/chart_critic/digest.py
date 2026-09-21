"""Daily digest: review the charts that changed yesterday, and say something only if it matters.

Selection is deliberately narrow. Charts whose **configuration** changed are ~17 a day, which is
a sweep that costs cents and finishes in a minute. Charts whose **data** was refreshed are ~105 a
day but 3,138 over a week, because one dataset update touches thousands at once — reviewing those
is available behind ``--include-data-updates`` and is a different job, needing a much larger cap
and much harder deduplication.

Three things keep the digest worth reading, and all three are about restraint:

- **It deduplicates by indicator as well as by chart.** One bad column of one ETL step produced the
  same finding on three separate charts; a dataset refresh would produce it on hundreds. And one
  chart is one post whatever is wrong with it: a defect in its text and a defect in its data are
  not two things to a reader who opens it.
- **It remembers which charts it has posted**, and does not post the same chart or indicator
  again for a week. Recognising the same *claim* across days was tried first, by comparing the
  words of the two claims, and the model rewords too freely for that: a finding ticked on Monday
  came back reworded on Tuesday after the chart was edited. A chart already under review does not
  need a second post to say so. Without either, the channel learns to skip the digest inside a week.
- **It says nothing when there is nothing.** No daily heartbeat. A digest that only speaks when it
  has something is one people keep reading.
"""

from __future__ import annotations

import functools
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from slack_sdk.errors import SlackClientError
from structlog import get_logger

from apps.chart_critic.bundle import GRAPHER_URL
from apps.chart_critic.critic import format_views
from etl.paths import CACHE_DIR

if TYPE_CHECKING:
    import pandas as pd

log = get_logger()

STATE_PATH = CACHE_DIR / "chart_critic" / "digest_state.json"
SLACK_CHANNEL = "C087DMCTYM9"  # #chart-reviews
ADMIN_URL = "https://admin.owid.io"

# Accounts that turn up as a chart's last editor without a person behind them.
SERVICE_ACCOUNTS = {"etl@ourworldindata.org"}

# A finding is laid out the way #chart-reviews already lays out its daily post: the chart's title,
# the chart itself, a row of actions, a bold question addressed to someone, the facts in a
# blockquote, and a tick to close it. Readers of that channel have learned to scan that shape and
# to tick when they are done — a bespoke shape asks them to learn a second one for nothing.
SEVERITY_DOT = {"high": ":red_circle:", "medium": ":large_orange_circle:", "low": ":large_yellow_circle:"}

# The channel's other posts ask a closed question — "Keep it online, or archive it?" — which is
# what makes them answerable in one reply. A finding gets the same treatment.
ASK = "*Is this a real error, or is the chart fine?*"

# How the channel closes a post, and why an unticked finding is not a lost one.
TICK = ":white_check_mark:  Tick when it's been checked."

# On every finding rather than once in the lead. The lead says what the sweep was; this says what
# the claim is worth, and it is the finding that gets forwarded, quoted and replied to on its own.
# Plain italics, no nested code span: Slack mrkdwn renders nested formatting unreliably.
CAVEAT = "_A chart-critic claim to check, not a confirmed error._"

# What a person reads over coffee, and now also what a low-traffic channel can absorb:
# #chart-reviews carries two or three posts a day with one default reviewer, and a finding held
# back is not dropped — it is not stamped either, so it comes back tomorrow if it still stands.
MAX_FINDINGS = 3

# How long a chart or indicator stays out of the digest once a finding on it has been posted.
# Long enough that a fix, a tick and the re-review the edit itself triggers all fall inside it;
# short enough that a chart which goes wrong again a few weeks later is news again.
COOLDOWN_DAYS = 7

# The digest file is the record of what was sent, and a digest is several messages, some of which
# land in a thread rather than in the channel. They are joined by these markers so the file shows
# the split the channel will see.
MESSAGE_SEPARATOR = "\n\n----- next message -----\n\n"
THREAD_SEPARATOR = "\n\n----- in thread -----\n\n"


@dataclass
class DigestMessage:
    """One Slack message, with whatever is posted underneath it in its own thread."""

    text: str
    thread: list[str] = field(default_factory=list)


def render(messages: list[DigestMessage]) -> str:
    """The digest as one text, for the ``--digest`` file and the console.

    The file is the record of what was sent, so it shows the split the channel will see: where
    one message ends and the next begins, and which text lands in a thread rather than in the
    channel itself.
    """
    return MESSAGE_SEPARATOR.join(THREAD_SEPARATOR.join([m.text, *m.thread]) for m in messages)


def post_count(messages: list[DigestMessage]) -> int:
    """How many Slack calls posting this digest takes — parents and thread replies alike."""
    return sum(1 + len(m.thread) for m in messages)


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
    """Every identity under which this finding is recognised, across days and charts.

    One key per indicator for a data-level finding, rather than one key for the chart's whole
    indicator set. The set was the obvious thing to hash and it does not work: a defective column
    read alone by chart A and alongside another column by chart B yields ``A`` and ``A|B``, so the
    identical finding still takes two of the digest slots. Matching on *any* shared indicator is
    what "one defective column is one finding" actually requires.

    The chart's own slug is always a key, a data-level finding's indicators additionally. Keying a
    data finding by indicator alone, and separating the two levels by ``kind``, let one chart take
    two slots of the same digest: ``agricultural-output-dollars`` was posted at 08:44 for a
    title that contradicts its indicator (chart-level) and again a second later for a step in the
    world series (data-level). Whoever opens the first post is looking at the chart the second one
    is about, so the second is at best a duplicate — "the same chart should not be popping multiple
    times for different issues".

    The key carries nothing of the claim itself, and nothing of its level either. It used to carry
    the claim — first the claim's significant words, then a word-overlap comparison against the
    claims already posted — and the model rewords a finding freely enough that the same defect came
    back as new the day after it was ticked. What is remembered now is only that this chart or
    indicator was posted, and when.
    """
    indicators = _indicators(facts, slug) if str(issue.get("kind", "")) == "data" else []
    return [*indicators, slug]


def load_state() -> dict[str, str]:
    """When each chart or indicator was last posted, as ``{key: "YYYY-MM-DD"}``."""
    if not STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(STATE_PATH.read_text())
    except json.JSONDecodeError:
        return {}
    return _upgrade(raw) if isinstance(raw, dict) else {}


def _strip_kind(key: str) -> str:
    """A key as :func:`_dedup_keys` writes it now, from any key an older run wrote.

    Every earlier format ended the key with the finding's level, and the level is no longer part
    of identity — a chart posted yesterday over its data has to be recognised today when the
    finding is about its text.
    """
    slug, sep, kind = key.rpartition(":")
    return slug if sep and kind in {"data", "chart"} else key


def _upgrade(raw: dict[str, Any]) -> dict[str, str]:
    """Read the three older state formats as well as the current one.

    The file on the runner is the only record of what the channel has seen, so none is dropped.
    The first format put the claim's words in the key — ``<key>:<eight-words>`` mapped to a date;
    the second mapped the key to a list of ``{"words", "date"}`` claims; the third dropped the
    claim but kept the finding's level, as ``<key>:data`` or ``<key>:chart``. All reduce to the
    same thing here: the latest date anything was posted under the key.
    """
    state: dict[str, str] = {}

    def seen(key: str, day: str) -> None:
        key = _strip_kind(key)
        if key and day:
            state[key] = max(state.get(key, ""), day)

    for key, value in raw.items():
        if isinstance(value, list):
            for claim in value:
                seen(str(key), str(claim.get("date", "")) if isinstance(claim, dict) else "")
        elif isinstance(value, str) and str(key).count(":") >= 2:
            seen(str(key).rpartition(":")[0], value)
        elif isinstance(value, str):
            seen(str(key), value)
    return state


def save_state(state: dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=1, sort_keys=True))


def _today() -> date:
    return datetime.now(timezone.utc).date()


def new_findings(
    results: list[dict[str, Any]],
    state: dict[str, str],
    facts: dict[str, dict[str, Any]] | None = None,
    today: date | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Findings on charts and indicators not posted in the last :data:`COOLDOWN_DAYS`, best first.

    Deduplicated by indicator for data-level findings and by chart for chart-level ones — pass
    ``facts`` from :func:`chart_facts` to get the former; without it everything falls back to
    per-chart, which over-reports.

    One finding per chart or indicator, in the run as well as across days: a chart that is
    already up for review does not need a second message saying so, and the best finding on it
    (highest severity, then most read) is the one that goes. A second, different problem on the
    same chart waits for the cooldown to pass — a smaller cost than what this replaced, where the
    same problem reworded came back the morning after it was ticked.
    """
    day = today or _today()
    cutoff = (day - timedelta(days=COOLDOWN_DAYS)).isoformat()
    posted = {key: when for key, when in state.items() if when >= cutoff}
    out: list[tuple[dict[str, Any], dict[str, Any]]] = []
    order = {"high": 0, "medium": 1, "low": 2}
    ranked = sorted(
        ((r, i) for r in results for i in r["issues"]),
        key=lambda ri: (order.get(ri[1].get("severity", "low"), 3), -ri[0].get("views", 0)),
    )
    for result, issue in ranked:
        keys = _dedup_keys(result["slug"], issue, facts)
        # A match under any one key counts as already-known: the same indicator on a second chart
        # is not news, even though the two charts are not the same chart.
        if any(key in posted for key in keys):
            continue
        for key in keys:
            posted[key] = day.isoformat()
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


def _thumbnail(url: str) -> str:
    """The chart's own render, as a bare URL for Slack to unfurl into the message.

    The one thing the digest lacked that every other #chart-reviews post has. A claim about a
    chart is adjudicated by looking at the chart, and a link alone makes everybody click.

    Built from the finding's URL rather than from the slug, so the picture is the *view* the claim
    is about — an mdim view, a country selection, the map tab — and not the chart's default. The
    thumbnail endpoint honours grapher query parameters, so this renders what the model was shown.
    """
    base, _, params = url.partition("?")
    slug = base.rsplit("/", 1)[-1]
    return f"{GRAPHER_URL}/thumbnail/{slug}.png" + (f"?{params}" if params else "")


def _evidence(issue: dict[str, Any], model: str) -> str:
    """The argument behind the claim, as a thread reply.

    ``evidence`` and ``reader_impact`` are two of the five things the model is asked for on every
    finding, and the digest used to compute both and post neither: they were visible only in the
    HTML report, which nobody in the channel opens. They are exactly what someone needs in order
    to *disagree* with a claim, so they belong one click away rather than nowhere.

    In the thread rather than in the message, which is what the channel's own daily post does and
    what keeps the message scannable: the parent is the decision, the reply is the argument, and
    only someone who disputes the claim has to read the argument.

    Confidence and the model ride along here for the same reason. They are per-finding facts —
    how sure the model was, and which model it was — that a lead message posted once cannot carry,
    and they are what makes "advisory only" a statement rather than a disclaimer.
    """
    lines: list[str] = []
    if evidence := issue.get("evidence"):
        lines += ["*Evidence*", str(evidence)]
    if impact := issue.get("reader_impact"):
        lines += ["*A reader would conclude*", str(impact)]
    lines.append(
        f"_Advisory only · {issue.get('confidence', 'unknown')} confidence · generated by "
        f"{model or 'an LLM'} from the chart, its metadata and its values._"
    )
    return "\n".join(lines)


def _format_finding(
    result: dict[str, Any],
    issue: dict[str, Any],
    facts: dict[str, dict[str, Any]] | None = None,
    model: str = "",
) -> DigestMessage:
    """One finding as its own message, in the shape the channel already uses: the chart's title,
    the chart, a row of actions, the question being asked and who is being asked it, the claim,
    the facts in a blockquote, and the tick that closes it — with the evidence in the thread.

    Self-contained on purpose. It is read next to the lead message but it is also what someone
    quotes, forwards or replies to on its own, so it carries the link, the edit action and — the
    part that would otherwise be left behind in the lead — what the claim is actually worth.
    """
    chart = (facts or {}).get(result["slug"]) or {}
    url = issue.get("url") or f"{GRAPHER_URL}/{result['slug']}"
    lines = [_chart_title(result), _thumbnail(url)]

    # A multi-dim view has no chart id, so it gets no edit link — there is no single chart to
    # edit. The live link is always there, and is what the tick is taken against.
    actions = [f":chart_with_upwards_trend:  <{url}|View live chart>"]
    if chart_id := chart.get("chart_id"):
        actions.append(f":pencil2:  <{ADMIN_URL}/admin/charts/{chart_id}/edit|Edit in admin>")
    lines.append("      ".join(actions))

    # The ask, and who is being asked. Near the top rather than in the footer: a finding whose
    # only mention sits at the end of a middot-separated footer does not read as addressed to
    # anyone, and a finding addressed to nobody gets read and forgotten.
    editor = chart.get("editor_mention")
    lines.append(ASK + (f"   {editor}   ·   _You last edited this chart._" if editor else ""))

    lines.append(issue.get("claim", "").rstrip(".") + ".")

    dot = SEVERITY_DOT.get(issue.get("severity", "low"), ":large_yellow_circle:")
    lines.append(f"> *Severity:*  {dot} {issue.get('severity', 'low')}  ·  {issue.get('kind', 'chart')}-level")
    if views := format_views(result.get("views")):
        lines.append(f"> *Readership:*  {views}")
    lines.append(f"{TICK}   ·   {CAVEAT}")
    return DigestMessage("\n".join(lines), [_evidence(issue, model)])


def format_slack(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    reviewed: int,
    candidates: int,
    incomplete: int = 0,
    window_days: int | None = None,
    facts: dict[str, dict[str, Any]] | None = None,
    cost: float = 0.0,
    model: str = "",
) -> list[DigestMessage]:
    """The digest as separate Slack mrkdwn messages — single asterisks for bold, in the shape
    #chart-reviews uses.

    A lead message saying what was swept and what it cost, then **one message per finding**. The
    findings used to be one message, which gave the whole digest a single thread: every reply
    about one chart landed in the same place as the replies about the others, and a fix on one
    could not be acknowledged without noise for the rest. One message each gives every claim its
    own thread, which is where the adjudication belongs. The lead stays separate rather than
    riding on the first finding, so the first finding is not privileged.

    The lead is the one part with no counterpart in that channel, where each post stands alone.
    It earns its place by carrying what no single finding can: what the sweep looked at, how much
    of it was reviewed, and what that cost. Everything that *is* per-finding — the model, its
    confidence, the caveat — sits on the finding instead.
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
    spend = f"${cost:,.2f}" if cost >= 0.01 else "<$0.01"
    lead.append(
        f"_Posted by `etl chart-critic`. Reviewing {reviewed} chart{'s' if reviewed != 1 else ''} cost {spend}._"
    )
    if incomplete:
        lead.append(f"_{incomplete} chart(s) could not be reviewed, so treat this as incomplete._")

    return [DigestMessage("\n".join(lead))] + [_format_finding(result, issue, facts, model) for result, issue in shown]


def stamp(
    findings: list[tuple[dict[str, Any], dict[str, Any]]],
    state: dict[str, str],
    facts: dict[str, dict[str, Any]] | None = None,
    today: date | None = None,
) -> dict[str, str]:
    """Record what was posted. ``facts`` must be the same mapping :func:`new_findings` used —
    a finding keyed per-chart here and looked up per-indicator tomorrow matches nothing, and the
    digest would re-post every finding every day."""
    day = (today or _today()).isoformat()
    for result, issue in findings[:MAX_FINDINGS]:
        for key in _dedup_keys(result["slug"], issue, facts):
            state[key] = day
    return state
