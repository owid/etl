"""owidbot's metadata-diff report: say in the PR comment that reader-facing text changed.

Metadata edits are easy to merge unreviewed. They don't show up in chart-diff (which compares chart
configs, while most text is authored in garden steps) and data-diff reports the values, not the words.
So a PR can change what every chart of a dataset says about itself with nothing in the comment to
suggest anyone should look.

This posts the counts and links to the tool. Deliberately no GitHub check run: metadata changes gate
nothing on merge — unlike chart-diff approvals, which gate `etl chart-sync` — so a failing check would
be noise rather than a signal.
"""

from sqlalchemy.engine.base import Engine
from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.discovery import Summary, affected_pages, group_by_edit, summarize
from etl.config import OWIDEnv, get_container_name
from etl.db import production_or_master_engine

log = get_logger()


def call_metadata_diff(branch: str) -> Summary:
    """Compare the branch's staging server against production (or master, when production is unavailable)."""
    source_engine = OWIDEnv.from_staging(branch).get_engine()
    target_engine = production_or_master_engine()
    return summarize(source_engine, target_engine, _master_engine(target_engine))


def _master_engine(target_engine: Engine) -> Engine | None:
    """Master's own staging server, which is what tells this branch's edits from master's.

    None when it is unreachable, or when the baseline already is that server — cross-checking a server
    against itself answers nothing. Attribution then reports "unknown" rather than guessing.
    """
    try:
        env = OWIDEnv.from_staging("master")
        engine = env.get_engine()
        if engine.url == target_engine.url:
            return None
        return engine
    except Exception as e:  # noqa: BLE001 — a missing master server must not stop the comment
        log.warning("owidbot.metadata_diff.master_engine_unavailable", error=str(e))
        return None


def status_icon(summary: Summary) -> str:
    # A stale server outranks the change count: until it is rebuilt, that count is about the wrong thing.
    if summary.stale:
        return "🚧"
    if summary.warnings:
        return "⚠️"
    return "✏️" if summary.has_changes else "✅"


def format_metadata_diff(summary: Summary) -> str:
    """The body of the comment section: one line of reach, plus whatever undermines it.

    Deliberately short. This block sits in a comment beside chart-diff and data-diff, and its job there is
    to say "metadata changed, roughly this much, go and look" — twelve bullets of per-surface counts made
    the comment longer without making that decision easier, and the detail is one click away on the page.

    What stays is what a reader cannot get from the counts: a stale server (which makes every number below
    it read backwards), indicators with no baseline text to diff, changes that are master's rather than
    this branch's, and anything the tool could not establish. Those are not detail; they are the reasons
    a number here might be wrong.

    Draft charts and unpublished MDim views are counted by `affected_pages` and deliberately not reported
    here. Nothing a reader can open is affected by them, so they do not bear on the "go and look" this
    block exists to prompt, and they are listed on the page for whoever does. The published counts exclude
    them either way, so leaving them out understates nothing.
    """
    # Lead with a stale server. Its differences read backwards — the branch appears to have written text
    # it removed — so every number below it is untrustworthy until the datasets are rebuilt.
    stale = ""
    if summary.stale:
        names = ", ".join(f"<code>{d}</code>" for d in sorted(summary.stale))
        stale = (
            f"<li>🚧 <b>This staging server is behind on {len(summary.stale)} dataset(s)</b> ({names}) — "
            "their differences below show older text as this branch's change. Rebuild them on the server "
            "(<code>etlr grapher://grapher/&lt;dataset&gt; --grapher</code>) and re-run.</li>"
        )

    if summary.warnings and not summary.has_changes:
        items = stale + "".join(f"<li>{w}</li>" for w in summary.warnings)
        return f"<ul>{items}</ul>"

    if not summary.has_changes:
        return f"<ul>{stale}</ul>" if stale else "No metadata text changes."

    items = [stale] if stale else []
    line = _reach_line(summary)
    if line:
        items.append(f"<li>{line}</li>")

    # Only what makes the line above less than the whole truth.
    if summary.n_new_indicators:
        # A version bump used to make every indicator new; the comparison now looks past the version, so
        # what is left here is genuinely absent from the baseline and has no old text to read.
        items.append(
            f"<li>New indicators: {summary.n_new_indicators} — absent from the baseline, so their text "
            "has no diff and is unreviewed</li>"
        )
    # Which count is a ceiling, not merely that one is: the two budgets are separate, so the published
    # count can be exact while the drafts overflowed, and a reader deciding whether to trust a number
    # needs to know which number.
    capped = [
        name
        for name, resolved in (("MDim", summary.mdims_resolved), ("unpublished-MDim", summary.draft_mdims_resolved))
        if not resolved
    ]
    if capped:
        which = " and ".join(capped)
        items.append(
            f"<li>The {which} count{'s are' if len(capped) > 1 else ' is'} a ceiling — too many to "
            "resolve view by view</li>"
        )
    from_master = summary.attribution.get("master", 0)
    if from_master:
        items.append(f"<li>{from_master} of these match master's server — master's edits, not this branch's</li>")
    unknown = summary.attribution.get("unknown", 0)
    if unknown:
        items.append(f"<li>❔ {unknown} could not be attributed — master's server was unreachable</li>")
    if not summary.narrowed:
        items.append("<li>⚠️ Could not narrow to this branch's files — may include changes from master</li>")
    for warning in summary.warnings:
        items.append(f"<li>⚠️ {warning}</li>")

    return f"<ul>{''.join(items)}</ul>"


def _reach_line(summary: Summary) -> str:
    """ "✏️ **6 edits** → 67 charts, 30 MDim views, 402 explorer views".

    Which fields were edited is deliberately not here. It read as a fourth list in a line that already
    carries two, and unlike the counts it answers nothing a reviewer decides from the comment — the
    edits are on the page, each with its own field label and its own diff.

    Edits on the left, pages on the right, and both sides counted once. Pages are the unit because they
    are the same unit on every surface — a chart, an MDim view and an explorer view are each something
    somebody opens — where the Summary's own numbers are not addable: `n_charts` and `n_charts_own_text`
    are overlapping sets of the same charts, and MDims were counted as MDims beside explorers counted as
    views. Only the reach can dedupe that, so a summary built from counts alone falls back to naming the
    surface counts separately rather than implying a total.
    """
    # Pages, in one unit: a chart, an MDim view and an explorer view are each something somebody opens.
    # Deduped from the reach, which also collapses the two overlapping chart counts the Summary carries
    # into the one honest number — they could never be added, and reporting both read as arithmetic.
    if summary.reach:
        pages = affected_pages(summary.reach)
        reach = [
            f"{pages[key]} {label}{'' if pages[key] == 1 else 's'}"
            for key, label in (("charts", "chart"), ("mdim_views", "MDim view"), ("explorer_views", "explorer view"))
            if pages[key]
        ]
    else:
        # A summary assembled from counts alone (no reach): the surface counts, kept apart because the two
        # chart numbers are overlapping sets and nothing here can dedupe them.
        reach = []
        if summary.n_charts:
            reach.append(f"{summary.n_charts} chart{'s' if summary.n_charts != 1 else ''}")
        if summary.n_charts_own_text:
            n = summary.n_charts_own_text
            reach.append(f"{n} chart{'s' if n != 1 else ''} via their own config")
        if summary.n_mdims:
            reach.append(f"{summary.n_mdims} MDim{'s' if summary.n_mdims != 1 else ''}")
        if summary.n_draft_mdims:
            n = summary.n_draft_mdims
            reach.append(f"{n} unpublished MDim{'s' if n != 1 else ''}")
        if summary.n_explorers:
            views = summary.n_explorer_views
            reach.append(
                f"{summary.n_explorers} explorer{'s' if summary.n_explorers != 1 else ''} "
                f"({views} view{'s' if views != 1 else ''})"
            )

    # **Edits**, not rendered texts. One reworded subtitle reaches 348 explorer views, each wording it
    # differently, and reporting 384 "text changes" for six authored edits overstates the work by sixty
    # times — which is the error the by-edit grouping exists to avoid. `group_by_edit` is the page's own
    # grouping and is pure, so this costs nothing. Where reach was never built (a summary assembled from
    # counts alone) the per-field tally stands in.
    n_edits = len(group_by_edit(summary.reach)) if summary.reach else sum(summary.fields.values())

    # ✏️ carries the same meaning here as the status icon on the `<summary>` line and as chart-diff's:
    # this is the edit line. It also keeps the line from opening on a bare digit.
    head = f"✏️ <b>{n_edits} edit{'s' if n_edits != 1 else ''}</b>" if n_edits else ""
    if not head:
        return ", ".join(reach)
    return f"{head} → {', '.join(reach)}" if reach else f"{head} — nothing published renders them yet"


def run(branch: str, summary: Summary) -> str:
    """The `<details>` block owidbot splices into its PR comment."""
    container_name = get_container_name(branch) if branch else "dry-run"

    body = f"""
<details open>
<summary><a href="http://{container_name}/etl/wizard/metadata-diff"><b>metadata-diff</b></a>: {status_icon(summary)}</summary>
{format_metadata_diff(summary)}
</details>
    """.strip()

    return body
