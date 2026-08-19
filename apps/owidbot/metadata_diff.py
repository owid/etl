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

from apps.wizard.app_pages.metadata_diff.discovery import Summary, summarize
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
    """The body of the comment section: what changed, and how far it reaches."""
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
    if summary.n_charts or summary.n_indicators:
        indicators = f" (from {summary.n_indicators} indicator{'s' if summary.n_indicators != 1 else ''})"
        items.append(f"<li>Charts: {summary.n_charts}{indicators}</li>")
    if summary.n_mdims:
        # A count we could not resolve view by view is a ceiling, and says so rather than overstating.
        qualifier = "" if summary.mdims_resolved else " (flagged; too many to resolve view by view)"
        items.append(f"<li>MDims: {summary.n_mdims}{qualifier}</li>")
    if summary.n_explorers:
        items.append(f"<li>Explorers: {summary.n_explorers} ({summary.n_explorer_views} views)</li>")
    if summary.fields:
        fields = ", ".join(f"{label} ({n})" for label, n in sorted(summary.fields.items()))
        items.append(f"<li>Fields: {fields}</li>")
    if summary.n_new_indicators:
        # A version bump makes every indicator new, so there is nothing to diff — and nothing has been
        # read either. Say that, rather than let an empty diff pass for an empty change.
        items.append(
            f"<li>New indicators: {summary.n_new_indicators} — absent from the baseline, so their text "
            "has no diff and is unreviewed</li>"
        )
    # Changes that match master's own server are master's work the baseline has not rebuilt yet.
    from_master = summary.attribution.get("master", 0)
    if from_master:
        items.append(
            f"<li>{from_master} of the changed indicators match master's server — master's edits the "
            "baseline has not rebuilt yet, not this branch's</li>"
        )
    unknown = summary.attribution.get("unknown", 0)
    if unknown:
        items.append(
            f"<li>❔ {unknown} could not be attributed (master's server was unreachable), so some of them "
            "may be master's rather than this branch's</li>"
        )
    if summary.n_other:
        items.append(
            f"<li>{summary.n_other} further MDim/explorer difference(s) are baseline lag, not this branch</li>"
        )
    if not summary.narrowed:
        items.append("<li>⚠️ Could not narrow to this branch's files — may include changes from master</li>")
    for warning in summary.warnings:
        items.append(f"<li>⚠️ {warning}</li>")

    return f"<ul>{''.join(items)}</ul>"


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
