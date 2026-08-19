"""owidbot's metadata-diff report: say in the PR comment that reader-facing text changed.

Metadata edits are easy to merge unreviewed. They don't show up in chart-diff (which compares chart
configs, while most text is authored in garden steps) and data-diff reports the values, not the words.
So a PR can change what every chart of a dataset says about itself with nothing in the comment to
suggest anyone should look.

This posts the counts and links to the tool. Deliberately no GitHub check run: metadata changes gate
nothing on merge — unlike chart-diff approvals, which gate `etl chart-sync` — so a failing check would
be noise rather than a signal.
"""

from structlog import get_logger

from apps.wizard.app_pages.metadata_diff.discovery import Summary, summarize
from etl.config import OWIDEnv, get_container_name
from etl.db import production_or_master_engine

log = get_logger()


def call_metadata_diff(branch: str) -> Summary:
    """Compare the branch's staging server against production (or master, when production is unavailable)."""
    source_engine = OWIDEnv.from_staging(branch).get_engine()
    target_engine = production_or_master_engine()
    return summarize(source_engine, target_engine)


def status_icon(summary: Summary) -> str:
    if summary.warnings:
        return "⚠️"
    return "✏️" if summary.has_changes else "✅"


def format_metadata_diff(summary: Summary) -> str:
    """The body of the comment section: what changed, and how far it reaches."""
    if summary.warnings and not summary.has_changes:
        items = "".join(f"<li>{w}</li>" for w in summary.warnings)
        return f"<ul>{items}</ul>"

    if not summary.has_changes:
        return "No metadata text changes."

    items = []
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
    # A difference in a dataset master also edited after this server was forked is not purely this PR's.
    shared = summary.attribution.get("mixed", 0) + summary.attribution.get("baseline_newer", 0)
    if shared:
        items.append(
            f"<li>⚠️ {shared} of them are in datasets the baseline also changed since this server was "
            "created — rebuild on master to tell them apart</li>"
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
