"""Metadata Diff: review how this branch changes the metadata texts readers see.

Unlike the config diff in chart-diff, this compares the *rendered* texts — indicator metadata merged
with any view-level overrides — so it catches edits authored in garden steps, which never appear in a
config diff. Titles, subtitles, footnotes, `description_short` and WYSK / `description_key`.

The page opens on the changes. Three sections, same order and icons as Chart Diff, each carrying a count
so you can see at a glance where this branch landed:

- **Blast radius** — everywhere the branch's edits land, across all three surfaces, by change or by
  affected page. The one view that crosses surfaces on purpose.
- **Charts** — indicator texts that changed, and the published charts that render them.
- **MDims** — MDims whose view texts changed, view by view or by the edit that changed them.
- **Explorers** — published explorer views whose resolved text changed.
- **Review** — the ticks and notes of a pass, and two markdown documents to paste elsewhere.

Each of the three surface sections reads **view by view** — one chart, MDim view or explorer view at a
time, with its own tick and note — or **by edit**, one authored change per card with everywhere it lands
under it. They are two ways through the same changes, not two halves of a job: ticking through either
finishes the section, and its badge in the bar follows whichever one is being used.

There is one baseline, resolved the way every other diff in the wizard resolves it (production where this
server has production credentials, `staging-site-master` otherwise). The old "Compare against" choice is
gone: it asked reviewers a question no other diff asks, and got in the way of simply seeing the changes.
"""

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.utils import WARN_MSG, get_engines
from apps.wizard.app_pages.metadata_diff import (
    blast_section,
    cached,
    charts_section,
    explorers_section,
    mdims_section,
    review_section,
)
from apps.wizard.app_pages.metadata_diff.core import empty_sections, foreign_params, section_progress
from apps.wizard.app_pages.metadata_diff.data import DECIDED, load_item_notes
from apps.wizard.app_pages.metadata_diff.discovery import keep_sections
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    st_section_switcher,
    st_stale_server_banner,
)
from apps.wizard.app_pages.metadata_diff.review_state import verdict_counts
from apps.wizard.utils.components import st_title_with_expert
from etl.config import OWID_ENV

log = get_logger()

st.set_page_config(
    page_title="Wizard: Metadata Diff",
    page_icon="🪄",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def _review_marks(source_engine, target_engine) -> dict[str, str]:
    """Per section: every item decided, or not yet.

    Done is along whichever layout the reviewer took — every view ticked view by view, *or* every edit
    ticked by edit. A section is reviewed one way or the other, so the bar has to agree with the way that
    was picked rather than demand both; `section_progress` makes that call, and the Review tab reads it too.

    Lazy on purpose. With nothing ticked nothing can be finished, so a fresh page marks every section ⏳
    without enumerating — and enumerating means diffing every changed view of every changed MDim. The
    totals are asked for only once something has been ticked, by which point those caches are warm from
    the reading.
    """
    # Either verdict is a decision: a section every one of whose edits was rejected has been gone
    # through, and telling its reviewer it is unfinished would be wrong.
    decided = [row for row in load_item_notes(source_engine) if row.get("status") in DECIDED]

    marks = {section: "todo" for section in ("charts", "mdims", "explorers")}
    if not decided:
        return marks

    index, totals = cached.item_index(source_engine, target_engine)
    # The index is what makes the count mean anything, and it used to be fetched and thrown away: a row
    # keeps its status when its text is rewritten, and it keeps it when its item leaves the comparison
    # altogether. Counting either put a ✅ on a section holding something nobody has read.
    ticked: dict[str, int] = {}
    for row in decided:
        if not verdict_counts(row, index):
            continue
        surface = str(row.get("catalogPath") or "")
        ticked[surface] = ticked.get(surface, 0) + 1

    for section, (done, total) in section_progress(ticked, totals).items():
        if total and done >= total:
            marks[section] = "done"
    return marks


def _section_totals(summary) -> dict[str, tuple[int, int]]:
    """(0, total) per section — how many distinct changes each one holds.

    The first element used to be how many were ticked, read fresh on every rerun so a toggle moved the
    counter above it. Sign-off is out of the UI for now, so nothing is ticked and nothing queries for it;
    the totals stay, because they are what decides whether a section has anything in it at all.
    """
    return {section: (0, len(entries)) for section, entries in summary.review_keys.items()}


def main() -> None:
    assert OWID_ENV.env_remote != "production", "Metadata Diff must run on a staging server, not production."
    source_engine, target_engine = get_engines()

    st_title_with_expert(
        title="Metadata Diff",
        icon=":material/difference:",
        help=f"""
**Metadata Diff** compares the metadata texts end users see — chart titles, subtitles and footnotes,
`description_short`, and *What you should know about this data* — between your
[`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment and `{BASELINE_NAME}`.

It resolves the text the way the site does, so it catches changes coming from **garden step metadata**
(including Jinja templates and shared `definitions`), which a config diff cannot see.

Each surface section reads **view by view** or **by edit** — one page at a time, or one authored change
with everywhere it lands. Either way takes a tick and a note, and either one finishes the section.

Nothing here is synced on merge: metadata ships through ETL when your PR merges. Use this to check that
what ships is what you meant, and to see how far each change reaches.
""",
    )

    if WARN_MSG:
        st.warning("- " + "\n\n- ".join(WARN_MSG))

    summary = cached.summary(source_engine, target_engine)
    progress = _section_totals(summary)
    for warning in summary.warnings:
        st.warning(warning)

    # Before any count: if this server is behind on a dataset, the counts below are about that, not the branch.
    st_stale_server_banner(summary.stale)

    if not summary.has_changes and not summary.warnings:
        st.success(f"**No metadata text changes** on this staging server against `{BASELINE_NAME}`.")

    # A zero badge is only trustworthy when the lookup behind it worked; `keep_sections` says which zeros
    # are silences rather than findings, and those sections stay reachable.
    section = st_section_switcher(
        progress,
        empty_sections(progress, keep_sections(summary)),
        _review_marks(source_engine, target_engine),
    )

    # The trail a reviewer leaves behind them. Pruned after the section is resolved and before it renders,
    # so the page's own widgets write theirs back on the same run and only the other sections' go.
    for stale in foreign_params(section, list(st.query_params.keys())):
        st.query_params.pop(stale, None)

    if section == "review":
        review_section.st_show_review(source_engine, target_engine)
    elif section == "blast":
        blast_section.st_show_blast_radius(source_engine, target_engine)
    elif section == "mdims":
        mdims_section.st_show_mdim_metadata_diffs(source_engine, target_engine)
    elif section == "explorers":
        explorers_section.st_show_explorer_metadata_diffs(source_engine, target_engine)
    else:
        charts_section.st_show_chart_metadata_diffs(source_engine, target_engine)


main()
