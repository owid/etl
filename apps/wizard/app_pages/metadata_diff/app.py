"""Metadata Diff: review how this branch changes the metadata texts readers see.

Unlike the config diff in chart-diff, this compares the *rendered* texts — indicator metadata merged
with any view-level overrides — so it catches edits authored in garden steps, which never appear in a
config diff. Titles, subtitles, footnotes, `description_short` and WYSK / `description_key`.

The page opens on the changes. Three sections, same order and icons as Chart Diff, each carrying a count
so you can see at a glance where this branch landed:

- **Blast radius** — everywhere the branch's edits land, across all three surfaces, by change or by
  affected page. The one view that crosses surfaces on purpose.
- **Charts** — indicator texts that changed, and the published charts that render them.
- **MDims** — MDims whose view texts changed, each with its changes inline and a PR brief to download.
- **Explorers** — published explorer views whose resolved text changed.

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
)
from apps.wizard.app_pages.metadata_diff.core import COUNTED_SECTIONS, empty_sections
from apps.wizard.app_pages.metadata_diff.data import count_ticked
from apps.wizard.app_pages.metadata_diff.render import (
    BASELINE_NAME,
    st_section_switcher,
    st_stale_server_banner,
)
from apps.wizard.utils.components import st_title_with_expert
from etl.config import OWID_ENV

log = get_logger()

st.set_page_config(
    page_title="Wizard: Metadata Diff",
    page_icon="🪄",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def _review_progress(source_engine, summary) -> dict[str, tuple[int, int]]:
    """(ticked, total) per section, with the ticks read fresh.

    `summary` is cached for minutes, which is right for what changed and wrong for what has been reviewed:
    pressing a toggle has to move the counter above it in the same rerun.
    """
    return {
        section: (count_ticked(source_engine, entries), len(entries))
        for section, entries in summary.review_keys.items()
    }


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

Nothing here is synced on merge: metadata ships through ETL when your PR merges. Use this to check that
what ships is what you meant, and to see how far each change reaches.
""",
    )

    if WARN_MSG:
        st.warning("- " + "\n\n- ".join(WARN_MSG))

    summary = cached.summary(source_engine, target_engine)
    progress = _review_progress(source_engine, summary)
    for warning in summary.warnings:
        st.warning(warning)

    # Before any count: if this server is behind on a dataset, the counts below are about that, not the branch.
    st_stale_server_banner(summary.stale)

    if not summary.has_changes and not summary.warnings:
        st.success(f"**No metadata text changes** on this staging server against `{BASELINE_NAME}`.")

    # A zero badge is only trustworthy when the lookup behind it worked. Any warning at all keeps every
    # section reachable — a surface that failed to load reads as "nothing here" otherwise — and so do new
    # indicators, which the Charts section reports even though they are not reviewable changes.
    keep = set(COUNTED_SECTIONS) if summary.warnings else set()
    if not summary.mdims_resolved:
        keep.add("mdims")
    if summary.n_new_indicators:
        keep.add("charts")

    section = st_section_switcher(progress, empty_sections(progress, keep))

    if section == "blast":
        blast_section.st_show_blast_radius(source_engine, target_engine)
    elif section == "mdims":
        mdims_section.st_show_mdim_metadata_diffs(source_engine, target_engine)
    elif section == "explorers":
        explorers_section.st_show_explorer_metadata_diffs(source_engine, target_engine)
    else:
        charts_section.st_show_chart_metadata_diffs(source_engine, target_engine)


main()
