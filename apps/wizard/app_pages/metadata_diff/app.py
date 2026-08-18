"""Metadata Diff: review how this branch changes the metadata texts readers see.

Unlike the config diff in chart-diff, this compares the *rendered* texts — indicator metadata merged
with any view-level overrides — so it catches edits authored in garden steps, which never appear in a
config diff. Titles, subtitles, footnotes, `description_short` and WYSK / `description_key`.

The page opens on the changes. Three sections, same order and icons as Chart Diff, each carrying a count
so you can see at a glance where this branch landed:

- **Charts** — indicator texts that changed, and the published charts that render them.
- **MDims** — MDims whose view texts changed, linking into the per-MDim Blast radius / View diff / Review.
- **Explorers** — published explorer views whose resolved text changed.

There is one baseline, resolved the way every other diff in the wizard resolves it (production where this
server has production credentials, `staging-site-master` otherwise). The old "Compare against" choice is
gone: it asked reviewers a question no other diff asks, and got in the way of simply seeing the changes.
"""

import streamlit as st
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.utils import WARN_MSG, get_engines
from apps.wizard.app_pages.metadata_diff import cached, charts_section, explorers_section, mdims_section
from apps.wizard.app_pages.metadata_diff.render import BASELINE_NAME
from apps.wizard.utils.components import st_title_with_expert, url_persist
from etl.config import OWID_ENV

log = get_logger()

st.set_page_config(
    page_title="Wizard: Metadata Diff",
    page_icon="🪄",
    layout="wide",
    initial_sidebar_state="collapsed",
)

SECTIONS = {
    "charts": (":material/show_chart:", "Charts"),
    "mdims": (":material/dashboard:", "MDims"),
    "explorers": (":material/explore:", "Explorers"),
}


def _section_label(section: str, counts: dict[str, int]) -> str:
    """Section label with its change count — the count is the point, so it is never hidden."""
    icon, name = SECTIONS[section]
    n = counts.get(section, 0)
    return f"{icon} {name} ({n})"


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
    counts = {
        "charts": summary.n_charts,
        "mdims": summary.n_mdims,
        "explorers": summary.n_explorers,
    }
    for warning in summary.warnings:
        st.warning(warning)

    if not summary.has_changes and not summary.warnings:
        st.success(f"**No metadata text changes** on this staging server against `{BASELINE_NAME}`.")

    section = url_persist(st.segmented_control)(
        label="Section",
        options=list(SECTIONS),
        format_func=lambda s: _section_label(s, counts),
        key="diff-type",
        value="charts",
        label_visibility="collapsed",
    )

    if section == "mdims":
        mdims_section.st_show_mdim_metadata_diffs(source_engine, target_engine)
    elif section == "explorers":
        explorers_section.st_show_explorer_metadata_diffs(source_engine, target_engine)
    else:
        charts_section.st_show_chart_metadata_diffs(source_engine, target_engine)


main()
