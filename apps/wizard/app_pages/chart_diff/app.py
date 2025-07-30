"""
TODO: Add sorting mechanism to the chart diff list.

- get_chart_diffs: Right after when we get charts, sort them based on what is the value in the chart-diff-sort-by-results selectbox.
- filter_chart_diffs: Add sorting whenever the user edits anything in the options popover?
- x Question: How to reconcile selectbox/URL Query params?
- At which stage should we get the analytics / anomalist scores? In ChartDiff.from_charts_df! (look for "TODO")
"""

import re
from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

# from st_copy_to_clipboard import st_copy_to_clipboard
from structlog import get_logger

from apps.wizard.app_pages.chart_diff.chart_diff import get_chart_diffs_from_grapher
from apps.wizard.app_pages.chart_diff.chart_diff_show import st_show
from apps.wizard.app_pages.chart_diff.utils import WARN_MSG, get_engines, indicators_in_charts
from apps.wizard.utils import set_states
from apps.wizard.utils.components import (
    Pagination,
    st_horizontal,
    st_title_with_expert,
    st_wizard_page_link,
    url_persist,
)
from etl.config import FORCE_DATASETTE, OWID_ENV
from etl.grapher import model as gm

log = get_logger()

# Config
st.set_page_config(
    page_title="Wizard: Chart Diff",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
    menu_items={
        "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
    },
)

########################################################################################################################
if FORCE_DATASETTE:
    st.warning(
        "Metabase credentials not found (using Datasette as fallback). To stop seeing this warning, set Metabse credentials in your .env file."
    )
########################################################################################################################

# Paths
CURRENT_DIR = Path(__file__).resolve().parent

# DB access
# Create connections to DB
SOURCE_ENGINE, TARGET_ENGINE = get_engines()


# Sorting options
class SortMethods:
    relevance: str = "relevance"
    chart_views_most_to_least: str = "chart_views_most_to_least"
    chart_views_least_to_most: str = "chart_views_least_to_most"
    articles_most_to_least: str = "articles_most_to_least"
    articles_least_to_most: str = "articles_least_to_most"
    anomalies_most_to_least: str = "anomalies_most_to_least"
    anomalies_least_to_most: str = "anomalies_least_to_most"
    last_updated: str = "last_updated"


SORTING_METHODS = {
    SortMethods.relevance: "Relevance",
    SortMethods.chart_views_most_to_least: "Chart views (last 30-day): Most to least",
    SortMethods.chart_views_least_to_most: "Chart views (last 30-day): Least to most",
    SortMethods.articles_most_to_least: "Articles (last 30-day): Most to least",
    SortMethods.articles_least_to_most: "Articles (last 30-day): Least to most",
    SortMethods.anomalies_most_to_least: "Anomalies: Most to least",
    SortMethods.anomalies_least_to_most: "Anomalies: Least to most",
    SortMethods.last_updated: "Last updated",
}

SORTING_QUERY_PARAM = "chart-diff-sort-by-results"

########################################
# SESSION STATE
########################################
st.session_state.chart_diffs = st.session_state.get("chart_diffs", {})
st.session_state.arrange_charts_vertically = st.session_state.get("arrange_charts_vertically", False)
st.session_state.conflicts_resolved_text = st.session_state.get("conflicts_resolved_text", {})

########################################
# LOAD VARIABLES
########################################
CHART_PER_PAGE = 10
# WARN_MSG += ["This tool is being developed! Please report any issues you encounter in `#proj-new-data-workflow`"]

# if str(config.GRAPHER_USER_ID) != "1":
#     WARN_MSG.append(
#         "`GRAPHER_USER_ID` from your .env is not set to 1 (Admin user). Please modify your .env or use STAGING=1 flag to set it automatically. "
#         "All changes on staging servers must be done with Admin user."
#     )

if WARN_MSG:
    st.warning("- " + "\n\n- ".join(WARN_MSG))

########################################
# FUNCTIONS
########################################


def get_chart_diffs():
    """Get chart diffs."""
    # Get actual charts
    if st.session_state.chart_diffs == {}:
        with st.spinner("Getting charts from database...", show_time=True):
            st.session_state.chart_diffs = get_chart_diffs_from_grapher(SOURCE_ENGINE, TARGET_ENGINE)

    # Sort charts
    st.session_state.chart_diffs = dict(
        sorted(
            st.session_state.chart_diffs.items(),
            # put errors first
            key=lambda item: (item[1].error is not None, item[1].latest_update),
            reverse=True,
        )
    )

    # Get indicators used in charts
    st.session_state.indicators_in_charts = indicators_in_charts(
        SOURCE_ENGINE, list(st.session_state.chart_diffs.keys())
    )

    # Init, can be changed by the toggle
    st.session_state.chart_diffs_filtered = st.session_state.chart_diffs


def filter_chart_diffs():
    """Filter chart diffs to display.

    This is based on the query parameters.
    """

    # TODO: Add sorting too?
    def _slugs_match(chart_slug_1, chart_slug_2):
        pattern = r"[,\s\-]+"
        chart_slug_1 = set(re.split(pattern, chart_slug_1.lower()))
        chart_slug_2 = set(re.split(pattern, chart_slug_2.lower()))
        if chart_slug_1.intersection(chart_slug_2):
            return True
        return False

    # Show all charts regardless of query params
    if "show_all" in st.query_params:
        st.session_state.chart_diffs_filtered = {k: v for k, v in st.session_state.chart_diffs_filtered.items()}
    else:
        # Filter based on query params
        if "chart_id" in st.query_params:
            chart_ids = list(map(int, st.query_params.get_all("chart_id")))
            st.session_state.chart_diffs_filtered = {
                k: v for k, v in st.session_state.chart_diffs_filtered.items() if v.chart_id in chart_ids
            }
        if "indicator_id" in st.query_params:
            indicator_ids = list(map(int, st.query_params.get_all("indicator_id")))

            # Get all charts containing any of the selected indicators
            with Session(SOURCE_ENGINE) as session:
                chart_ids = gm.ChartDimensions.chart_ids_with_indicators(session, indicator_ids)

            st.session_state.chart_diffs_filtered = {
                k: v for k, v in st.session_state.chart_diffs_filtered.items() if v.chart_id in chart_ids
            }
        if "chart_slug" in st.query_params:
            chart_slug = st.query_params.get("chart_slug", "")

            st.session_state.chart_diffs_filtered = {
                k: v for k, v in st.session_state.chart_diffs_filtered.items() if _slugs_match(chart_slug, v.slug)
            }
        if "show_reviewed" not in st.query_params:
            st.session_state.chart_diffs_filtered = {
                k: v for k, v in st.session_state.chart_diffs_filtered.items() if not v.is_reviewed
            }
        if "modified_or_new" in st.query_params:
            modified_or_new = st.query_params.get_all("modified_or_new")
            st.session_state.chart_diffs_filtered = {
                k: v
                for k, v in st.session_state.chart_diffs_filtered.items()
                if (v.is_modified and "modified" in modified_or_new) or (v.is_new and "new" in modified_or_new)
            }
        if "change_type" in st.query_params:
            # keep chart diffs with at least one change type (could be data, metadata or config)
            change_types = st.query_params.get_all("change_type")
        else:
            # filter to changed config by default
            change_types = ["new", "config"]

        st.session_state.chart_diffs_filtered = {
            k: v
            for k, v in st.session_state.chart_diffs_filtered.items()
            if set(v.change_types) & set(change_types) or v.is_new
        }

    # SORT chart diffs
    sort_chart_diffs()

    # Return boolean if there was any filter applied (except for hiding approved charts)
    # Not really used!
    if (
        "chart_id" in st.query_params
        or "chart_slug" in st.query_params
        # or "modified_or_new" in st.query_params
        or "change_type" in st.query_params
    ):
        return True
    return False


def sort_chart_diffs():
    """Sort chart diffs."""
    sort_by = st.query_params.get(SORTING_QUERY_PARAM, SortMethods.relevance)
    if sort_by == SortMethods.relevance:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.relevance,
                reverse=True,
            )
        )
    elif sort_by == SortMethods.chart_views_most_to_least:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.chart_views,
                reverse=True,
            )
        )
    elif sort_by == SortMethods.chart_views_least_to_most:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.chart_views,
                reverse=False,
            )
        )
    elif sort_by == SortMethods.articles_most_to_least:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.num_articles,
                reverse=True,
            )
        )
    elif sort_by == SortMethods.articles_least_to_most:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.num_articles,
                reverse=False,
            )
        )
    elif sort_by == SortMethods.anomalies_most_to_least:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.anomaly,
                reverse=True,
            )
        )
    elif sort_by == SortMethods.anomalies_least_to_most:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].scores.anomaly,
                reverse=False,
            )
        )
    elif sort_by == SortMethods.last_updated:
        st.session_state.chart_diffs_filtered = dict(
            sorted(
                st.session_state.chart_diffs_filtered.items(),
                key=lambda item: item[1].latest_update,
                reverse=True,
            )
        )
    else:
        raise ValueError(f"Unknown sorting method: {sort_by}!")


@st.dialog(title="Set all charts to Pending")
def set_chart_diffs_to_pending(engine: Engine) -> None:
    """Set approval status of all chart diffs to pending."""
    st.markdown("**Do you want to set all charts-diffs to pending?** this will lose all your progress on reviews.")
    st.info(
        "💡 **Note:** This may take a moment. After completed, you may need to refresh the page to see the updated chart statuses."
    )
    if st.button("Yes", type="primary"):
        with Session(engine) as session:
            for _, chart_diff in st.session_state.chart_diffs.items():
                chart_diff.unreview(session)
        st.rerun()


@st.dialog(title="Set all pending charts to Approved")
def set_chart_diffs_to_approved(engine: Engine) -> None:
    """Set approval status of all pending chart diffs to approved."""
    pending_non_conflict_count = len(
        [chart for chart in st.session_state.chart_diffs.values() if chart.is_pending and not chart.in_conflict]
    )
    pending_conflict_count = len(
        [chart for chart in st.session_state.chart_diffs.values() if chart.is_pending and chart.in_conflict]
    )
    st.markdown(
        f"**Do you want to approve all {pending_non_conflict_count} pending (non-conflicted) chart diffs?** This will approve them for sync to production."
    )
    if pending_conflict_count > 0:
        st.warning(
            f"⚠️ **Note:** {pending_conflict_count} pending charts with conflicts will be skipped. Consider first using the 'Resolve all conflicts' button, or manually inspect them."
        )
    st.info(
        "💡 **Note:** This may take a moment. After completed, you may need to refresh the page to see the updated chart statuses."
    )
    if st.button("Yes", type="primary"):
        with Session(engine) as session:
            for _, chart_diff in st.session_state.chart_diffs.items():
                if chart_diff.is_pending and not chart_diff.in_conflict:  # Skip conflicted charts
                    chart_diff.approve(session)
        st.rerun()


@st.dialog(title="Resolve all conflicts (Accept staging changes)")
def resolve_all_conflicts_accept_staging(engine: Engine) -> None:
    """Resolve all conflicts by accepting staging changes and approve charts."""
    conflict_count = len([chart for chart in st.session_state.chart_diffs.values() if chart.in_conflict])
    st.markdown(
        f"**Do you want to resolve all {conflict_count} conflicts by accepting staging changes?** This will override any changes made in production when merging this to production."
    )
    st.info(
        "💡 **Note:** This may take a moment. After completed, you may need to refresh the page to see the updated chart statuses."
    )
    if st.button("Yes, accept staging changes", type="primary"):
        with Session(engine) as session:
            for _, chart_diff in st.session_state.chart_diffs.items():
                if chart_diff.in_conflict:
                    chart_diff.set_conflict_to_resolved(session)
                    chart_diff.approve(session)
        st.rerun()


def _show_options_filters():
    def show_reviewed():
        # st.toast(f"ENTERING hide: {st.session_state['show-reviewed-charts']}")
        if st.session_state["show-reviewed-charts"]:
            st.query_params.update({"show_reviewed": ""})  # type: ignore
        else:
            st.query_params.pop("show_reviewed", None)

    def show_all():
        # st.toast(f"ENTERING hide: {st.session_state['show-reviewed-charts']}")
        if st.session_state["show-all-charts"]:
            st.query_params.update({"show_all": ""})  # type: ignore
        else:
            st.query_params.pop("show_all", None)

    def apply_search_filters():
        """Apply filters.

        Get filter parameters from session state."""

        def _apply_search_filters(session_key, query_key):
            if st.session_state[session_key]:
                st.query_params.update({query_key: st.session_state[session_key]})
            else:
                st.query_params.pop(query_key, None)

        # Chart ID filter
        _apply_search_filters("chart-diff-filter-id", "chart_id")
        # Indicator filter
        _apply_search_filters("chart-diff-filter-indicator", "indicator_id")
        # Slug filter
        _apply_search_filters("chart-diff-filter-slug", "chart_slug")
        # Change type filter
        _apply_search_filters("chart-diff-change-type", "change_type")

    st.markdown("#### Search filters")
    st.toggle(
        "**Show** reviewed charts",
        key="show-reviewed-charts",
        value="show_reviewed" in st.query_params,
        on_change=show_reviewed,  # type: ignore
        help="Show only chart diffs that are pending approval (or rejection).",
    )
    st.toggle(
        "**Show all charts** (ignores all filters)",
        key="show-all-charts",
        value="show_all" in st.query_params,
        on_change=show_all,  # type: ignore
        help="Show all charts. This option ignores all the filters.\n\nIf you want to apply any filter, uncheck this option.",
    )
    with st.form("chart-diff-filters"):
        default = [change for change in st.query_params.get_all("change_type")]
        if not default:
            default = ["new", "config"]
        st.multiselect(
            label="Chart change types",
            options=["new", "data", "metadata", "config"],  # type: ignore
            format_func=lambda x: x if x == "new" else f"{x} modified",
            default=default,  # type: ignore
            key="chart-diff-change-type",
            help="Show new charts or modified ones with changes in data, metadata, or config.",
            placeholder="config, data, metadata",
        )
        st.multiselect(
            label="Chart IDs",
            options=[c.chart_id for c in st.session_state.chart_diffs.values()],
            default=[int(n) for n in st.query_params.get_all("chart_id")],  # type: ignore
            key="chart-diff-filter-id",
            help="Filter chart diffs with charts with given IDs.",
            placeholder="Select chart IDs",
        )
        st.multiselect(
            label="Indicator IDs",
            options=sorted(st.session_state.indicators_in_charts.keys()),
            format_func=lambda s: f"[{s}] {st.session_state.indicators_in_charts[s]}",
            default=[int(n) for n in st.query_params.get_all("indicator_id")],  # type: ignore
            key="chart-diff-filter-indicator",
            help="Filter chart diffs to charts containing any of the selected indicators.",
            placeholder="Select indicator IDs",
        )
        st.text_input(
            label="Chart slug",
            value=st.query_params.get("chart_slug", ""),  # type: ignore
            placeholder="Search for a slug",
            key="chart-diff-filter-slug",
            help="Filter chart diffs with charts with slugs containing any of the given words (fuzzy match).",
        )

        st.form_submit_button(
            "Apply filters",
            on_click=apply_search_filters,  # type: ignore
        )


def _show_options_display():
    def arrange_charts():
        set_states(
            {
                "arrange_charts_vertically": not st.session_state.arrange_charts_vertically,
            }
        )

    # Display options
    st.markdown("#### Results page")

    col1, col2, col3 = st.columns(3, vertical_alignment="bottom")
    ## Sorting
    with col1:
        url_persist(st.selectbox)(
            label="Sort by",
            options=SORTING_METHODS.keys(),
            format_func=lambda x: SORTING_METHODS[x],
            key=SORTING_QUERY_PARAM,
            help="Sort chart diffs by relevance, user chart views, anomaly score, last updated, etc.",
            index=0,
        )

    ## Display options
    with col2:
        st.selectbox(
            "Number of charts per page",
            options=[
                # 1,
                5,
                10,
                20,
                50,
                100,
            ],
            key="charts-per-page",
            help="Select the number of charts to display per page.",
            index=1,
        )

    with col3:
        st.toggle(
            "Use **vertical arrangement** for chart diffs",
            key="arrange-charts-vertically",
            on_change=arrange_charts,  # type: ignore
        )


def _show_options_misc():
    """Show other options."""
    st.button(
        "🔄 Refresh all charts",
        key="refresh-btn-general",
        on_click=lambda: set_states({"chart_diffs": get_chart_diffs_from_grapher(SOURCE_ENGINE, TARGET_ENGINE)}),
        help="Get the latest chart versions, both from the staging and production servers.",
    )
    with st.container(border=True):
        st.markdown("Danger zone ⚠️")
        if st.button(
            "Resolve all conflicts **(Accept staging changes)**",
            key="resolve-all-conflicts",
            help="This will accept all staging changes for charts with conflicts. Use with caution!",
        ):
            resolve_all_conflicts_accept_staging(SOURCE_ENGINE)

        if st.button(
            "Set all charts to **Pending**",
            key="unapprove-all-charts",
            # on_click=lambda e=SOURCE_ENGINE: set_chart_diffs_to_pending(e),
            help="This sets the status of all chart diffs to 'Pending'. This means that you will need to review them again.",
        ):
            set_chart_diffs_to_pending(SOURCE_ENGINE)

        if st.button(
            "Set all pending charts to **Approved**",
            key="approve-all-pending-charts",
            help="This approves all pending (non-conflicted) chart diffs for sync to production. Charts with conflicts will be skipped.",
        ):
            set_chart_diffs_to_approved(SOURCE_ENGINE)


def _show_options():
    """Show options pane."""

    with st.popover("⚙️ Options", width="stretch"):
        # col1, col2, col3 = st.columns(3)

        # Filters
        # with col1:
        _show_options_filters()
        # Display
        # with col2:
        _show_options_display()
        # Buttons (refresh, unreview)
        # with col3:
        st.divider()
        _show_options_misc()


def _show_summary_top(chart_diffs):
    """Text summarizing the state of the revision."""
    # Review status
    num_charts_total = len(st.session_state.chart_diffs)
    num_charts_listed = len(chart_diffs)
    num_charts_approved = len([chart for chart in st.session_state.chart_diffs.values() if chart.is_approved])
    num_charts_rejected = len([chart for chart in st.session_state.chart_diffs.values() if chart.is_rejected])
    num_charts_reviewed = num_charts_approved + num_charts_rejected
    text = f"ℹ️ {num_charts_reviewed}/{num_charts_total} charts reviewed :small[:gray[(:material/thumb_up: {num_charts_approved} :material/thumb_down: {num_charts_rejected})]]"

    # Signal filtering (if any)
    if num_charts_listed != num_charts_total:
        text_warning = f"{num_charts_total-num_charts_listed} charts are hidden (already reviewed, or filtered)."
        text += f" :orange-badge[:small[{text_warning}]]"
        st.markdown(
            text,
            help="**Notes**\n\n- By default, only charts that haven't been reviewed are shown. You can change this behavior in the ⚙️ Options menu.\n- The displayed information on the number of charts reviewed or hidden is not updated dynamically, but only when the page is refreshed.",
        )
    else:
        st.markdown(text, help="The number of reviewed charts is only updated when the page is loaded.")


def render_app():
    """Render app.

    This involves: displaying the chart diffs according to filters applied by user.
    """
    if len(st.session_state.chart_diffs) == 0:
        st.warning("No chart modifications found in the staging environment.")
    else:
        # Filter based on query params
        _ = filter_chart_diffs()

        # Show all of the charts
        col1, col2 = st.columns([1, 1], vertical_alignment="top")
        with col2:
            _show_options()

        # Show diffs
        if len(st.session_state.chart_diffs) == 0:
            with col1:
                with st.container(border=True):
                    st.warning("No charts to be shown. Try changing the filters in the Options menu.")
        else:
            with col1:
                with st.container(border=True):
                    _show_summary_top([chart for chart in st.session_state.chart_diffs_filtered.values()])
            # if len(st.session_state.chart_diffs_filtered) != 0:
            # Show changed charts (modified, new, etc.)
            if st.session_state.chart_diffs_filtered:
                # Render chart diffs
                with Session(SOURCE_ENGINE) as source_session, Session(TARGET_ENGINE) as target_session:
                    show_chart_diffs(
                        [chart for chart in st.session_state.chart_diffs_filtered.values()],
                        "pagination",
                        source_session,
                        target_session,
                    )
            # else:
            #     st.warning(
            #         "No chart changes found in the staging environment. Try unchecking the 'Hide approved charts' toggle in case there are hidden ones."
            #     )


def show_chart_diffs(chart_diffs, pagination_key, source_session: Session, target_session: Session) -> None:
    """Display chart diffs."""
    # Pagination menu
    with st.container(border=True):
        # Information
        # _show_summary_top(chart_diffs)

        # Pagination
        pagination = Pagination(
            chart_diffs,
            items_per_page=st.session_state["charts-per-page"],
            pagination_key=pagination_key,
        )
        ## Show controls only if needed
        if len(chart_diffs) > st.session_state["charts-per-page"]:
            pagination.show_controls(mode="bar")

    # Show charts
    with Session(TARGET_ENGINE) as target_session:
        for chart_diff in pagination.get_page_items():
            st_show(chart_diff, source_session, target_session)


def st_docs():
    # Chart sync documentation
    # TODO: keep this in sync with `etl chart-sync` CLI docs
    with st.expander("📋 What gets synced when you merge your PR?", expanded=False):
        st.markdown("""
        When you merge your PR to master, the **chart-sync** process automatically runs and syncs approved charts from your staging environment to production. Here's what happens:

        **Charts that get synced:**
        - ✅ **Approved charts** (new or modified) are synced to production
        - ✅ **New charts** include their tags when synced
        - ⚠️ **Pending charts** are NOT synced (and will cause a Slack notification)
        - ❌ **Rejected charts** are skipped entirely

        **What gets synced for each chart:**
        - **Chart configuration** (title, subtitle, axis labels, chart type, etc.)
        - **Variable mappings** (automatically migrated from staging to production IDs)
        - **Tags** (tags for charts that are not in chart-diff won't be synced)
        - **Chart metadata** (description, notes, etc.)

        **Additional items synced:**
        - **DoDs (Data on Demand)** that were created or modified since staging server creation

        **Important considerations:**
        - The underlying **dataset and indicators** must already exist in production (via ETL pipeline)
        - Charts modified in production after staging creation will cause **conflicts** and require manual resolution
        - **Deleted charts** are NOT synced (deletions must be done manually in production)
        - Charts with **slug conflicts** (new chart with existing slug) will be skipped with an error
        """)


########################################
# MAIN
########################################
def main():
    # Title and links
    st_title_with_expert(
        title="Chart Diff",
        icon=":material/difference:",
        help=f"""
**Chart diff** is a living page that compares all ongoing charts between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.

It lists all those charts that have been modified in the `{OWID_ENV.name}` environment.

If you want any of the modified charts in `{OWID_ENV.name}` to be migrated to `production`, you can approve them by clicking on the toggle button.
""",
    )

    with st_horizontal(vertical_alignment="center"):
        st.markdown("Other links: ")
        st_wizard_page_link("mdim-diff")
        st_wizard_page_link("explorer-diff")

    st_docs()

    # Get actual charts
    get_chart_diffs()

    # Render app
    render_app()


main()
