import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

# from st_copy_to_clipboard import st_copy_to_clipboard
from structlog import get_logger

from apps.chart_sync.cli import _modified_chart_ids_by_admin
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.app_pages.chart_diff.show import st_show
from apps.wizard.app_pages.chart_diff.utils import WARN_MSG, get_engines
from apps.wizard.utils import Pagination, set_states
from apps.wizard.utils.env import OWID_ENV

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

# Paths
CURRENT_DIR = Path(__file__).resolve().parent

# DB access
# Create connections to DB
SOURCE_ENGINE, TARGET_ENGINE = get_engines()


########################################
# SESSION STATE
########################################
st.session_state.chart_diffs = st.session_state.get("chart_diffs", {})
st.session_state.arrange_charts_vertically = st.session_state.get("arrange_charts_vertically", False)

########################################
# LOAD VARIABLES
########################################
CHART_PER_PAGE = 10
WARN_MSG += ["This tool is being developed! Please report any issues you encounter in `#proj-new-data-workflow`"]
# st.warning("- " + "\n\n- ".join(warn_msg))


########################################
# FUNCTIONS
########################################
def _get_chart_diff(chart_id: int) -> ChartDiffModified:
    with Session(SOURCE_ENGINE) as source_session, Session(TARGET_ENGINE) as target_session:
        return ChartDiffModified.from_chart_id(
            chart_id=chart_id,
            source_session=source_session,
            target_session=target_session,
        )


def get_chart_diffs_from_grapher(max_workers: int = 10) -> dict[int, ChartDiffModified]:
    """Get chart diffs from Grapher.

    This means, checking for chart changes in the database.

    Changes in charts can be due to: chart config changes, changes in indicator timeseries, in indicator metadata, etc.
    """
    # Get IDs from modified charts
    with Session(SOURCE_ENGINE) as source_session:
        chart_ids = _modified_chart_ids_by_admin(source_session)

    # Get all chart diffs in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        chart_diffs_futures = {
            chart_id: executor.submit(_get_chart_diff, chart_id, SOURCE_ENGINE, TARGET_ENGINE) for chart_id in chart_ids
        }
        chart_diffs = {}
        for chart_id, future in chart_diffs_futures.items():
            chart_diffs[chart_id] = future.result()

    return chart_diffs


def set_chart_diffs_to_pending(engine: Engine) -> None:
    """Set approval status of all chart diffs to pending."""
    with Session(engine) as session:
        for _, chart_diff in st.session_state.chart_diffs.items():
            chart_diff.unreview(session)


def _show_options_filters():
    def hide_reviewed():
        # st.toast(f"ENTERING hide: {st.session_state['hide-reviewed-charts']}")
        if st.session_state["hide-reviewed-charts"]:
            st.query_params.update({"hide_reviewed": ""})  # type: ignore
        else:
            st.query_params.pop("hide_reviewed", None)

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
        # Slug filter
        _apply_search_filters("chart-diff-filter-slug", "chart_slug")
        # Change type filter
        _apply_search_filters("chart-diff-change-type", "change_type")

    st.markdown("#### Filters")
    st.toggle(
        "**Hide** reviewed charts",
        key="hide-reviewed-charts",
        value="hide_reviewed" in st.query_params,
        on_change=hide_reviewed,  # type: ignore
        help="Show only chart diffs that are pending approval (or rejection).",
    )
    with st.form("chart-diff-filters"):
        st.multiselect(
            label="Select chart IDs",
            options=[c.chart_id for c in st.session_state.chart_diffs.values()],
            default=[int(n) for n in st.query_params.get_all("chart_id")],  # type: ignore
            key="chart-diff-filter-id",
            help="Filter chart diffs with charts with given IDs.",
        )
        st.text_input(
            label="Search by slug name",
            value=st.query_params.get("chart_slug", ""),  # type: ignore
            placeholder="Search for a slug",
            key="chart-diff-filter-slug",
            help="Filter chart diffs with charts with slugs containing any of the given words (fuzzy match).",
        )
        st.multiselect(
            label="Chart changes type",
            options=["modified", "new"],
            default=[change for change in st.query_params.get_all("change_type")],  # type: ignore
            key="chart-diff-change-type",
            help="Show new charts, and/or modified charts.",
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
    st.markdown("#### Display")
    st.toggle(
        "Use **vertical arrangement** for chart diffs",
        key="arrange-charts-vertically",
        on_change=arrange_charts,  # type: ignore
    )
    st.selectbox(
        "Number of charts per page",
        options=[
            # 1,
            10,
            20,
            50,
            100,
        ],
        key="charts-per-page",
        help="Select the number of charts to display per page.",
        index=0,
    )


def _show_options_misc():
    """Show other options."""
    st.button(
        "🔄 Refresh all charts",
        key="refresh-btn-general",
        on_click=lambda: set_states({"chart_diffs": get_chart_diffs_from_grapher()}),
        help="Get the latest chart versions, both from the staging and production servers.",
    )
    st.divider()
    with st.container(border=True):
        st.markdown("Danger zone ⚠️")
        st.button(
            "Set _all_ charts to **Pending**",
            key="unapprove-all-charts",
            on_click=lambda s=SOURCE_ENGINE: set_chart_diffs_to_pending(s),
            help="This sets the status of all chart diffs to 'Pending'. This means that you will need to review them again.",
        )


def _show_options():
    """Show options pane."""

    with st.popover("⚙️ Options", use_container_width=True):
        col1, col2, col3 = st.columns(3)

        # Filters
        with col1:
            _show_options_filters()
        # Display
        with col2:
            _show_options_display()
        # Buttons (refresh, unreview)
        with col3:
            _show_options_misc()


def get_chart_diffs():
    """Get chart diffs."""
    # Get actual charts
    if st.session_state.chart_diffs == {}:
        with st.spinner("Getting charts from database..."):
            st.session_state.chart_diffs = get_chart_diffs_from_grapher()

    # Sort charts
    st.session_state.chart_diffs = dict(
        sorted(st.session_state.chart_diffs.items(), key=lambda item: item[1].latest_update, reverse=True)
    )

    # Init, can be changed by the toggle
    st.session_state.chart_diffs_filtered = st.session_state.chart_diffs


def filter_chart_diffs():
    """Filter chart diffs to display.

    This is based on the query parameters.
    """

    def _slugs_match(chart_slug_1, chart_slug_2):
        pattern = r"[,\s\-]+"
        chart_slug_1 = set(re.split(pattern, chart_slug_1.lower()))
        chart_slug_2 = set(re.split(pattern, chart_slug_2.lower()))
        if chart_slug_1.intersection(chart_slug_2):
            return True
        return False

    # Filter based on query params
    if "chart_id" in st.query_params:
        chart_ids = list(map(int, st.query_params.get_all("chart_id")))
        st.session_state.chart_diffs_filtered = {
            k: v for k, v in st.session_state.chart_diffs_filtered.items() if v.chart_id in chart_ids
        }
    if "chart_slug" in st.query_params:
        chart_slug = st.query_params.get("chart_slug", "")

        st.session_state.chart_diffs_filtered = {
            k: v for k, v in st.session_state.chart_diffs_filtered.items() if _slugs_match(chart_slug, v.slug)
        }
    if "hide_reviewed" in st.query_params:
        st.session_state.chart_diffs_filtered = {
            k: v for k, v in st.session_state.chart_diffs_filtered.items() if not v.is_reviewed
        }
    if "change_type" in st.query_params:
        change_types = st.query_params.get_all("change_type")
        st.session_state.chart_diffs_filtered = {
            k: v
            for k, v in st.session_state.chart_diffs_filtered.items()
            if (v.is_modified and "modified" in change_types) or (v.is_new and "new" in change_types)
        }

    # Return boolean if there was any filter applied (except for hiding approved charts)
    if "chart_id" in st.query_params or "chart_slug" in st.query_params or "change_type" in st.query_params:
        return True
    return False


def render_chart_diffs(chart_diffs, pagination_key, source_session: Session, target_session: Session) -> None:
    """Display chart diffs."""
    # Pagination menu
    with st.container(border=True):
        # Information
        num_charts_total = len(st.session_state.chart_diffs)
        num_charts = len(chart_diffs)
        num_charts_reviewed = len([chart for chart in chart_diffs if chart.is_reviewed])
        text = f"ℹ️ {num_charts_reviewed}/{num_charts_total} charts reviewed."
        if num_charts != num_charts_total:
            text += f" Showing {num_charts} after filtering."
        st.markdown(text)

        # Pagination
        modified_charts_pagination = Pagination(
            chart_diffs,
            items_per_page=st.session_state["charts-per-page"],
            pagination_key=pagination_key,
        )
        ## Show controls only if needed
        if len(chart_diffs) > st.session_state["charts-per-page"]:
            modified_charts_pagination.show_controls()

    # Show charts
    with Session(TARGET_ENGINE) as target_session:
        for chart_diff in modified_charts_pagination.get_page_items():
            st_show(chart_diff, source_session, target_session)


########################################
# MAIN
########################################
def main():
    st.title(
        "Chart ⚡ **:gray[Diff]**",
        help=f"""
**Chart diff** is a living page that compares all ongoing charts between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.

It lists all those charts that have been modified in the `{OWID_ENV.name}` environment.

If you want any of the modified charts in `{OWID_ENV.name}` to be migrated to `production`, you can approve them by clicking on the toggle button.
""",
    )

    # Get actual charts
    get_chart_diffs()

    if len(st.session_state.chart_diffs) == 0:
        st.warning("No chart modifications found in the staging environment.")
    else:
        # Filter based on query params
        _ = filter_chart_diffs()

        # Show all of the charts
        _show_options()

        # Show diffs
        if len(st.session_state.chart_diffs_filtered) == 0:
            st.warning("No charts to be shown. Try changing the filters in the Options menu.")
        else:
            # Show changed charts (modified, new, etc.)
            if st.session_state.chart_diffs_filtered:
                # Render chart diffs
                with Session(SOURCE_ENGINE) as source_session, Session(TARGET_ENGINE) as target_session:
                    render_chart_diffs(
                        [chart for chart in st.session_state.chart_diffs_filtered.values()],
                        "pagination_modified",
                        source_session,
                        target_session,
                    )
            else:
                st.warning(
                    "No chart changes found in the staging environment. Try unchecking the 'Hide approved charts' toggle in case there are hidden ones."
                )


main()
