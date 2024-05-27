from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from st_pages import add_indentation
from structlog import get_logger

from apps.staging_sync.cli import _modified_chart_ids_by_admin
from apps.wizard.pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.pages.chart_diff.config_diff import st_show_diff
from apps.wizard.utils import Pagination, chart_html, set_states
from apps.wizard.utils.env import OWID_ENV, OWIDEnv
from etl import config

log = get_logger()

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent

st.session_state.chart_diffs = st.session_state.get("chart_diffs", {})


########################################
# PAGE CONFIG
########################################
st.set_page_config(
    page_title="Wizard: Chart Diff",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
)
add_indentation()
st.session_state.hide_approved_charts = st.session_state.get("hide_approved_charts", False)


########################################
# LOAD ENVS
########################################
SOURCE = OWID_ENV
assert OWID_ENV.env_type_id != "production", "Your .env points to production DB, please use a staging environment."

# Try to compare against production DB if possible, otherwise compare against staging-site-master
if config.ENV_FILE_PROD:
    TARGET = OWIDEnv.from_env_file(config.ENV_FILE_PROD)
else:
    warning_msg = "ENV file doesn't connect to production DB, comparing against staging-site-master"
    log.warning(warning_msg)
    st.warning(warning_msg)
    TARGET = OWIDEnv.from_staging("master")

CHART_PER_PAGE = 10


########################################
# FUNCTIONS
########################################


def _get_chart_diff(chart_id: int, source_engine: Engine, target_engine: Engine) -> ChartDiffModified:
    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            return ChartDiffModified.from_chart_id(
                chart_id=chart_id,
                source_session=source_session,
                target_session=target_session,
            )


def get_chart_diffs(
    source_engine: Engine, target_engine: Engine, max_workers: int = 10
) -> dict[int, ChartDiffModified]:
    with Session(source_engine) as source_session:
        # Get IDs from modified charts
        chart_ids = _modified_chart_ids_by_admin(source_session)

    # Get all chart diffs in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        chart_diffs_futures = {
            chart_id: executor.submit(_get_chart_diff, chart_id, source_engine, target_engine) for chart_id in chart_ids
        }
        chart_diffs = {}
        for chart_id, future in chart_diffs_futures.items():
            chart_diffs[chart_id] = future.result()

    return chart_diffs


def st_show(diff: ChartDiffModified, source_session, target_session=None) -> None:
    """Show the chart diff in Streamlit."""
    # Define label
    emoji = "✅" if diff.approved else "⏳"
    label = f"{emoji} {diff.source_chart.config['slug']}"

    # Define action for Toggle on change
    def tgl_on_change(diff, session) -> None:
        with st.spinner():
            diff.switch_state(session=session)

    # Define action for Refresh on click
    def refresh_on_click(source_session=source_session, target_session=None):
        diff_new = ChartDiffModified.from_chart_id(
            chart_id=diff.chart_id,
            source_session=source_session,
            target_session=target_session,
        )
        st.session_state.chart_diffs[diff.chart_id] = diff_new

    # Get the right arguments for the toggle, button and diff show
    if diff.is_modified:
        # Arguments for the toggle
        # label_tgl = "Approved new chart version"

        # Arguments for diff
        kwargs_diff = {
            "source_chart": diff.source_chart,
            "target_chart": diff.target_chart,
        }
    elif diff.is_new:
        # Arguments for the toggle
        # label_tgl = "Approved new chart"

        # Arguments for diff
        kwargs_diff = {
            "source_chart": diff.source_chart,
        }
    else:
        raise ValueError("chart_diff show have flags `is_modified = not is_new`.")

    # Actually show stuff
    with st.expander(label, not diff.approved):
        col1, col2 = st.columns([1, 3])

        # Refresh
        with col2:
            st.button(
                "🔄 Refresh",
                key=f"refresh-btn-{diff.chart_id}",
                on_click=lambda s=source_session, t=target_session: refresh_on_click(s, t),
                help="Get the latest version of the chart from the staging server.",
            )

        options = ["Pending", "Approve"]
        options = {
            "Approve": "green",
            "Pending": "orange",
            # "Reject": "red",
        }
        option_names = list(options.keys())
        with col1:
            st.radio(
                label="Approve or reject chart",
                key=f"radio-{diff.chart_id}",
                options=option_names,
                horizontal=True,
                format_func=lambda x: f":{options.get(x)}-background[{x}]",
                index=option_names.index("Approve") if diff.approved else option_names.index("Pending"),
                on_change=lambda diff=diff, session=source_session: tgl_on_change(diff, session),
                # label_visibility="collapsed",
            )

        if diff.is_modified:
            tab1, tab2 = st.tabs(["Charts", "Config diff"])
            with tab1:
                # Chart diff
                compare_charts(**kwargs_diff)
            with tab2:
                assert diff.target_chart is not None
                st_show_diff(diff.target_chart.config, diff.source_chart.config)
        elif diff.is_new:
            compare_charts(**kwargs_diff)


def pretty_date(chart):
    """Obtain prettified date from a chart.

    Format is:
        - Previous years: `Jan 10, 2020 10:15`
        - This year: `Mar 15, 10:15` (no need to explicitly show the year)
    """
    if chart.updatedAt.year == datetime.now().date().year:
        return chart.updatedAt.strftime("%b %d, %H:%M")
    else:
        return chart.updatedAt.strftime("%b %d, %Y %H:%M")


def compare_charts(
    source_chart,
    target_chart=None,
) -> None:
    # Only one chart: new chart
    if target_chart is None:
        st.markdown(f"New version ┃ _{pretty_date(source_chart)}_")
        chart_html(source_chart.config, owid_env=SOURCE)
    # Two charts, actual diff
    else:
        # Create two columns for the iframes
        col1, col2 = st.columns(2)
        prod_is_newer = target_chart.updatedAt > source_chart.updatedAt
        # Standard diff
        if not prod_is_newer:
            with col1:
                st.markdown(f"Production ┃ _{pretty_date(target_chart)}_")
                chart_html(target_chart.config, owid_env=TARGET)
            with col2:
                st.markdown(f":green[New version ┃ _{pretty_date(source_chart)}_]")
                chart_html(source_chart.config, owid_env=SOURCE)
        # Conflict with live
        else:
            with col1:
                st.markdown(
                    f":red[Production ┃ _{pretty_date(target_chart)}_] ⚠️",
                    help="The chart in production was modified after creating the staging server. Please resolve the conflict by integrating the latest changes from production into staging.",
                )
                chart_html(target_chart.config, owid_env=TARGET)
            with col2:
                st.markdown(f"New version ┃ _{pretty_date(source_chart)}_")
                chart_html(source_chart.config, owid_env=SOURCE)


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    return SOURCE.get_engine(), TARGET.get_engine()


def show_help_text():
    with st.popover("How does this work?"):
        staging_name = OWID_ENV.name.upper()
        st.markdown(
            f"""
        **Chart diff** is a living page that compares all ongoing charts between `PRODUCTION` and your `{staging_name}` environment.

        It lists all those charts that have been modified in the `{staging_name}` environment.

        If you want any of the modified charts in `{staging_name}` to be migrated to `PRODUCTION`, you can approve them by clicking on the toggle button.
        """
        )


def reject_chart_diffs(engine):
    with Session(engine) as session:
        for _, chart_diff in st.session_state.chart_diffs.items():
            chart_diff.reject(session)

    ########################################


# MAIN
########################################
def main():
    st.title("Chart ⚡ **:gray[Diff]**")
    st.warning("This tool is being developed! Please report any issues you encounter in #proj-new-data-workflow")
    show_help_text()

    # Get stuff from DB
    source_engine, target_engine = get_engines()

    with st.popover("Options"):
        st.button(
            "🔄 Refresh all charts",
            key="refresh-btn-general",
            on_click=lambda source_engine=source_engine, target_engine=target_engine: set_states(
                {"chart_diffs": get_chart_diffs(source_engine, target_engine)}
            ),
            help="Get the latest chart versions, both from the staging and production servers.",
        )

        # Other options
        # with st.popover("Other options"):
        st.button(
            "🔙 Unapprove all charts",
            key="unapprove-all-charts",
            on_click=lambda e=source_engine: reject_chart_diffs(e),
        )

        def hide_approved():
            set_states(
                {
                    "hide_approved_charts": not st.session_state.hide_approved_charts,
                }
            )

            # Hide approved (if option selected)
            if st.session_state.hide_approved_charts:
                st.session_state.chart_diffs_filtered = {
                    k: v for k, v in st.session_state.chart_diffs.items() if not v.approved
                }
            else:
                st.session_state.chart_diffs_filtered = st.session_state.chart_diffs

        st.toggle("Hide appoved charts", key="hide-approved-charts", on_change=hide_approved)

    # Get actual charts
    if st.session_state.chart_diffs == {}:
        with st.spinner("Getting charts from database..."):
            st.session_state.chart_diffs = get_chart_diffs(source_engine, target_engine)

    # Sort charts
    st.session_state.chart_diffs = dict(
        sorted(st.session_state.chart_diffs.items(), key=lambda item: item[1].latest_update, reverse=True)
    )

    # Init, can be changed by the toggle
    if not hasattr(st.session_state, "chart_diffs_filtered"):
        st.session_state.chart_diffs_filtered = st.session_state.chart_diffs

    # Modified / New charts
    chart_diffs_modified = [
        chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_modified
    ]
    chart_diffs_new = [chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_new]

    # Show diffs
    if len(st.session_state.chart_diffs) == 0:
        st.warning("No chart modifications found in the staging environment.")
    elif len(st.session_state.chart_diffs_filtered) == 0:
        st.warning("All charts are approved. To view them, uncheck the 'Hide approved charts' toggle.")
    else:
        # Show modified/new charts
        with Session(source_engine) as source_session:
            with Session(target_engine) as target_session:
                if chart_diffs_modified:
                    st.header("Modified charts")
                    st.markdown(f"{len(chart_diffs_modified)} charts modified in [{OWID_ENV.name}]({OWID_ENV.site})")

                    modified_charts_pagination = Pagination(
                        chart_diffs_modified, items_per_page=CHART_PER_PAGE, pagination_key="pagination_modified"
                    )
                    modified_charts_pagination.show_controls()

                    for chart_diff in modified_charts_pagination.get_page_items():
                        st_show(chart_diff, source_session, target_session)
                else:
                    st.warning(
                        "No chart modifications found in the staging environment. Try unchecking the 'Hide approved charts' toggle in case there are hidden ones."
                    )

                if chart_diffs_new:
                    st.header("New charts")
                    st.markdown(f"{len(chart_diffs_new)} new charts in [{OWID_ENV.name}]({OWID_ENV.site})")

                    new_charts_pagination = Pagination(
                        chart_diffs_new, items_per_page=CHART_PER_PAGE, pagination_key="pagination_new"
                    )
                    new_charts_pagination.show_controls()

                    for chart_diff in new_charts_pagination.get_page_items():
                        st_show(chart_diff, source_session, target_session)
                else:
                    st.warning(
                        "No chart new charts found in the staging environment. Try unchecking the 'Hide approved charts' toggle in case there are hidden ones."
                    )


main()
