from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from st_copy_to_clipboard import st_copy_to_clipboard
from structlog import get_logger

import etl.grapher_model as gm
from apps.chart_sync.cli import _modified_chart_ids_by_admin
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.app_pages.chart_diff.config_diff import st_show_diff
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
st.session_state.hide_reviewed_charts = st.session_state.get("hide_reviewed_charts", False)
st.session_state.arrange_charts_vertically = st.session_state.get("arrange_charts_vertically", False)


########################################
# LOAD ENVS
########################################
warn_msg = []

SOURCE = OWID_ENV
assert OWID_ENV.env_type_id != "production", "Your .env points to production DB, please use a staging environment."

# Try to compare against production DB if possible, otherwise compare against staging-site-master
if config.ENV_FILE_PROD:
    TARGET = OWIDEnv.from_env_file(config.ENV_FILE_PROD)
else:
    warning_msg = "ENV file doesn't connect to production DB, comparing against `staging-site-master`."
    log.warning(warning_msg)
    warn_msg.append(warning_msg)
    TARGET = OWIDEnv.from_staging("master")

CHART_PER_PAGE = 10


########################################
# WARNING MSG
########################################
warn_msg += ["This tool is being developed! Please report any issues you encounter in `#proj-new-data-workflow`"]
st.warning("- " + "\n\n- ".join(warn_msg))

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


def st_show(diff: ChartDiffModified, source_session, target_session=None, expander: bool = True) -> None:
    """Show the chart diff in Streamlit."""
    # DISPLAY options
    DISPLAY_STATE_OPTIONS = {
        gm.ChartStatus.APPROVED.value: {
            "label": "Approve",
            "color": "green",
            "icon": "✅",
        },
        gm.ChartStatus.REJECTED.value: {
            "label": "Reject",
            "color": "red",
            "icon": "❌",
        },
        gm.ChartStatus.PENDING.value: {
            "label": "Pending",
            "color": "gray",
            "icon": "⏳",
        },
    }

    # Define label
    print("Showing diff, state:", diff.is_approved, diff.is_rejected, diff.is_pending)
    emoji = DISPLAY_STATE_OPTIONS[diff.approval_status]["icon"]  # type: ignore
    label = f"{emoji} {diff.slug}"

    # Define action for Toggle on change
    def chart_state_change(diff, session) -> None:
        # print(st.session_state.chart_diffs[diff.chart_id].approval_status)
        with st.spinner():
            status = st.session_state[f"radio-{diff.chart_id}"]
            diff.set_status(session=session, status=status)

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
    def st_show_actually():
        col1, col2, col3 = st.columns([1, 1, 1])

        # Refresh
        with col2:
            st.button(
                "🔄 Refresh",
                key=f"refresh-btn-{diff.chart_id}",
                on_click=lambda s=source_session, t=target_session: refresh_on_click(s, t),
                help="Get the latest version of the chart from the staging server.",
            )

        # Copy link
        with col3:
            st.markdown(f"{OWID_ENV.wizard_url}?page=chart-diff&slug={diff.slug}")
            st_copy_to_clipboard(
                text=f"{OWID_ENV.wizard_url}?page=chart-diff&slug={diff.slug}",
                before_copy_label="🔗 Copy link",
                after_copy_label="✅ Copy link",
            )

        # Actions on chart diff: approve, pending, reject
        option_names = list(DISPLAY_STATE_OPTIONS.keys())
        with col1:
            st.radio(
                label="Approve or reject chart",
                key=f"radio-{diff.chart_id}",
                options=option_names,
                horizontal=True,
                format_func=lambda x: f":{DISPLAY_STATE_OPTIONS[x]['color']}-background[{DISPLAY_STATE_OPTIONS[x]['label']}]",
                index=option_names.index(diff.approval_status),  # type: ignore
                on_change=lambda diff=diff, session=source_session: chart_state_change(diff, session),
                # label_visibility="collapsed",
            )

        # Show diff
        if diff.is_modified:
            tab1, tab2, tab3 = st.tabs(["Charts", "Config diff", "Change history"])
            with tab1:
                arrange_vertical = (
                    st.session_state.get(f"arrange-charts-vertically-{diff.chart_id}", False)
                    | st.session_state.arrange_charts_vertically
                )
                # Chart diff
                st_compare_charts(
                    **kwargs_diff,
                    arrange_vertical=arrange_vertical,
                )
                st.toggle(
                    "Arrange charts vertically",
                    key=f"arrange-charts-vertically-{diff.chart_id}",
                    # on_change=None,
                )
            with tab2:
                assert diff.target_chart is not None
                st_show_diff(diff.target_chart.config, diff.source_chart.config)
            with tab3:
                approvals = diff.get_all_approvals(source_session)

                # Get text
                text = ""
                for counter, approval in enumerate(approvals):
                    emoji = DISPLAY_STATE_OPTIONS[str(approval.status)]["icon"]
                    color = DISPLAY_STATE_OPTIONS[str(approval.status)]["color"]
                    text_ = f"{approval.updatedAt}: {emoji} :{color}[{approval.status}]"

                    if counter == 0:
                        text_ = f"**{text_}**"

                    text += text_ + "\n\n"

                st.markdown(text)

        elif diff.is_new:
            st_compare_charts(**kwargs_diff)

    if expander:
        with st.expander(label, not diff.is_reviewed):
            st_show_actually()
    else:
        st_show_actually()


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


def st_compare_charts(
    source_chart,
    target_chart=None,
    arrange_vertical=False,
) -> None:
    # Only one chart: new chart
    if target_chart is None:
        st.markdown(f"New version ┃ _{pretty_date(source_chart)}_")
        chart_html(source_chart.config, owid_env=SOURCE)
    # Two charts, actual diff
    else:
        # Show charts
        if arrange_vertical:
            st_compare_charts_vertically(target_chart, source_chart)
        else:
            st_compare_charts_horizontally(target_chart, source_chart)


def st_compare_charts_vertically(target_chart, source_chart):
    # Check if production's chart is newer
    prod_is_newer = target_chart.updatedAt > source_chart.updatedAt

    # Define chart titles
    text_production = _get_chart_text_production(prod_is_newer, target_chart)
    text_staging = _get_text_staging(prod_is_newer, source_chart)

    # Chart production
    if prod_is_newer:
        help_text = _get_chart_text_help_production()
        st.markdown(text_production, help=help_text)
    else:
        st.markdown(text_production)
    chart_html(target_chart.config, owid_env=TARGET)

    # Chart staging
    st.markdown(text_staging)
    chart_html(source_chart.config, owid_env=SOURCE)


def st_compare_charts_horizontally(target_chart, source_chart):
    # Create two columns for the iframes
    col1, col2 = st.columns(2)

    # Check if production's chart is newer
    prod_is_newer = target_chart.updatedAt > source_chart.updatedAt

    # Define chart titles
    text_production = _get_chart_text_production(prod_is_newer, target_chart)
    text_staging = _get_text_staging(prod_is_newer, source_chart)

    with col1:
        if prod_is_newer:
            help_text = _get_chart_text_help_production()
            st.markdown(text_production, help=help_text)
        else:
            st.markdown(text_production)
        chart_html(target_chart.config, owid_env=TARGET)
    with col2:
        st.markdown(text_staging)
        chart_html(source_chart.config, owid_env=SOURCE)


def _get_chart_text_production(prod_is_newer: bool, production_chart):
    # Everything is fine
    if not prod_is_newer:
        text_production = f"Production ┃ _{pretty_date(production_chart)}_"
    # Conflict with live
    else:
        text_production = f":red[Production ┃ _{pretty_date(production_chart)}_] ⚠️"

    return text_production


def _get_chart_text_help_production():
    return "The chart in production was modified after creating the staging server. Please resolve the conflict by integrating the latest changes from production into staging."


def _get_text_staging(prod_is_newer: bool, staging_chart):
    # Everything is fine
    if not prod_is_newer:
        text_staging = f":green[New version ┃ _{pretty_date(staging_chart)}_]"
    # Conflict with live
    else:
        text_staging = f"New version ┃ _{pretty_date(staging_chart)}_"

    return text_staging


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    return SOURCE.get_engine(), TARGET.get_engine()


def show_help_text():
    with st.popover("How does this work?"):
        st.markdown(
            f"""
        **Chart diff** is a living page that compares all ongoing charts between [`production`](http://owid.cloud) and your [`{OWID_ENV.name}`]({OWID_ENV.admin_site}) environment.

        It lists all those charts that have been modified in the `{OWID_ENV.name}` environment.

        If you want any of the modified charts in `{OWID_ENV.name}` to be migrated to `production`, you can approve them by clicking on the toggle button.
        """
        )


def unreview_chart_diffs(engine):
    with Session(engine) as session:
        for _, chart_diff in st.session_state.chart_diffs.items():
            chart_diff.unreview(session)


def st_show_options(source_engine, target_engine):
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
        st.toggle(
            "**Unreview** all charts",
            key="unapprove-all-charts",
            on_change=lambda e=source_engine: unreview_chart_diffs(e),
        )

        def hide_reviewed():
            set_states(
                {
                    "hide_reviewed_charts": not st.session_state.hide_reviewed_charts,
                }
            )

            # Hide approved (if option selected)
            if st.session_state.hide_reviewed_charts:
                st.session_state.chart_diffs_filtered = {
                    k: v for k, v in st.session_state.chart_diffs.items() if not v.is_reviewed
                }
            else:
                st.session_state.chart_diffs_filtered = st.session_state.chart_diffs

        def arrange_charts():
            set_states(
                {
                    "arrange_charts_vertically": not st.session_state.arrange_charts_vertically,
                }
            )

        st.toggle("**Hide** reviewed charts", key="hide-reviewed-charts", on_change=hide_reviewed)
        st.toggle(
            "Use **vertical arrangement** for chart diffs",
            key="arrange-charts-vertically",
            on_change=arrange_charts,
        )


########################################
# MAIN
########################################
def main():
    st.title("Chart ⚡ **:gray[Diff]**")
    show_help_text()

    # Get stuff from DB
    source_engine, target_engine = get_engines()

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

    # Just show one slug
    if "slug" in st.query_params:
        slugs = st.query_params.get_all("slug")
        with Session(source_engine) as source_session:
            with Session(target_engine) as target_session:
                charts_match = [
                    chart_diff
                    for chart_diff in st.session_state.chart_diffs_filtered.values()
                    if chart_diff.slug in slugs
                ]
                if len(charts_match) == 0:
                    st.error(f"No chart diff with slug in {slugs}")
                else:
                    for chart_diff in charts_match:
                        st_show(chart_diff, source_session, target_session, expander=True)
                    st.button(
                        label="See all chart diffs",
                        key="see-all-charts",
                        on_click=lambda: st.query_params.from_dict({}),
                        type="primary",
                    )

    # Show all of the charts
    else:
        # Show options
        st_show_options(source_engine, target_engine)

        # Modified / New charts
        chart_diffs_modified = [
            chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_modified
        ]
        chart_diffs_new = [
            chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_new
        ]

        # Show diffs
        if len(st.session_state.chart_diffs) == 0:
            st.warning("No chart modifications found in the staging environment.")
        elif len(st.session_state.chart_diffs_filtered) == 0:
            st.warning("All charts are approved. To view them, uncheck the 'Hide approved charts' toggle.")
        else:
            # Show modified/new charts
            with Session(source_engine) as source_session:
                with Session(target_engine) as target_session:
                    # Show modified charts
                    if chart_diffs_modified:
                        st.header("Modified charts")
                        st.markdown(
                            f"{len(chart_diffs_modified)} chart{'s' if len(chart_diffs_modified) > 1 else ''} modified in [{OWID_ENV.name}]({OWID_ENV.site})"
                        )

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

                    # Show new charts
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
