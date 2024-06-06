import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

# from st_copy_to_clipboard import st_copy_to_clipboard
from structlog import get_logger

import etl.grapher_model as gm
from apps.chart_sync.cli import _modified_chart_ids_by_admin
from apps.wizard.app_pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.app_pages.chart_diff.config_diff import st_show_diff
from apps.wizard.utils import Pagination, chart_html, set_states
from apps.wizard.utils.env import OWID_ENV, OWIDEnv
from etl import config

log = get_logger()

# Config
st.set_page_config(
    page_title="Wizard: Chart Diff",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
    menu_items={
        "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
        #     "About": """
        # """,
    },
)

CURRENT_DIR = Path(__file__).resolve().parent

st.session_state.chart_diffs = st.session_state.get("chart_diffs", {})


########################################
# PAGE CONFIG
########################################
st.session_state.arrange_charts_vertically = st.session_state.get("arrange_charts_vertically", False)

########################################
# LOAD ENVS
########################################
warn_msg = []

SOURCE = OWID_ENV
assert OWID_ENV.env_remote != "production", "Your .env points to production DB, please use a staging environment."

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
# st.warning("- " + "\n\n- ".join(warn_msg))


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


def get_chart_diffs_from_grapher(
    source_engine: Engine, target_engine: Engine, max_workers: int = 10
) -> dict[int, ChartDiffModified]:
    # st.toast("Getting charts...")
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


def st_show(
    diff: ChartDiffModified, source_session, target_session=None, expander: bool = True, show_link: bool = True
) -> None:
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
        # st.toast(f"updating chart diff {diff.chart_id}...")
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
        if show_link:
            with col3:
                st.caption(f"**{OWID_ENV.wizard_url}?page=chart-diff&chart_id={diff.chart_id}**")

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
                arrange_vertical = st.session_state.get(
                    f"arrange-charts-vertically-{diff.chart_id}", False
                ) | st.session_state.get("arrange-charts-vertically", False)
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
    def arrange_charts():
        # st.toast("ENTERING -- arrange_charts")
        set_states(
            {
                "arrange_charts_vertically": not st.session_state.arrange_charts_vertically,
            }
        )

    def hide_reviewed():
        # st.toast(f"ENTERING hide: {st.session_state['hide-reviewed-charts']}")
        if st.session_state["hide-reviewed-charts"]:
            st.query_params.update({"hide_reviewed": ""})  # type: ignore
        else:
            st.query_params.pop("hide_reviewed", None)

    def apply_search_filters():
        # Chart ID filter
        if st.session_state["chart-diff-filter-id"]:
            st.query_params.update({"chart_id": st.session_state["chart-diff-filter-id"]})
        else:
            st.query_params.pop("chart_id", None)
        # Slug filter
        if st.session_state["chart-diff-filter-slug"] == "":
            st.query_params.pop("chart_slug", None)
        else:
            st.query_params.update({"chart_slug": st.session_state["chart-diff-filter-slug"]})

    with st.popover("Options"):
        # Main options
        st.button(
            "🔄 Refresh all charts",
            key="refresh-btn-general",
            on_click=lambda source_engine=source_engine, target_engine=target_engine: set_states(
                {"chart_diffs": get_chart_diffs_from_grapher(source_engine, target_engine)}
            ),
            help="Get the latest chart versions, both from the staging and production servers.",
        )
        st.button(
            "**Unreview** all charts",
            key="unapprove-all-charts",
            on_click=lambda e=source_engine: unreview_chart_diffs(e),
        )

        # Filters
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
            st.form_submit_button(
                "Apply filters",
                on_click=apply_search_filters,  # type: ignore
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


def get_chart_diffs(source_engine, target_engine):
    """Get chart diffs."""
    # Get actual charts
    if st.session_state.chart_diffs == {}:
        with st.spinner("Getting charts from database..."):
            st.session_state.chart_diffs = get_chart_diffs_from_grapher(source_engine, target_engine)

    # Sort charts
    st.session_state.chart_diffs = dict(
        sorted(st.session_state.chart_diffs.items(), key=lambda item: item[1].latest_update, reverse=True)
    )

    if len(st.session_state.chart_diffs) == 0:
        st.warning("No chart modifications found in the staging environment.")

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


def render_chart_diffs(source_session, target_session, chart_diffs, pagination_key) -> None:
    """Display chart diffs."""
    # Information
    num_charts = len(chart_diffs)
    num_charts_reviewed = len([chart for chart in chart_diffs if chart.is_reviewed])
    text = f"{num_charts_reviewed}/{num_charts} charts reviewed."
    st.markdown(text)

    # Pagination
    modified_charts_pagination = Pagination(
        chart_diffs,
        items_per_page=st.session_state["charts-per-page"],
        pagination_key=pagination_key,
    )
    modified_charts_pagination.show_controls()

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

    # Get stuff from DB
    source_engine, target_engine = get_engines()

    # Get actual charts
    get_chart_diffs(source_engine, target_engine)

    # Filter based on query params
    filter_chart_diffs()

    # Show all of the charts
    st_show_options(source_engine, target_engine)

    # Show diffs
    if len(st.session_state.chart_diffs_filtered) == 0:
        st.warning("All charts are approved. To view them, uncheck the 'Hide approved charts' toggle.")
    else:
        # Get (i) modified and (ii) new charts
        # chart_diffs_modified = [
        #     chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_modified
        # ]
        # chart_diffs_new = [
        #     chart_diff for chart_diff in st.session_state.chart_diffs_filtered.values() if chart_diff.is_new
        # ]

        # Show changed charts (modified, new, etc.)
        with Session(source_engine) as source_session:
            with Session(target_engine) as target_session:
                # Show modified charts
                if st.session_state.chart_diffs_filtered:
                    # Render chart diffs
                    render_chart_diffs(
                        source_session,
                        target_session,
                        st.session_state.chart_diffs_filtered,
                        "pagination_modified",
                    )
                else:
                    st.warning(
                        "No chart changes found in the staging environment. Try unchecking the 'Hide approved charts' toggle in case there are hidden ones."
                    )


# [{OWID_ENV.name}]({OWID_ENV.site})
main()
