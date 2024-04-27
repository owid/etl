from pathlib import Path
from typing import Set

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel
from st_pages import add_indentation

from apps.staging_sync.cli import _get_engine_for_env, _modified_chart_ids_by_admin, _validate_env
from apps.wizard.pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.utils import chart_html, set_states
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent
# TODO: unhardcode this
SOURCE_ENV = "staging-site-streamlit-chart-approval"
# TODO: switch to production once we are ready
TARGET_ENV = "staging-site-master"

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


########################################
# FUNCTIONS
########################################
def get_chart_diffs(source_engine, target_engine):
    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # Get IDs from modified charts
            chart_ids = _modified_chart_ids_by_admin(source_session)
            chart_diffs = {
                chart_id: ChartDiffModified.from_chart_id(
                    chart_id=chart_id,
                    source_session=source_session,
                    target_session=target_session,
                )
                for chart_id in chart_ids
            }
    return chart_diffs


def st_show(diff: ChartDiffModified, source_session, target_session=None) -> None:
    """Show the chart diff in Streamlit."""
    # Define label
    emoji = "✅" if diff.approved else "⏳"
    label = f"{emoji} {diff.source_chart.config['slug']}"

    # Define action for Toggle on change
    def tgl_on_change(diff, session) -> None:
        with st.spinner():
            diff.update_state(session=session)

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
        label_tgl = "Approved new chart version"

        # Arguments for diff
        kwargs_diff = {
            "source_chart": diff.source_chart,
            "target_chart": diff.target_chart,
        }
    elif diff.is_new:
        # Arguments for the toggle
        label_tgl = "Approved new chart"

        # Arguments for diff
        kwargs_diff = {
            "source_chart": diff.source_chart,
        }
    else:
        raise ValueError("chart_diff show have flags `is_modified = not is_new`.")

    # Actually show stuff
    with st.expander(label, not diff.approved):
        # Toggle
        st.toggle(
            label=label_tgl,
            key=f"toggle-{diff.chart_id}",
            value=diff.approved,
            on_change=lambda diff=diff, session=source_session: tgl_on_change(diff, session),
        )
        # Refresh
        st.button(
            "🔄 Refresh",
            key=f"refresh-btn-{diff.chart_id}",
            on_click=lambda s=source_session, t=target_session: refresh_on_click(s, t),
            help="Get the latest version of the chart from the staging server.",
        )

        # Chart diff
        compare_charts(**kwargs_diff)


def compare_charts(
    source_chart,
    target_chart=None,
) -> None:
    if target_chart is None:
        st.markdown(f"New version   |   `{source_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
        chart_html(source_chart.config, base_url=SOURCE_ENV)
    else:
        # Create two columns for the iframes
        col1, col2 = st.columns(2)
        prod_is_newer = target_chart.updatedAt > source_chart.updatedAt
        with col1:
            # st.selectbox(label="version", options=["Source"], key=f"selectbox-left-{identifier}")
            if prod_is_newer:
                st.markdown("Production :red[(⚠️was modified after your staging edits!)]")
            else:
                st.markdown(f"Production   |   `{target_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
            chart_html(target_chart.config, base_url=TARGET_ENV)

        with col2:
            # st.selectbox(label="version", options=["Target"], key=f"selectbox-right-{identifier}")
            st.markdown(f"New version   |   `{source_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
            # st.write(target_chart.config)
            chart_html(source_chart.config, base_url=SOURCE_ENV)


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    s = Path(SOURCE_ENV)
    t = Path(TARGET_ENV)

    _validate_env(s)
    _validate_env(t)

    source_engine = _get_engine_for_env(s)
    target_engine = _get_engine_for_env(t)

    return source_engine, target_engine


def show_help_text():
    with st.popover("How does this work?"):
        st.markdown(
            """
        **Chart diff** is a living page that compares all ongoing charts between `PRODUCTION` and your `STAGING` environment.

        It lists all those charts that have been modified in the `STAGING` environment.

        If you want any of the modified charts in `STAGING` to be migrated to `PRODUCTION`, you can approve them by clicking on the toggle button.
        """
        )


########################################
# MAIN
########################################
def main():
    st.title("Chart ⚡ **:gray[Diff]**")
    show_help_text()

    # Get stuff from DB
    source_engine, target_engine = get_engines()

    st.button(
        "🔄 Refresh all charts",
        key="refresh-btn-general",
        on_click=lambda source_engine=source_engine, target_engine=target_engine: set_states(
            {"chart_diffs": get_chart_diffs(source_engine, target_engine)}
        ),
        help="Get the latest chart versions, both from the staging and production servers.",
    )

    # TODO: this should be created via migration in owid-grapher!!!!!
    # create chart_diff_approvals table if it doesn't exist
    SQLModel.metadata.create_all(source_engine, [gm.ChartDiffApprovals.__table__])  # type: ignore
    # Get actual charts
    if st.session_state.chart_diffs == {}:
        with st.spinner("Getting charts from database..."):
            st.session_state.chart_diffs = get_chart_diffs(source_engine, target_engine)

    # Modified / New charts
    chart_diffs_modified = [
        chart_diff for chart_diff in st.session_state.chart_diffs.values() if chart_diff.is_modified
    ]
    chart_diffs_new = [chart_diff for chart_diff in st.session_state.chart_diffs.values() if chart_diff.is_new]
    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            if chart_diffs_modified:
                st.header("Modified charts")
                st.markdown(f"{len(chart_diffs_modified)} charts modified in `{SOURCE_ENV}`")
                for chart_diff in chart_diffs_modified:
                    st_show(chart_diff, source_session, target_session)

            if chart_diffs_new:
                st.header("New charts")
                st.markdown(f"{len(chart_diffs_new)} new charts in `{SOURCE_ENV}`")
                for chart_diff in chart_diffs_new:
                    st_show(chart_diff, source_session, target_session)


main()
