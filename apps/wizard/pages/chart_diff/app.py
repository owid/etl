from pathlib import Path
from typing import Set

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel
from st_pages import add_indentation
from streamlit_extras.stylable_container import stylable_container

from apps.staging_sync.cli import _get_engine_for_env, _modified_chart_ids_by_admin, _validate_env
from apps.wizard.pages.chart_diff.chart_diff import ChartDiffModified
from apps.wizard.utils import chart_html
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent
# TODO: unhardcode this
SOURCE_ENV = "staging-site-streamlit-chart-approval"

# TODO: switch to production once we are ready
TARGET_ENV = "staging-site-master"

# st.session_state.chart_approval_list = st.session_state.get("chart_approval_list", [])


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


def get_modified_chart_ids(source_session) -> Set[int]:
    # These contain both updated and new charts!!!
    return _modified_chart_ids_by_admin(source_session)


def get_new_chart_ids():
    raise NotImplementedError()


def get_modified_explorers():
    return []


def show(diff: ChartDiffModified, source_session, target_session) -> None:
    # Define label
    emoji = "✅" if diff.approved else "⏳"
    label = f"{emoji} {diff.source_chart.config['slug']}"

    with st.expander(label, not diff.approved):
        # Approval toggle
        st.toggle(
            label="Approved new chart version",
            key=f"toggle-{diff.chart_id}",
            value=diff.approved,
            on_change=lambda session=source_session: diff.update_state(session),
        )

        # Refresh button (get updated chart from source environment)
        st.button(
            "🔄 Refresh",
            key=f"refresh-btn-{diff.chart_id}",
            on_click=lambda source_session=source_session, target_session=target_session: diff.sync(
                source_session, target_session
            ),
            help="Get the latest version of the chart from the staging server.",
        )

        # Chart comparison
        compare_charts(diff.source_chart, diff.target_chart)


def show_new(diff: ChartDiffModified, source_session, target_session=None) -> None:
    # Define label
    emoji = "✅" if diff.approved else "⏳"
    label = f"{emoji} {diff.source_chart.config['slug']}"

    with st.expander(label, not diff.approved):
        # Approval Toggle
        if diff.is_modified:
            label = "Approved new chart version"
        elif diff.is_new:
            label = "Approved new chart"
        else:
            raise ValueError("chart_diff show have flags `is_modified = not is_new`.")

        st.toggle(
            label=label,
            key=f"toggle-{diff.chart_id}",
            value=diff.approved,
            on_change=lambda session=source_session: diff.update_state(session),
        )

        # Refresh button (get updated chart from source environment)
        if diff.is_modified:
            st.button(
                "🔄 Refresh",
                key=f"refresh-btn-{diff.chart_id}",
                on_click=lambda source_session=source_session, target_session=target_session: diff.sync(
                    source_session, target_session
                ),
                help="Get the latest version of the chart from the staging server.",
            )
        elif diff.is_new:
            st.button(
                "🔄 Refresh",
                key=f"refresh-btn-{diff.chart_id}",
                on_click=lambda source_session=source_session: diff.sync(source_session),
                help="Get the latest version of the chart from the staging server.",
            )
        else:
            raise ValueError("chart_diff show have flags `is_modified = not is_new`.")

        # Chart comparison
        compare_charts(diff.source_chart)


def compare_charts(
    source_chart,
    target_chart=None,
):
    # Create two columns for the iframes
    col1, col2 = st.columns(2)

    prod_is_newer = source_chart.updatedAt > target_chart.updatedAt

    if target_chart is None:
        # st.selectbox(label="version", options=["Target"], key=f"selectbox-right-{identifier}")
        st.markdown(f"New version   |   `{target_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
        # st.write(target_chart.config)
        chart_html(target_chart.config, base_url=SOURCE_ENV)
    else:
        with col1:
            # st.selectbox(label="version", options=["Source"], key=f"selectbox-left-{identifier}")
            if prod_is_newer:
                st.markdown("Production :red[(⚠️was modified)]")
            else:
                st.markdown(f"Production   |   `{source_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
            chart_html(source_chart.config, base_url=TARGET_ENV)

        with col2:
            # st.selectbox(label="version", options=["Target"], key=f"selectbox-right-{identifier}")
            st.markdown(f"New version   |   `{target_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
            # st.write(target_chart.config)
            chart_html(target_chart.config, base_url=SOURCE_ENV)


@st.cache_resource
def get_engines() -> tuple[Engine, Engine]:
    s = Path(SOURCE_ENV)
    t = Path(TARGET_ENV)

    _validate_env(s)
    _validate_env(t)

    source_engine = _get_engine_for_env(s)
    target_engine = _get_engine_for_env(t)

    return source_engine, target_engine


def update_expander(chart_id, title, expanded: bool):
    st.session_state.expanders[chart_id] = {
        "label": f"✅ {title}" if st.session_state.expanders[chart_id]["label"] == "" else "",
        "expanded": expanded,
    }


def show_help_text():
    with st.popover("How does this work?"):
        st.markdown(
            """
        **Chart diff** is a living page that compares all ongoing charts between `PRODUCTION` and your `STAGING` environment.

        It lists all those charts that have been modified in the `STAGING` environment.

        If you want any of the modified charts in `STAGING` to be migrated to `PRODUCTION`, you can approve them by clicking on the toggle button.
        """
        )


def main():
    st.title("Chart ⚡ **:gray[Diff]**")
    show_help_text()

    st.button(
        "🔄 Refresh all charts",
        key="refresh-btn-general",
        on_click=lambda _: print("refresh"),
        help="Get the latest chart versions, both from the staging and production servers.",
    )

    # Get stuff from DB
    source_engine, target_engine = get_engines()
    # TODO: this should be created via migration in owid-grapher!!!!!
    # create chart_diff_approvals table if it doesn't exist
    SQLModel.metadata.create_all(source_engine, [gm.ChartDiffApprovals.__table__])  # type: ignore
    # chart_ids_new = get_new_chart_ids()
    # explorers_modified = get_modified_explorers()
    # Get actual charts
    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # Get IDs from modified charts
            charts_modified_ids = get_modified_chart_ids(source_session)
            chart_diffs = [
                ChartDiffModified.from_chart_id(
                    chart_id=chart_id,
                    source_session=source_session,
                    target_session=target_session,
                )
                for chart_id in charts_modified_ids
            ]

    # MODIFIED CHARTS
    chart_diffs_modified = [chart_diff for chart_diff in chart_diffs if chart_diff.is_modified]
    chart_diffs_new = [chart_diff for chart_diff in chart_diffs if chart_diff.is_new]
    with Session(source_engine) as source_engine:
        with Session(target_engine) as target_engine:
            if charts_modified_ids:
                st.header("Modified charts")
                st.markdown(f"{len(charts_modified_ids)} charts modified in `{SOURCE_ENV}`")
                for chart_diff in chart_diffs_modified:
                    show(chart_diff, source_session, target_engine)

            if chart_diffs_new:
                st.header("New charts")
                st.markdown(f"{len(chart_diffs_new)} new charts in `{SOURCE_ENV}`")
                for chart_diff in chart_diffs_new:
                    show_new(chart_diff, source_session, target_engine)


main()
