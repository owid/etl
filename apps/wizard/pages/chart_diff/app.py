from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel
from st_pages import add_indentation
from streamlit_extras.stylable_container import stylable_container

from apps.staging_sync.cli import _get_engine_for_env, _validate_env
from apps.wizard.utils import chart_html
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent
SOURCE_ENV = "staging-site-mojmir"
TARGET_ENV = "live"

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


def get_modified_chart_ids():
    chart_ids = [
        2000,
        2001,
        2002,
        2003,
        4005,
    ]
    return chart_ids


def get_new_chart_ids():
    chart_ids = [
        3000,
        3001,
    ]
    return chart_ids


def get_modified_explorers():
    return []


def compare_charts(
    source_chart,
    target_chart,
):
    # Create two columns for the iframes
    col1, col2 = st.columns(2)

    prod_is_newer = source_chart.updatedAt > target_chart.updatedAt

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


class ChartDiffModified:
    def __init__(self, source_chart, target_chart, approval_status):
        self.source_chart = source_chart
        self.target_chart = target_chart
        self.approval_status = approval_status
        assert source_chart.id == target_chart.id, "Missmatch in chart ids between Target and Source!"
        self.chart_id = source_chart.id

    @property
    def approved(self):
        return self.approval_status == "approved"

    @classmethod
    def from_chart_id(cls, chart_id, source_session, target_session):
        """Get chart diff from chart id.

        - Get charts from source and target
        - Get its approval state
        - Build diff object
        """
        # Get charts
        source_chart = gm.Chart.load_chart(source_session, chart_id=chart_id)
        target_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)
        # Get approval status
        approval_status = gm.ChartDiffApprovals.latest_chart_status(
            source_session,
            chart_id,
            source_chart.updatedAt,
            target_chart.updatedAt,
        )

        # Build object
        chart_diff = cls(source_chart, target_chart, approval_status)

        return chart_diff

    def sync(self, source_session, target_session):
        """Sync chart diff."""

        # Synchronize with latest chart from source environment
        self = self.from_chart_id(
            chart_id=self.chart_id,
            source_session=source_session,
            target_session=target_session,
        )

    def update_state(self, session) -> None:
        """Update the state of the chart diff."""
        # Update status variable
        self.approval_status = "approved" if self.approval_status == "rejected" else "rejected"

        # Update approval status (in database)
        st.toast(f"Updating state for **chart {self.chart_id}** to `{self.approval_status}`")
        approval = gm.ChartDiffApprovals(
            chartId=self.chart_id,
            sourceUpdatedAt=self.source_chart.updatedAt,
            targetUpdatedAt=self.target_chart.updatedAt,
            status="approved" if self.approved else "rejected",
        )

        session.add(approval)
        session.commit()

        # Update expander display
        # update_expander(chart_id=chart_id, title=title, expanded=not self.approved)

    def show(self, source_session, target_session) -> None:
        # Define label
        emoji = "✅" if self.approved else "⏳"
        label = f"{emoji} {self.source_chart.config['slug']}"

        with st.expander(label, not self.approved):
            # Approval toggle
            st.toggle(
                label="Approved new chart version",
                key=f"toggle-{self.chart_id}",
                value=self.approved,
                on_change=lambda session=source_session: self.update_state(session),
            )

            # Refresh button (get updated chart from source environment)
            with stylable_container(
                key=f"test-{self.chart_id}",
                css_styles="""
                    button {
                        margin-left: auto;
                        margin-right: 0;
                        text-align: end;
                        text-align: right;
                        flex-direction: row-reverse;

                    }
                    """,
            ):
                st.button(
                    "🔄 Refresh",
                    key=f"refresh-btn-{self.chart_id}",
                    on_click=lambda source_session=source_session, target_session=target_session: self.sync(
                        source_session, target_session
                    ),
                    help="Click on this button to get the latest version of the chart from the source environment.",
                )

            # Chart comparison
            compare_charts(self.source_chart, self.target_chart)


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

    # Get stuff from DB
    source_engine, target_engine = get_engines()
    # TODO: this should be created via migration in owid-grapher!!!!!
    # create chart_diff_approvals table if it doesn't exist
    SQLModel.metadata.create_all(source_engine, [gm.ChartDiffApprovals.__table__])
    # Get IDs from modified charts
    charts_modified_ids = get_modified_chart_ids()
    # chart_ids_new = get_new_chart_ids()
    # explorers_modified = get_modified_explorers()
    # Get actual charts

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            chart_diffs = [
                ChartDiffModified.from_chart_id(
                    chart_id=chart_id,
                    source_session=source_session,
                    target_session=target_session,
                )
                for chart_id in charts_modified_ids
            ]

    # Show information
    st.info(f"There are {len(charts_modified_ids)} chart updates")

    # MODIFIED CHARTS
    st.header("Modified charts")
    st.markdown(f"{len(charts_modified_ids)} charts modified in `{SOURCE_ENV}`")
    with Session(source_engine) as source_engine:
        with Session(target_engine) as target_engine:
            for chart_diff in chart_diffs:
                assert chart_diff.source_chart.id
                chart_diff.show(source_session, target_engine)


main()
