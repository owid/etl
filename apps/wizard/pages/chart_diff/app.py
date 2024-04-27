from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel
from st_pages import add_indentation

from apps.staging_sync.cli import _get_engine_for_env, _validate_env
from apps.wizard.utils import chart_html
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent
SOURCE_ENV = "staging-site-mojmir"
TARGET_ENV = "staging-site-mojmir"

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
        chart_html(source_chart.config)

    with col2:
        # st.selectbox(label="version", options=["Target"], key=f"selectbox-right-{identifier}")
        st.markdown(f"New version   |   `{target_chart.updatedAt.strftime('%Y-%m-%d %H:%M:%S')}`")
        chart_html(target_chart.config)


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


def update_revision_state(chart_id, title, source_session, source_chart: gm.Chart, target_chart: gm.Chart):
    status = st.session_state[f"toggle-{chart_id}"]

    # Update approval status (in database)
    approval = gm.ChartDiffApprovals(
        chartId=chart_id,
        sourceUpdatedAt=source_chart.updatedAt,
        targetUpdatedAt=target_chart.updatedAt,
        status="approved" if status else "rejected",
    )

    source_session.add(approval)
    source_session.commit()

    # Update expander display
    update_expander(chart_id=chart_id, title=title, expanded=status == "rejected")


def main():
    st.title("Chart ⚡ **:gray[Diff]**")

    source_engine, target_engine = get_engines()

    # TODO: this should be created via migration in owid-grapher!!!!!
    # create chart_diff_approvals table if it doesn't exist
    SQLModel.metadata.create_all(source_engine, [gm.ChartDiffApprovals.__table__])

    chart_ids_modified = get_modified_chart_ids()
    chart_ids_new = get_new_chart_ids()
    # explorers_modified = get_modified_explorers()

    st.markdown(f"There are {len(chart_ids_modified)} chart updates, {len(chart_ids_new)} new charts.")

    st.session_state.expanders = st.session_state.get(
        "expanders",
        {
            chart_id: {
                "label": "",
                "expanded": True,
            }
            for chart_id in [*chart_ids_modified, *chart_ids_new]
        },
    )

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # MODIFIED CHARTS
            st.header("Modified charts")
            st.markdown(f"{len(chart_ids_modified)} charts modified in `{SOURCE_ENV}`")
            for chart_id in chart_ids_modified:
                # Get charts
                source_chart = gm.Chart.load_chart(source_session, chart_id=chart_id)
                target_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)

                assert source_chart.id

                # Get existing approvals
                # TODO: should we highlight if the chart has been explicitly rejected?
                approval_status = gm.ChartDiffApprovals.latest_chart_status(
                    source_session, source_chart.id, source_chart.updatedAt, target_chart.updatedAt
                )

                if approval_status == "approved":
                    update_expander(chart_id, target_chart.config["slug"], expanded=False)

                with st.expander(
                    label=st.session_state.expanders[chart_id]["label"],
                    expanded=st.session_state.expanders[chart_id]["expanded"],
                ):
                    st.toggle(
                        label="Approve new chart version",
                        key=f"toggle-{chart_id}",
                        value=approval_status == "approved",
                        on_change=lambda chart_id=chart_id, target_chart=target_chart: update_revision_state(
                            chart_id=chart_id,
                            title=target_chart.config["slug"],
                            source_session=source_session,
                            source_chart=source_chart,
                            target_chart=target_chart,
                        ),
                    )

                    compare_charts(source_chart, target_chart)

                    # Update list
                    # st.session_state.chart_approval_list.append(
                    #     {
                    #         "id": target_chart.id,
                    #         "approved": False,
                    #         "updated": target_chart.updatedAt,
                    #     }
                    # )

            # NEW CHARTS
            # TODO: fix approvals for new charts
            st.header("New charts")
            st.markdown(f"{len(chart_ids_new)} new charts in `{SOURCE_ENV}`")
            chart_ids_new = get_new_chart_ids()
            for chart_id in chart_ids_new:
                with st.expander(
                    label=st.session_state.expanders[chart_id]["label"],
                    expanded=st.session_state.expanders[chart_id]["expanded"],
                ):
                    st.toggle(
                        label="Approve new chart",
                        key=f"toggle-{chart_id}",
                        on_change=lambda chart_id=chart_id, target_chart=target_chart: update_expander(  # type: ignore
                            chart_id=chart_id,
                            title=target_chart.config["slug"],
                            expanded=st.session_state[f"toggle-{chart_id}"],
                        ),
                    )
                    chart_html(source_chart.config)  # type: ignore


# if __name__ == "__main__":
main()
