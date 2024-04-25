from pathlib import Path

import streamlit as st
from sqlalchemy.engine.base import Engine
from sqlmodel import Session
from st_pages import add_indentation

from apps.staging_sync.cli import _get_engine_for_env, _validate_env
from apps.wizard.utils import chart_html
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent
SOURCE_ENV = "staging-site-mojmir"
TARGET_ENV = "staging-site-mojmir"

st.session_state.chart_approval_list = st.session_state.get("chart_approval_list", [])


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
        2005,
    ]
    return chart_ids


def get_new_chart_ids():
    chart_ids = [
        3000,
        3001,
    ]
    return chart_ids


def compare_charts(
    source_chart,
    target_chart,
):
    # Create two columns for the iframes
    col1, col2 = st.columns(2)

    prod_is_newer = source_chart.updatedAt > target_chart.updatedAt

    with col1:
        # st.selectbox(label="version", options=["Source"], key=f"selectbox-left-{identifier}")
        if not prod_is_newer:
            st.markdown("Production :red[(⚠️was modified)]")
        else:
            st.markdown("Production")
        chart_html(source_chart.config)

    with col2:
        # st.selectbox(label="version", options=["Target"], key=f"selectbox-right-{identifier}")
        st.markdown("New version")
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


def update_expander(chart_id, title):
    st.session_state.expanders[chart_id] = {
        "label": f"✅ {title}" if st.session_state.expanders[chart_id]["label"] == "" else "",
        "expanded": not st.session_state.expanders[chart_id]["expanded"],
    }


def main():
    st.title("Chart ⚡ **:gray[Diff]**")

    source_engine, target_engine = get_engines()

    chart_ids_modified = get_modified_chart_ids()
    st.session_state.expanders = st.session_state.get(
        "expanders",
        {
            chart_id: {
                "label": "",
                "expanded": True,
            }
            for chart_id in chart_ids_modified
        },
    )

    with Session(source_engine) as source_session:
        with Session(target_engine) as target_session:
            # MODIFIED CHARTS
            st.markdown(f"{len(chart_ids_modified)} charts modified in {SOURCE_ENV}")
            for chart_id in chart_ids_modified:
                with st.expander(
                    label=st.session_state.expanders[chart_id]["label"],
                    expanded=st.session_state.expanders[chart_id]["expanded"],
                ):
                    # Get charts
                    source_chart = gm.Chart.load_chart(source_session, chart_id=chart_id)
                    target_chart = gm.Chart.load_chart(target_session, chart_id=chart_id)

                    # with st.expander(label=f"## {slug}", expanded=True):

                    st.toggle(
                        label="Approve chart",
                        key=f"toggle-{chart_id}",
                        on_change=lambda chart_id=chart_id, target_chart=target_chart: update_expander(
                            chart_id=chart_id,
                            title=target_chart.config["slug"],
                        ),
                    )

                    compare_charts(source_chart, target_chart)

                    # Update list
                    st.session_state.chart_approval_list.append(
                        {
                            "id": target_chart.id,
                            "approved": False,
                            "updated": target_chart.updatedAt,
                        }
                    )

            # NEW CHARTS
            st.markdown(f"{len(chart_ids_modified)} new charts in {SOURCE_ENV}")
            chart_ids_new = get_new_chart_ids()
            for chart_id in chart_ids_new:
                chart_html(source_chart.config)


# if __name__ == "__main__":
main()
