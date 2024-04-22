import json
import time
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components
from sqlmodel import Session
from st_pages import add_indentation

from apps.staging_sync.cli import _get_engine_for_env, _validate_env
from apps.wizard.utils import chart_html
from etl import grapher_model as gm

# from apps.wizard import utils as wizard_utils

# wizard_utils.enable_bugsnag_for_streamlit()

CURRENT_DIR = Path(__file__).resolve().parent


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


def compare_charts(source_chart, target_chart):
    # Create two columns for the iframes
    col1, col2 = st.columns(2)

    with col1:
        st.write("Source")
        chart_html(source_chart.config)

    with col2:
        st.write("Target")
        chart_html(target_chart.config)


def main():
    t = time.time()

    st.title("Chart 🔄 **:gray[Diff]**")
    st.markdown(
        """\
    TODO

    🔨 TODO
    """
    )

    st.markdown("### Config")
    source = st.text_input("Source", placeholder="my-branch", value="staging-site-mojmir")
    target = st.text_input("Target", value=".env.prod")

    source = Path(source)
    target = Path(target)

    _validate_env(source)
    _validate_env(target)

    source_engine = _get_engine_for_env(source)
    target_engine = _get_engine_for_env(target)

    tab_summary, tab_new_charts, tab_updated_charts, tab_chart_conflicts = st.tabs(
        ["Summary", "New charts", "Updated charts", "Chart conflicts"]
    )

    with tab_summary:
        st.markdown(
            """
        ### Summary
        +1 new chart

        ~3 updated charts
        """
        )

    with tab_new_charts:
        slugs = [
            "food-supply-vs-life-expectancy",
            "gender-wage-gap-vs-gdp-per-capita-income-groups",
            "time-spent-sports",
            "energy-use-per-person-vs-gdp-per-capita",
            "private-health-expenditure-per-person",
            "infant-mortality-vs-prenatal-care",
            "share-of-adults-who-are-overweight",
            "annual-percentage-change-in-solar-and-wind-energy-generation",
            "water-withdrawals-per-kg-poore",
            "iwc-status",
            "the-number-of-new-tetanus-infections-by-world-region",
        ]

        with Session(source_engine) as source_session:
            with Session(target_engine) as target_session:
                for slug in slugs:
                    st.markdown(f"### {slug}")
                    source_chart = gm.Chart.load_chart(source_session, slug=slug)
                    target_chart = gm.Chart.load_chart(target_session, slug=slug)

                    compare_charts(source_chart, target_chart)


# if __name__ == "__main__":
main()
