import pandas as pd
import streamlit as st

from apps.wizard.utils.components import grapher_chart, st_horizontal
from apps.wizard.utils.dataset import load_datasets_uri_from_db
from apps.wizard.utils.indicator import load_indicator_uris_from_db

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
)
# OTHER CONFIG
ANOMALY_TYPES = [
    "Upgrade",
    "Abrupt change",
    "Context change",
]

# SESSION STATE
st.session_state.datasets_selected = st.session_state.get("datasets_selected", [])
st.session_state.filter_indicators = st.session_state.get("filter_indicators", [])
st.session_state.indicators = st.session_state.get("indicators", [])

# PAGE TITLE
st.title(":material/planner_review: Anomalist")


# DATASET SEARCH
st.markdown(
    """
    <style>
       .stMultiSelect [data-baseweb=select] span{
            max-width: 1000px;
        }
    </style>""",
    unsafe_allow_html=True,
)
with st.form(key="dataset_search"):
    st.session_state.datasets_selected = st.multiselect(
        "Select datasets",
        options=load_datasets_uri_from_db(),
        max_selections=1,
    )

    st.form_submit_button("Detect anomalies", type="primary")


# FILTER PARAMS
with st.container(border=True):
    st.markdown("##### Filter Parameters")
    options = []
    if len(st.session_state.datasets_selected) > 0:
        st.session_state.indicators = load_indicator_uris_from_db(st.session_state.datasets_selected)
        options = [o.catalogPath for o in st.session_state.indicators]

    st.session_state.filter_indicators = st.multiselect(
        label="Indicator",
        options=options,
    )

    with st_horizontal():
        st.session_state.filter_indicators = st.multiselect(
            label="Indicator type",
            options=["New indicator", "Indicator upgrade"],
        )
        st.session_state.filter_indicators = st.multiselect(
            label="Anomaly type",
            options=ANOMALY_TYPES,
        )

        # st.multiselect("Anomaly type", min_value=0.0, max_value=1.0, value=0.5, step=0.01)
        st.number_input("Minimum score", min_value=0.0, max_value=1.0, value=0.5, step=0.01)

# SHOW ANOMALIES
data = {
    "anomaly": ["Anomaly 1", "Anomaly 2", "Anomaly 3"],
    "description": ["Description 1", "Description 2", "Description 3"],
}


# SHOW ANOMALIES
def show_anomaly(df: pd.DataFrame):
    if len(st.session_state.anomalies["selection"]["rows"]) > 0:
        # Get selected row number
        row_num = st.session_state.anomalies["selection"]["rows"][0]
        # Get indicator id
        indicator_id = df.index[row_num]
        action(indicator_id)


@st.dialog("Show anomaly", width="large")
def action(indicator_id):
    grapher_chart(variable_id=indicator_id)


if len(st.session_state.indicators) > 0:
    df = pd.DataFrame(
        {
            "indicator_id": [i.id for i in st.session_state.indicators],
            "reviewed": [False for i in st.session_state.indicators],
        },
    ).set_index("indicator_id")

    st.dataframe(
        df,
        key="anomalies",
        selection_mode="single-row",
        on_select=lambda df=df: show_anomaly(df),
        use_container_width=True,
    )
