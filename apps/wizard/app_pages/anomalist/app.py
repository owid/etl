import pandas as pd
import streamlit as st

from apps.wizard.utils import cached
from apps.wizard.utils.components import grapher_chart, st_horizontal

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Anomalist",
    page_icon="🪄",
    layout="wide",
)
# OTHER CONFIG
ANOMALY_TYPES = [
    {
        "title": "Time change",
        "color": "orange",
        "icon": ":material/timeline",
    },
    {
        "title": "Version change",
        "color": "blue",
        "icon": ":material/upgrade",
    },
    {
        "title": "Missing point",
        "color": "red",
        "icon": ":material/hide_source",
    },
    {
        "title": "AI",
        "color": "rainbow",
        "icon": ":material/lightbulb",
    },
]
ANOMALY_TYPE_NAMES = [a["title"] for a in ANOMALY_TYPES]
ANOMALY_TYPE_DISPLAY = {a["title"]: f":{a['color']}-background[{a['icon']}: {a['title']}]" for a in ANOMALY_TYPES}
#
# SESSION STATE
st.session_state.datasets_selected = st.session_state.get("datasets_selected", [])
st.session_state.indicators = st.session_state.get("indicators", [])

st.session_state.anomalist_filter_entities = st.session_state.get("anomalist_filter_entities", [])
st.session_state.anomalist_filter_indicators = st.session_state.get("anomalist_filter_indicators", [])

# DEBUGGING
ENTITIES = [
    "Afghanistan",
    "Albania",
    "Algeria",
]
YEAR_MIN = 1950
YEAR_MAX = 2021
ANOMALIES = [
    {
        "title": "Coal consumption - Malaysia - 1983",
        "description": "There are 12 missing points that used to be informed in the previous version",
        "category": "Missing point",
        "country": "Malaysia",
        "year": 1983,
    },
    {
        "title": "Gas production - Ireland - 2000",
        "description": "There are 2 abrupt changes in the time series.",
        "category": "Time change",
        "country": "Ireland",
        "year": 2000,
    },
    {
        "title": "Nuclear production - France - 2010",
        "description": "There is 1 abrupt changes in the time series.",
        "category": "AI",
        "country": "France",
        "year": 2010,
    },
]
ANOMALIES = ANOMALIES + ANOMALIES + ANOMALIES + ANOMALIES
DATASETS_DEBUG = ["grapher/energy/2024-06-20/energy_mix"]

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
        options=cached.load_dataset_uris(),
        max_selections=1,
        default=DATASETS_DEBUG,
    )

    st.form_submit_button("Detect anomalies", type="primary")


# st.session_state.datasets_selected = DATASETS_DEBUG

# FILTER PARAMS
with st.container(border=True):
    st.markdown("##### Select filters")
    indicator_uris = []
    if len(st.session_state.datasets_selected) > 0:
        st.session_state.indicators = cached.load_variables_in_dataset(
            st.session_state.datasets_selected,
        )
        indicator_uris = cached.get_variable_uris(st.session_state.indicators, True)

    col1, col2 = st.columns([10, 2])
    # Indicator
    with col1:
        st.session_state.anomalist_filter_indicators = st.multiselect(
            label="Indicators",
            options=indicator_uris,
            help="Show anomalies affecting only a selection of indicators.",
        )

    with col2:
        # Entity
        st.session_state.anomalist_filter_entities = st.multiselect(
            label="Entities",
            options=ENTITIES,
            help="Show anomalies affecting only a selection of entities.",
        )

    # Anomaly type
    col1, col2 = st.columns([10, 3])
    with col1:
        st.slider(
            label="Years",
            min_value=YEAR_MIN,
            max_value=YEAR_MAX,
            value=(YEAR_MIN, YEAR_MAX),
            help="Show anomalies occuring in a particular time range.",
        )
    with col2:
        col21, col22 = st.columns(2)
        with col21:
            # Anomaly sorting
            st.multiselect(
                label="Anomaly type",
                options=ANOMALY_TYPE_NAMES,
                # default=ANOMALY_TYPES,
            )
        with col22:
            # Anomaly sorting
            st.multiselect(
                label="Sort by",
                options=[
                    "Anomaly score",
                    "Population",
                    "Chart views",
                ],
            )

    # st.multiselect("Anomaly type", min_value=0.0, max_value=1.0, value=0.5, step=0.01)
    # st.number_input("Minimum score", min_value=0.0, max_value=1.0, value=0.5, step=0.01)

# SHOW ANOMALIES
data = {
    "anomaly": ["Anomaly 1", "Anomaly 2", "Anomaly 3"],
    "description": ["Description 1", "Description 2", "Description 3"],
}


# SHOW ANOMALIES
def show_anomaly(anomaly, indicator):
    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(ANOMALY_TYPE_DISPLAY[anomaly["category"]])
            st.markdown(f"##### {anomaly['title']}")
            st.markdown(f"{anomaly['description']}")
        with col2:
            # st.write(indicator.id)
            grapher_chart(variable_id=indicator.id, selected_entities=[anomaly["country"]])


def trigger_dialog_for_df_selection(df: pd.DataFrame):
    if len(st.session_state.anomalies["selection"]["rows"]) > 0:
        # Get selected row number
        row_num = st.session_state.anomalies["selection"]["rows"][0]
        # Get indicator id
        indicator_id = df.index[row_num]
        action(indicator_id)


@st.dialog("Show anomaly", width="large")
def action(indicator_id):
    grapher_chart(variable_id=indicator_id)


# If any indicator is given, show the anomalies
if len(st.session_state.indicators) > 0:
    for index, anomaly in enumerate(ANOMALIES):
        # Pic random indicator
        indicator = st.session_state.indicators[index * 3]
        show_anomaly(ANOMALIES[index], indicator)
    # df = pd.DataFrame(
    #     {
    #         "indicator_id": [i.id for i in st.session_state.indicators],
    #         "reviewed": [False for i in st.session_state.indicators],
    #     },
    # ).set_index("indicator_id")

    # st.dataframe(
    #     df,
    #     key="anomalies",
    #     selection_mode="single-row",
    #     on_select=lambda df=df: trigger_dialog_for_df_selection(df),
    #     use_container_width=True,
    # )
