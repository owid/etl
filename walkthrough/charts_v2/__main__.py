from urllib.error import URLError

import pandas as pd
import streamlit as st
from MySQLdb import OperationalError
from structlog import get_logger

from etl import config
from etl.chart_revision.v2.core import (
    build_updaters_and_get_charts,
    create_chart_comparison,
    submit_chart_comparisons,
    update_chart_config,
)
from etl.db import get_all_datasets, get_connection, get_variables_in_dataset
from etl.match_variables import (
    SIMILARITY_NAMES,
    find_mapping_suggestions,
    preliminary_mapping,
)
from walkthrough.utils import OWIDEnv

# logger
log = get_logger()


# Functions
@st.cache_data
def get_datasets():
    """Load datasets."""
    with st.spinner("Retrieving datasets..."):
        try:
            datasets = get_all_datasets(archived=False)
        except OperationalError as e:
            raise OperationalError(
                f"Could not retrieve datasets. Try reloading the page. If the error persists, please report an issue. Error: {e}"
            )
        else:
            return datasets.sort_values("name")


def get_variables_from_datasets(dataset_id_1: int, dataset_id_2: int):
    """Get variables from two datasets."""
    with get_connection() as db_conn:
        # Get variables from old dataset that have been used in at least one chart.
        old_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_1, only_used_in_charts=True)
        # Get all variables from new dataset.
        new_variables = get_variables_in_dataset(db_conn=db_conn, dataset_id=dataset_id_2, only_used_in_charts=False)
    return old_variables, new_variables


def _check_env() -> bool:
    """Check if environment variables are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(st.markdown(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?"))

    if ok:
        st.success("`.env` configured correctly")
    return ok


def _show_environment():
    # show variables (from .env)
    st.info(
        f"""
    * **GRAPHER_USER_ID**: `{config.GRAPHER_USER_ID}`
    * **DB_USER**: `{config.DB_USER}`
    * **DB_NAME**: `{config.DB_NAME}`
    * **DB_HOST**: `{config.DB_HOST}`
    """
    )


@st.cache_resource
def _check_env_and_environment():
    ok = _check_env()
    if ok:
        # check that you can connect to DB
        try:
            with st.spinner():
                _ = get_connection()
        except OperationalError as e:
            st.error(
                "We could not connect to the database. If connecting to a remote database, remember to"
                f" ssh-tunel into it using the appropriate ports and then try again.\n\nError:\n{e}"
            )
            ok = False
        except Exception as e:
            raise e
        else:
            msg = "Connection to the Grapher database was successfull!"
            st.success(msg)
            st.subheader("Environment")
            _show_environment()


@st.cache_data
def build_updaters_and_get_charts_cached(variable_mapping):
    return build_updaters_and_get_charts(variable_mapping=variable_mapping)


st.set_page_config(page_title="Chart revisions baker", layout="wide", page_icon="🧑‍🍳")
st.title("🧑‍🍳 Chart revisions baker")
# get dataset
DATASETS = get_datasets()
# build dataset display name
DATASETS["display_name"] = "[" + DATASETS["id"].astype(str) + "] " + DATASETS["name"]
display_name_to_id_mapping = DATASETS.set_index("display_name")["id"].to_dict()
# OWID Env
env = OWIDEnv()
# Session states
if "submitted_datasets" not in st.session_state:
    st.session_state.submitted_datasets = False
if "submitted_variables" not in st.session_state:
    st.session_state.submitted_variables = False
if "submitted_revisions" not in st.session_state:
    st.session_state.submitted_revisions = False
if "charts_obtained" not in st.session_state:
    st.session_state.charts_obtained = False
# Others
old_var_selectbox = []
new_var_selectbox = []
ignore_selectbox = []
charts = []
updaters = []
num_charts = 0

# CONFIGURATION SIDEBAR
with st.sidebar:
    t1, t2 = st.tabs(["Environment", "About this tool"])
    with t1:
        _check_env_and_environment()
    with t2:
        st.markdown(
            """
After the new dataset has been correctly upserted into the database, we need to update the affected charts. This step helps with that. These are the steps (this is all automated):

- The user is asked to choose the _old dataset_ and the _new dataset_.
- The user has to establish a mapping between variables in the _old dataset_ and in the _new dataset_. This mapping tells Grapher how to "replace" old variables with new ones.
- The tool creates chart revisions for all the public charts using variables in the _old dataset_ that have been mapped to variables in the _new dataset_.
- Once the chart revisions are created, you can review these and submit them to the database so that they become available on the _Approval tool_.

Note that this step is equivalent to running `etl-match-variables` and `etl-chart-suggester` commands in terminal. Call them in terminal with option `--help` for more details.
"""
        )

##########################################################################################
# 1 DATASET MAPPING
##########################################################################################
with st.form("form-datasets"):
    st.header(
        "Dataset update",
        help="Variable mapping will be done from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts.",
    )
    col1, col2 = st.columns(2)
    with col1:
        dataset_old = st.selectbox(
            label="Old dataset",
            options=DATASETS["display_name"],
            help="Dataset containing variables to be replaced in our charts.",
        )
    with col2:
        dataset_new = st.selectbox(
            label="New dataset",
            options=DATASETS["display_name"],
            help="Dataset contianinng the new variables. These will replace the old variables in our charts.",
        )
    col0, _, _ = st.columns(3)
    with col0:
        with st.expander("Other parameters"):
            map_identical = st.checkbox("Map identically named variables", value=True)
            similarity_name = st.selectbox(
                label="Similarity matching function",
                options=SIMILARITY_NAMES,
                help="Select the prefered function for matching variables. Find more details at https://www.analyticsvidhya.com/blog/2021/07/fuzzy-string-matching-a-hands-on-guide/",
            )
    submitted_datasets = st.form_submit_button("Submit", type="primary")
    if submitted_datasets:
        st.session_state.submitted_datasets = True
        st.session_state.submitted_variables = False
        st.session_state.submitted_revisions = False
        log.info(
            f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
        )

    # Get IDs of datasets
    dataset_old_id = display_name_to_id_mapping[dataset_old]
    dataset_new_id = display_name_to_id_mapping[dataset_new]


##########################################################################################
# 2 VARIABLE MAPPING
##########################################################################################
if st.session_state.submitted_datasets:
    # 2.1 INTERNAL PROCESSING

    # Get variables from old and new datasets
    old_variables, new_variables = get_variables_from_datasets(dataset_old_id, dataset_new_id)

    # Build display mappings: id -> display_name, display_name -> id
    df = pd.concat([old_variables, new_variables], ignore_index=True)
    df["display_name"] = "[" + df["id"].astype(str) + "] " + df["name"]
    variable_id_to_display = df.set_index("id")["display_name"].to_dict()

    # Get auto variable mapping (if mapping by identical name is enabled)
    mapping, missing_old, missing_new = preliminary_mapping(old_variables, new_variables, map_identical)
    if not mapping.empty:
        variable_mapping_auto = mapping.set_index("id_old")["id_new"].to_dict()
    else:
        variable_mapping_auto = {}
    # Get remaining mapping suggestions
    suggestions = find_mapping_suggestions(missing_old, missing_new, similarity_name)  # type: ignore
    # Sort by max similarity: First suggestion is that one that has the highest similarity score with any of its suggested new vars
    suggestions = sorted(suggestions, key=lambda x: x["new"]["similarity"].max(), reverse=True)

    with st.expander("👷  Mapping details (debugging)"):
        st.subheader("Auto mapping")
        st.write(variable_mapping_auto)
        st.subheader("Suggestions (needs manual mapping)")
        for suggestion in suggestions:
            st.write(suggestion["old"])
            st.write(suggestion["new"])

    # 2.2 DISPLAY MAPPING SECTION
    st.header(
        "Map variables",
        help="Map variables from the old to the new dataset. The idea is that the variables in the new dataset will replace those from the old dataset in our charts. You can choose to ignore some variables if you want to.",
    )
    if not variable_mapping_auto and not suggestions:
        st.warning(
            f"It looks as the dataset [{dataset_old_id}](https://owid.cloud) has no variable in use in any chart! Therefore, no mapping is needed."
        )
    else:
        with st.form("form-variables"):
            col1, col2 = st.columns(2)
            col_1_widths = [5, 1]
            col_2_widths = [5, 1]
            # Left column (old variables)
            with col1:
                st.subheader("Old dataset")
                col11, col12 = st.columns(col_1_widths)
                with col11:
                    st.caption(f"[Explore dataset]({env.admin_url}/datasets/{dataset_old_id}/)")
                with col12:
                    st.caption("Ignore", help="Check to ignore this variable in the mapping.")
            with col2:
                st.subheader("New dataset")
                col21, col22 = st.columns(col_2_widths)
                with col21:
                    st.caption(f"[Explore dataset]({env.admin_url}/datasets/{dataset_new_id}/)")
                with col22:
                    st.caption(
                        "Score",
                        help="Similarity score between the old variable and the 'closest' new variable (from 0 to 100%). Variables with low scores are likely not to have a good match.",
                    )
            old_var_selectbox = []
            ignore_selectbox = []
            new_var_selectbox = []

            # Automatically mapped variables (non-editable)
            for i, (variable_old, variable_new) in enumerate(variable_mapping_auto.items()):
                with st.container():
                    col_auto_1, col_auto_2 = st.columns(2)
                    with col_auto_1:
                        col_auto_11, col_auto_12 = st.columns(col_1_widths)
                        with col_auto_11:
                            element = st.selectbox(
                                label=f"auto-{i}-left",
                                options=[variable_id_to_display[variable_old]],
                                disabled=True,
                                label_visibility="collapsed",
                            )
                            old_var_selectbox.append(element)
                        with col_auto_12:
                            element = st.checkbox("Ignore", key=f"auto-ignore-{i}", label_visibility="collapsed")
                            ignore_selectbox.append(element)
                    with col_auto_2:
                        element = st.selectbox(
                            label=f"auto-{i}-right",
                            options=[variable_id_to_display[variable_new]],
                            disabled=True,
                            label_visibility="collapsed",
                        )
                        new_var_selectbox.append(element)

            # Remaining variables (editable)
            for i, suggestion in enumerate(suggestions):
                with st.container():
                    variable_old = suggestion["old"]["id_old"]
                    similarity_max = int(suggestion["new"]["similarity"].max().round(0))
                    col_manual_1, col_manual_2 = st.columns(2)
                    with col_manual_1:
                        col_manual_11, col_manual_12 = st.columns(col_1_widths)
                        with col_manual_11:
                            element = st.selectbox(
                                label=f"manual-{i}-left",
                                options=[variable_old],
                                disabled=True,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            old_var_selectbox.append(element)
                        with col_manual_12:
                            element = st.checkbox("Ignore", key=f"manual-ignore-{i}", label_visibility="collapsed")
                            ignore_selectbox.append(element)
                    with col_manual_2:
                        col_manual_21, col_manual_22 = st.columns(col_2_widths)
                        with col_manual_21:
                            element = st.selectbox(
                                label=f"manual-{i}-right",
                                options=suggestion["new"]["id_new"],
                                disabled=False,
                                label_visibility="collapsed",
                                format_func=variable_id_to_display.get,
                            )
                            new_var_selectbox.append(element)
                        with col_manual_22:
                            if similarity_max > 80:
                                color = "blue"
                            elif similarity_max > 60:
                                color = "green"
                            elif similarity_max > 40:
                                color = "orange"
                            else:
                                color = "red"
                            st.markdown(f":{color}[**{similarity_max}%**]")

            submitted_variables = st.form_submit_button("Submit", type="primary")
            if submitted_variables:
                st.session_state.submitted_variables = True
                st.session_state.submitted_revisions = False
                log.info(
                    f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
                )

##########################################################################################
# 3 CHART REVISIONS BAKING
##########################################################################################
if st.session_state.submitted_variables:
    st.header("Submission details")
    # BUILD MAPPING
    if len(old_var_selectbox) != len(new_var_selectbox):
        raise ValueError("Something went wrong! The number of old and new variables is different.")
    if len(old_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of old variables and ignore checkboxes is different.")
    if len(new_var_selectbox) != len(ignore_selectbox):
        raise ValueError("Something went wrong! The number of new variables and ignore checkboxes is different.")
    variable_mapping = {
        int(old): int(new)
        for old, new, ignore in zip(old_var_selectbox, new_var_selectbox, ignore_selectbox)
        if not ignore
    }

    # Get updaters and charts to update
    with st.spinner("Retrieving charts to be updated. This can take up to 1 minute..."):
        log.info("chart_revision: building updaters and getting charts!!!!")
        try:
            updaters, charts = build_updaters_and_get_charts_cached(variable_mapping=variable_mapping)
        except URLError as e:
            st.error(e)
            error_submitting_variables = True
        else:
            error_submitting_variables = False
        # st.session_state.charts_obtained = True
    if not error_submitting_variables:
        # Display details
        num_charts = len(charts)
        with st.container():
            st.info(f"""Number of charts to be updated: {num_charts}""")
        with st.expander("🔎  Show variable id mapping"):
            st.write(variable_mapping)
        with st.expander("📊  Show affected charts (before update)"):
            for chart in charts:
                slug = chart.config["slug"]
                st.markdown(
                    f"""<iframe src="https://ourworldindata.org/grapher/{slug}" loading="lazy" style="width: 100%; height: 600px; border: 0px none;"></iframe>""",
                    unsafe_allow_html=True,
                )

        # Button to finally submit the revisions
        submitted_revisions = st.button(
            label="🚀 CREATE AND SUBMIT CHART REVISIONS", use_container_width=True, type="primary"
        )
        if submitted_revisions:
            st.session_state.submitted_revisions = True
            log.info(
                f"{st.session_state.submitted_datasets}, {st.session_state.submitted_variables}, {st.session_state.submitted_revisions}"
            )
        st.divider()

##########################################################################################
# 4 CHART REVISIONS SUBMISSION
##########################################################################################
if st.session_state.submitted_revisions:
    # Create chart comparisons
    progress_text = "Creating chart revisions..."
    bar = st.progress(0, progress_text)
    comparisons = []
    for i, chart in enumerate(charts):
        log.info(f"chart_revision: creating comparison for chart {chart.id}")
        # Update chart config
        config_new = update_chart_config(chart.config, updaters)
        # Create chart comparison and add to list
        comparison = create_chart_comparison(chart.config, config_new)
        comparisons.append(comparison)
        # Show progress bar
        percent_complete = int(100 * (i + 1) / num_charts)
        bar.progress(percent_complete, text=f"{progress_text} {percent_complete}%")

    # Submit chart comparisons
    try:
        submit_chart_comparisons(comparisons)
    except Exception as e:
        st.error(f"Something went wrong! {e}")
    else:
        st.balloons()
        st.success(
            f"Chart revisions submitted successfully! Now review these at the [approval tool]({env.chart_approval_tool_url})!"
        )
