"""Streamlit-based tool for chart revision baker.

Run as `walkthrough charts` or python -m walkthrough.charts_v2
"""

import streamlit as st
from structlog import get_logger

from etl.match_variables import SIMILARITY_NAMES
from walkthrough.charts_v2.init_config import init_app, set_session_states
from walkthrough.charts_v2.search_config import build_dataset_form
from walkthrough.charts_v2.submission import create_submission, push_submission
from walkthrough.charts_v2.utils import get_datasets, get_schema
from walkthrough.charts_v2.variable_config import ask_and_get_variable_mapping
from walkthrough.utils import OWIDEnv

# logger
log = get_logger()


# Main app settings
init_app()
# Get datasets (might take some time)
DATASETS = get_datasets()
# Get schema
SCHEMA_CHART_CONFIG = get_schema()
# OWID Env
owid_env = OWIDEnv()
# Session states
set_session_states()

# Avoid "unbound" errors
old_var_selectbox = []
new_var_selectbox = []
ignore_selectbox = []
charts = []
updaters = []
num_charts = 0
variable_mapping = {}
variable_config = None

##########################################################################################
# 1 DATASET MAPPING
#
# Presents the user with the a form to select the old and new datasets. Additionally,
# some search paramterers can be configured. The dataset IDs are used to retrieve the
# relevant variables from the database/S3
#
##########################################################################################
with st.form("form-datasets"):
    search_form = build_dataset_form(DATASETS, SIMILARITY_NAMES)


##########################################################################################
# 2 VARIABLE MAPPING
#
# TODO: add description
##########################################################################################
if st.session_state.submitted_datasets:
    variable_config = ask_and_get_variable_mapping(search_form, owid_env)

##########################################################################################
# 3 CHART REVISIONS BAKING
#
# TODO: add description
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.show_submission_details:
    st.header("Submission details")
    if variable_config is not None:
        submission_config = create_submission(variable_config, SCHEMA_CHART_CONFIG)


##########################################################################################
# 4 CHART REVISIONS SUBMISSION
#
# TODO: add description
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_revisions:
    if submission_config is not None and submission_config.is_valid:
        push_submission(submission_config, owid_env)
