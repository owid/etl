"""Streamlit-based tool for chart revision baker.

Run as `wizard charts` or python -m wizard.pages.charts
"""

import streamlit as st
from structlog import get_logger

from apps.wizard.pages.charts.init_config import init_app, set_session_states
from apps.wizard.pages.charts.search_config import build_dataset_form
from apps.wizard.pages.charts.submission import create_submission, push_submission
from apps.wizard.pages.charts.utils import OWIDEnv, get_datasets, get_schema
from apps.wizard.pages.charts.variable_config import ask_and_get_variable_mapping
from etl.match_variables import SIMILARITY_NAMES

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
submission_config = None

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
    log.info(f"SEARCH FORM: {search_form}")
    variable_config = ask_and_get_variable_mapping(search_form, owid_env)
    log.info(f"VARIABLE CONFIG (2): {variable_config}")
##########################################################################################
# 3 CHART REVISIONS BAKING
#
# TODO: add description
##########################################################################################
if (
    st.session_state.submitted_datasets
    and st.session_state.submitted_variables
    and st.session_state.show_submission_details
):
    log.info(f"VARIABLE CONFIG (3): {variable_config}")
    if variable_config is not None:
        if variable_config.is_valid:
            submission_config = create_submission(variable_config, SCHEMA_CHART_CONFIG)
        else:
            st.error("Something went wrong with the submission. Please try again. Report the error #003001")

##########################################################################################
# 4 CHART REVISIONS SUBMISSION
#
# TODO: add description
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_revisions:
    if submission_config is not None:
        if submission_config.is_valid:
            push_submission(submission_config, owid_env)
        else:
            st.error("Something went wrong with the submission. Please try again. Report the error #004001")
