"""Streamlit-based tool for chart upgrader.

Run as `wizard charts` or python -m wizard.pages.charts

The code is structured as follows:

- `__main__.py`: The entrypoint to the app. This is what gets first rendered. From here, we call the rest of the submodules.
- `init_config.py`: Initial configuration of the app. This includes setting up the session states and other app settings.
- `search_config.py`: Dataset search form. This is the first thing we ask the user to fill in. "Which dataset are you updating to which dataset?"
- `variable_config.py`: Variable mapping form. Map variables from the old dataset to variables in the new dataset.
- `submission.py`: Find out the charts affected by the submitted variable mapping. Create the submission.
- `utils.py`: Utility functions.


We use various session state variables to control the flow of the app:

- `submitted_datasets` [default False]: Whether the user has clicked on the first form (dataset form). Shows/hides the steps after the first form.
    - Set to True: When the user submits the first form (Old dataset -> New dataset)
    - Set to False: Never. Once is submitted, something will be shown below. This can be done here bc it is an actual form and changing its fields won't trigger re-runs!
- `submitted_variables` [default False]: Whether the user has submitted the second form (variable mapping form). Controls the creation and preview of the chart revisions.
    - Set to True: When user submits variable mapping form.
    - Set to False: When user submits dataset form. When the user interacts with the variable form changing the mapping (i.e. ignore checkboxes, new variable selectboxes, but NOT the explore toggle)
- `submitted_revisions` [default False]: Whether the user has submitted the chart revisions.
    - Set to True: When the user clicks on "Finish (3/3)" in the third form.
    - Set to False:
"""

import streamlit as st
from structlog import get_logger

from apps.wizard.pages.charts.init_config import init_app, set_session_states
from apps.wizard.pages.charts.search_config import build_dataset_form
from apps.wizard.pages.charts.submission import create_submission, push_submission
from apps.wizard.pages.charts.utils import get_datasets, get_schema
from apps.wizard.pages.charts.variable_config import ask_and_get_variable_mapping
from apps.wizard.utils.env import OWIDEnv
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

# DEBUGGING
# st.write(f"SUBMITTED DATASETS: {st.session_state.submitted_datasets}")
# st.write(f"SUBMITTED VARIALBES: {st.session_state.submitted_variables}")
# st.write(f"SUBMITTED REVISIONS: {st.session_state.submitted_revisions}")
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
# We present the user with the variables from the old and the new dataset.
# The user is asked to define the mapping between these, so that we can create the submission.
#
##########################################################################################
if st.session_state.submitted_datasets:
    # log.info(f"SEARCH FORM: {search_form}")
    variable_config = ask_and_get_variable_mapping(search_form)
    # log.info(f"VARIABLE CONFIG (2): {variable_config}")


##########################################################################################
# 3 CHART REVISIONS BAKING
#
# Once the variable mapping is done, we create the revisions. We store these under what we
# call the "submission_config". This is a dataclass that holds the charts and updaters.
#
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_variables:
    # log.info(f"VARIABLE CONFIG (3): {variable_config}")
    # st.write(variable_config)
    if variable_config is not None:
        if not variable_config.variable_mapping:
            msg_error = "No indicators selected! Please select at least one indicator."
            st.error(msg_error)
        elif variable_config.is_valid:
            submission_config = create_submission(
                variable_config,
                SCHEMA_CHART_CONFIG,
            )
        else:
            st.error("Something went wrong with the submission. Please try again. Report the error #003001")

##########################################################################################
# 4 CHART REVISIONS SUBMISSION
#
# TODO: add description
##########################################################################################
if (
    st.session_state.submitted_datasets
    and st.session_state.submitted_variables
    and st.session_state.submitted_revisions
):
    if submission_config is not None:
        if submission_config.is_valid:
            # st.write(st.session_state.gpt_tweaks)
            push_submission(submission_config)
        else:
            st.error("Something went wrong with the submission. Please try again. Report the error #004001")
