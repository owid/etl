"""Streamlit-based tool for chart upgrader.

The code is structured as follows:

- `__main__.py`: The entrypoint to the app. This is what gets first rendered. From here, we call the rest of the submodules.
- `init_config.py`: Other app Initial configuration of the app. This includes setting up the session states andsettings.
- `search_config.py`: Dataset search form. This is the first thing we ask the user to fill in. "Which dataset are you updating?"
- `indicator_config.py`: Indicator mapping form. Map indicators from the old dataset to indicators in the new dataset.
- `submission.py`: Find out the charts affected by the submitted indicator mapping. Create the submission.
- `utils.py`: Utility functions.


We use various session state indicators to control the flow of the app:

- `submitted_datasets` [default False]: Whether the user has clicked on the first form (dataset form). Shows/hides the steps after the first form.
    - Set to True: When the user submits the first form (Old dataset -> New dataset)
    - Set to False: Never. Once is submitted, something will be shown below. This can be done here bc it is an actual form and changing its fields won't trigger re-runs!
- `submitted_indicators` [default False]: Whether the user has submitted the second form (indicator mapping form). Controls the creation and preview of the chart revisions.
    - Set to True: When user submits indicator mapping form.
    - Set to False: When user submits dataset form. When the user interacts with the indicator form changing the mapping (i.e. ignore checkboxes, new indicator selectboxes, but NOT the explore toggle)
- `submitted_revisions` [default False]: Whether the user has submitted the chart revisions.
    - Set to True: When the user clicks on "Finish (3/3)" in the third form.
    - Set to False:
"""

import streamlit as st
from structlog import get_logger

from apps.wizard.pages.charts_v2.indicator_config import ask_and_get_variable_mapping
from apps.wizard.pages.charts_v2.init_config import init_app, set_session_states
from apps.wizard.pages.charts_v2.search_config import build_dataset_form
from apps.wizard.pages.charts_v2.submission import create_submission, push_submission
from apps.wizard.pages.charts_v2.utils import get_datasets, get_schema
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
indicator_config = None
submission_config = None

# DEBUGGING
# st.write(f"SUBMITTED DATASETS: {st.session_state.submitted_datasets}")
# st.write(f"SUBMITTED VARIALBES: {st.session_state.submitted_indicators}")
# st.write(f"SUBMITTED REVISIONS: {st.session_state.submitted_revisions}")
##########################################################################################
# 1 DATASET MAPPING
#
# Presents the user with a form to select the old and new datasets. Additionally,
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
    indicator_config = ask_and_get_variable_mapping(search_form)
    # log.info(f"VARIABLE CONFIG (2): {indicator_config}")


##########################################################################################
# 3 CHART REVISIONS BAKING
#
# Once the variable mapping is done, we create the revisions. We store these under what we
# call the "submission_config". This is a dataclass that holds the charts and updaters.
#
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_indicators:
    # log.info(f"VARIABLE CONFIG (3): {indicator_config}")
    # st.write(reset_indicator_form)
    if indicator_config is not None:
        if not indicator_config.variable_mapping:
            msg_error = "No indicators selected! Please select at least one indicator."
            st.error(msg_error)
        elif indicator_config.is_valid:
            submission_config = create_submission(
                indicator_config,
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
    and st.session_state.submitted_indicators
    and st.session_state.submitted_revisions
):
    if submission_config is not None:
        if submission_config.is_valid:
            # st.write(st.session_state.gpt_tweaks)
            push_submission(submission_config)
        else:
            st.error("Something went wrong with the submission. Please try again. Report the error #004001")
