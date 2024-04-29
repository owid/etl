"""Streamlit-based tool for chart upgrader.

The code is structured as follows:

- `__main__.py`: The entrypoint to the app. This is what gets first rendered. From here, we call the rest of the submodules.
- `init_config.py`: Other app Initial configuration of the app. This includes setting up the session states andsettings.
- `dataset_selection.py`: Dataset search form. This is the first thing we ask the user to fill in. "Which dataset are you updating?"
- `indicator_mapping.py`: Indicator mapping form. Map indicators from the old dataset to indicators in the new dataset.
- `charts_update.py`: Find out the charts affected by the submitted indicator mapping. Create the submission.
- `utils.py`: Utility functions.


We use various session state indicators to control the flow of the app:

- `submitted_datasets` [default False]: Whether the user has clicked on the first form (dataset form). Shows/hides the steps after the first form.
    - Set to True: When the user submits the first form (Old dataset -> New dataset)
    - Set to False: Never. Once is submitted, something will be shown below. This can be done here bc it is an actual form and changing its fields won't trigger re-runs!
- `submitted_indicators` [default False]: Whether the user has submitted the second form (indicator mapping form). Controls the creation and preview of the updated charts.
    - Set to True: When user submits indicator mapping form.
    - Set to False: When user submits dataset form. When the user interacts with the indicator form changing the mapping (i.e. ignore checkboxes, new indicator selectboxes, but NOT the explore toggle)
- `submitted_charts` [default False]: Whether the user has submitted the updated charts.
    - Set to True: When the user clicks on "Finish (3/3)" in the third form.
    - Set to False:
"""

import streamlit as st
from st_pages import add_indentation
from structlog import get_logger

from apps.wizard import utils
from apps.wizard.pages.indicator_upgrade.charts_update import get_affected_charts_and_preview, push_new_charts
from apps.wizard.pages.indicator_upgrade.dataset_selection import build_dataset_form
from apps.wizard.pages.indicator_upgrade.indicator_mapping import ask_and_get_indicator_mapping
from apps.wizard.pages.indicator_upgrade.utils import get_datasets, get_schema
from apps.wizard.utils.env import OWIDEnv
from etl.match_variables import SIMILARITY_NAMES

# logger
log = get_logger()

# Main app settings
st.set_page_config(
    page_title="Wizard: Indicator Upgrader",
    layout="wide",
    page_icon="🪄",
    initial_sidebar_state="collapsed",
    menu_items={
        "Report a bug": "https://github.com/owid/etl/issues/new?assignees=marigold%2Clucasrodes&labels=wizard&projects=&template=wizard-issue---.md&title=wizard%3A+meaningful+title+for+the+issue",
        "About": """
    After a new dataset has been added to our database, we need to update the affected charts. These are the steps:
    - Select the _old dataset_ and the _new dataset_.
    - Map old indicators in the _old dataset_ to their corresponding new versions in the _new dataset_. This mapping tells Grapher how to "replace" old indicators with new ones.
    - Review the mapping.
    - Update all chart references
    """,
    },
)
add_indentation()
st.title("Indicator 🧬 **:gray[Upgrader]**")
st.markdown("Update indicators to their new versions.")  # Get datasets (might take some time)
DATASETS = get_datasets()
# Get schema
SCHEMA_CHART_CONFIG = get_schema()
# OWID Env
owid_env = OWIDEnv()
# Session states
utils.set_states(
    {
        "submitted_datasets": False,
        "submitted_indicators": False,
        "submitted_charts": False,
        "indicator_mapping": {},
    },
    only_if_not_exists=True,
)
# Avoid "unbound" errors
old_var_selectbox = []
new_var_selectbox = []
ignore_selectbox = []
charts = []
updaters = []
num_charts = 0
indicator_mapping = {}
indicator_config = None
submission_config = None

# DEBUGGING
# st.write(f"SUBMITTED DATASETS: {st.session_state.submitted_datasets}")
# st.write(f"SUBMITTED VARIALBES: {st.session_state.submitted_indicators}")
# st.write(f"SUBMITTED REVISIONS: {st.session_state.submitted_charts}")
##########################################################################################
# 1 DATASET MAPPING
#
# Presents the user with a form to select the old and new datasets. Additionally,
# some search paramterers can be configured. The dataset IDs are used to retrieve the
# relevant indicators from the database/S3
#
##########################################################################################
with st.form("form-datasets"):
    search_form = build_dataset_form(DATASETS, SIMILARITY_NAMES)


##########################################################################################
# 2 INDICATORS MAPPING
#
# We present the user with the indicators from the old and the new dataset.
# The user is asked to define the mapping between these, so that we can create the submission.
#
##########################################################################################
if st.session_state.submitted_datasets:
    # log.info(f"SEARCH FORM: {search_form}")
    indicator_config = ask_and_get_indicator_mapping(search_form)
    # log.info(f"INDICATORS CONFIG (2): {indicator_config}")


##########################################################################################
# 3 GET CHARTS
#
# Once the indicator mapping is done, we retrieve the affected charts (those that rely on
# the indicators in the mapping. create the revisions. We store these under what we
# call the "submission_config". This is a dataclass that holds the charts and updaters.
#
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_indicators:
    # log.info(f"INDICATOR CONFIG (3): {indicator_config}")
    # st.write(reset_indicator_form)
    if indicator_config is not None:
        if not indicator_config.indicator_mapping:
            msg_error = "No indicators selected! Please select at least one indicator."
            st.error(msg_error)
        elif indicator_config.is_valid:
            charts = get_affected_charts_and_preview(
                indicator_config.indicator_mapping,
            )
        else:
            "Something went wrong when trying to update the charts and pushing them to the database. Please try again or report the error #003001"

##########################################################################################
# 4 UPDATE CHARTS
#
# TODO: add description
##########################################################################################
if st.session_state.submitted_datasets and st.session_state.submitted_indicators and st.session_state.submitted_charts:
    if isinstance(charts, list) and len(charts) > 0:
        try:
            push_new_charts(charts, SCHEMA_CHART_CONFIG)
        except Exception:
            st.error(
                "Something went wrong when trying to update the charts and pushing them to the database. Please try again or report the error #004001"
            )
