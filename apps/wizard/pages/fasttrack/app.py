"""Fast-track import FRONT-END (WIP).

TODO: Fully decouple front-end from backend (see fasttrack.utils for back-end, i.e. should not have streamlit calls).
"""
from pathlib import Path

import pandas as pd
import streamlit as st
from st_pages import add_indentation
from structlog import get_logger

from apps.utils.files import generate_step_to_channel
from apps.wizard import utils as wizard_utils
from apps.wizard.pages.fasttrack.load import load_existing_sheets_from_snapshots
from apps.wizard.pages.fasttrack.process import processing_part_1, processing_part_2
from apps.wizard.pages.fasttrack.utils import (
    FERNET_KEY,
    IMPORT_GSHEET,
    LOCAL_CSV,
    UPDATE_GSHEET,
    set_states,
)
from etl import config
from etl.command import main as etl_main
from etl.paths import DAG_DIR

# Page config
st.set_page_config(page_title="Wizard: Import data via Fast-Track", page_icon="🪄")
add_indentation()


# Reset states
def reset_states() -> None:
    """Reset states so nothing is executed (only first form is shown)."""
    set_states(
        {
            "to_be_submitted": False,
            "to_be_submitted_confirmed_1": False,
            "to_be_submitted_confirmed_2": False,
        }
    )


# CONFIG
CURRENT_DIR = Path(__file__).parent
DAG_FASTTRACK_PATH = DAG_DIR / "fasttrack.yml"
# Config style
wizard_utils.config_style_html()
# Logger
log = get_logger()
# Bugsnag
wizard_utils.enable_bugsnag_for_streamlit()


# Initialize session state
st.session_state["to_be_submitted"] = st.session_state.get("to_be_submitted", False)
st.session_state["to_be_submitted_confirmed_1"] = st.session_state.get("to_be_submitted_confirmed_1", False)
st.session_state["to_be_submitted_confirmed_2"] = st.session_state.get("to_be_submitted_confirmed_2", False)
st.session_state["fast_import"] = st.session_state.get("fast_import", None)
# App state
st.session_state["step_name"] = "fasttrack"
APP_STATE = wizard_utils.AppState()

##########################################################
# MAIN ###################################################
##########################################################
# TITLE & description
st.title("Import data via  **:gray[Fast-Track]**")
st.markdown(
    """
            Fast-track is a tool for importing datasets from Google Sheets. The idea is to keep all data and metadata there, and use this interface to import or update the data in grapher database where it can be used to create charts. Fast-track also commits your work to [ETL repository](https://github.com/owid/etl) where you can further process your data with Python.
"""
)
# Warning (if needed)
# st.warning("This tool is still in beta. Please report any issues to @Mojmir.")

##########################################################
# IMPORT method: there are three different ways to import a dataset via Fast-Track:
# 1. Import from a Google sheet
# 2. Update from an existing Google sheet
# 3. Import from a local CSV
##########################################################
IMPORT_OPTIONS = {
    IMPORT_GSHEET: {
        "title": "New Google sheet",
        "description": "Import data from a Google sheet",
        "guidelines": {
            "heading": "**How to import from a Google sheet**",
            "file_path": CURRENT_DIR / "markdown" / "fasttrack_gsheet_import.md",
        },
    },
    UPDATE_GSHEET: {
        "title": "Existing Google sheet",
        "description": "Update from a Google sheet (already imported in the database)",
        "guidelines": {
            "heading": "**How to update from an existing sheet**",
            "file_path": CURRENT_DIR / "markdown" / "fasttrack_gsheet_update.md",
        },
    },
    LOCAL_CSV: {
        "title": "Local CSV",
        "description": "Import from a local CSV",
        "guidelines": {
            "heading": "**How to import a local CSV**",
            "file_path": CURRENT_DIR / "markdown" / "fasttrack_csv.md",
        },
    },
}
# Choose import method
import_method = st.radio(
    label="How do you want to import the dataset?",
    options=IMPORT_OPTIONS.keys(),
    captions=[IMPORT_OPTIONS[x]["description"] for x in IMPORT_OPTIONS],
    format_func=lambda x: IMPORT_OPTIONS[x]["title"],
    help="Select the source of your data.",
    horizontal=True,
)
# Show brief guidelines for each import method
for option, option_params in IMPORT_OPTIONS.items():
    if import_method == option:
        with st.expander(f"More info: {option_params['guidelines']['heading']}", expanded=False):
            with open(option_params["guidelines"]["file_path"], "r") as f:
                st.markdown(f.read())


##########################################################
# CREATE AND SHOW THE FORM
##########################################################
with st.form("fasttrack-form"):
    existing_google_sheet = None
    placeholder_for_existing_google_sheet = None

    # Import field
    if import_method == IMPORT_GSHEET:
        st.text_input(
            label="New Google Sheets URL",
            help="In the Google spreadsheet, click on `File -> Share -> Publish to Web` and share the entire document as csv.",
            key="dataset_uri",
        )
    elif import_method == UPDATE_GSHEET:
        options = load_existing_sheets_from_snapshots()
        placeholder_for_existing_google_sheet = st.empty()
    else:
        st.file_uploader(
            label="Upload Local CSV",
            type=["csv"],
            help="Upload a local CSV file to import data.",
            key="dataset_uri",
        )
    # Other parameters
    st.checkbox(
        label="Infer missing metadata (instead of raising an error)",
        value=True,
        key="infer_metadata",
    )

    placeholder_for_private = st.empty()

    submitted = st.form_submit_button(
        "Submit",
        type="primary",
        use_container_width=True,
        on_click=lambda: set_states(
            {
                "to_be_submitted": True,
                "to_be_submitted_confirmed_1": False,
                "to_be_submitted_confirmed_2": False,
            }
        ),
    )

# These need to be defined outside of the form to be able to make the `is_public` checkbox dependent
# on the `existing_google_sheet` value
if import_method == UPDATE_GSHEET:
    if placeholder_for_existing_google_sheet is None:
        raise ValueError("placeholder_for_existing_google_sheet is None. This was not expected.")
    else:
        with placeholder_for_existing_google_sheet:
            existing_google_sheet = st.selectbox(
                label="Existing Google Sheets",
                options=options,  # type: ignore
                format_func=lambda x: x["label"],
                help="Selected sheet will be used if you don't specify Google Sheets URL.",
                key="dataset_uri",
                on_change=reset_states,
            )

with placeholder_for_private:
    if existing_google_sheet:
        default_is_public = existing_google_sheet["is_public"]
    else:
        default_is_public = True

    st.checkbox(
        label="Make dataset private (your metadata will be still public!)",
        value=not default_is_public,
        key="fasttrack_is_private",
        on_change=reset_states,
    )

if (FERNET_KEY is None) and (st.session_state.fasttrack_is_private):
    if import_method == UPDATE_GSHEET:
        st.error(
            "FASTTRACK_SECRET_KEY not found in environment variables. Not using encryption. Therefore, won't be able to decrypt the existing Google Sheets URL for private datasets!"
        )
        st.stop()
    else:
        st.warning("FASTTRACK_SECRET_KEY not found in environment variables. Not using encryption.")

##########################################################
# USER CLICKS ON SUBMIT
##########################################################
if st.session_state.to_be_submitted:
    # Sanity check: dataset_uri is not empty?
    if st.session_state["dataset_uri"] in (None, ""):
        st.error("Please provide a valid dataset URI.")
        st.stop()

    #####################################################
    # IMPORT & PROCESS DATA
    #####################################################
    status_main = st.status("Importing data from Google Sheets...", expanded=False)
    with status_main:
        data, dataset_meta, variables_meta_dict, origin, unknown_countries, dataset_uri = processing_part_1(
            import_method=import_method,
            dataset_uri=st.session_state["dataset_uri"],
            infer_metadata=st.session_state["infer_metadata"],
            is_private=st.session_state["fasttrack_is_private"],
            _status=status_main,
        )

    # If all countries are known, proceed without alterations
    if unknown_countries in ([], None):
        # continue with submission
        st.session_state["fast_import"] = processing_part_2(
            data=data,
            dataset_meta=dataset_meta,
            variables_meta_dict=variables_meta_dict,
            origin=origin,
            dataset_uri=dataset_uri,
            status=status_main,
            import_method=import_method,
        )

    # If there are unknown countries, do something about it
    else:
        # 1/ Ask what to do
        with st.form("unknown_countries_form"):
            st.radio(
                label="⚠️ There are unknown countries. What do you want to do?",
                options=["Keep unknown entities", "Drop unknown entities"],
                key="unknown_countries_action",
            )
            with st.expander(f"See list of {len(unknown_countries)} unknown entities", expanded=False):
                df_countries = pd.DataFrame(unknown_countries, columns=["entity"]).sort_values(by="entity")
                st.dataframe(df_countries, hide_index=True)
            proceed_1 = st.form_submit_button(
                "Proceed",
                type="primary",
                use_container_width=True,
                on_click=lambda: set_states(
                    {
                        "to_be_submitted_confirmed_1": True,
                        "to_be_submitted_confirmed_2": False,
                    }
                ),
            )

        if st.session_state.to_be_submitted_confirmed_1:
            # 2/ Do something
            status_second = st.status("Processing unknown countries...", expanded=False)
            with status_second:
                if (unknown_countries not in ([], None)) & (
                    st.session_state["unknown_countries_action"] == "Drop unknown entities"
                ):
                    st.write("Dropping unknown entities...")
                    data = data.loc[~data.index.get_level_values("country").isin(unknown_countries)]

            # 3/ Proceed
            st.session_state["fast_import"] = processing_part_2(
                data=data,
                dataset_meta=dataset_meta,
                variables_meta_dict=variables_meta_dict,
                origin=origin,
                dataset_uri=dataset_uri,
                status=status_second,
                import_method=import_method,
            )


##########################################################
# DATA HAS BEEN PROCESSED
##########################################################
if st.session_state.to_be_submitted_confirmed_2:
    if st.session_state.fast_import:
        with st.status("Uploading to Grapher...", expanded=True):
            fast_import = st.session_state.fast_import
            # add dataset to dag
            st.write("Adding dataset to the DAG...")
            dag_content = fast_import.add_to_dag()

            # create step and metadata file
            st.write("Creating step and metadata files...")
            generate_step_to_channel(CURRENT_DIR / "cookiecutter/", fast_import.meta.to_dict())
            fast_import.save_metadata()

            # Uploading snapshot
            st.write("Uploading snapshot...")
            snapshot_path = fast_import.upload_snapshot()
            st.success("Upload successful!")

            # Running ETL and upserting to GrapherDB...
            st.write("Running ETL and upserting to GrapherDB...")
            step = f"{fast_import.dataset.metadata.uri}"
            etl_main(
                dag_path=DAG_FASTTRACK_PATH,
                steps=[step],
                grapher=True,
                private=not fast_import.dataset.metadata.is_public,
                workers=1,
                # NOTE: force is necessary because we are caching checksums with files.CACHE_CHECKSUM_FILE
                # we could have cleared the cache, but this is cleaner
                force=True,
            )
            st.success("Import to MySQL successful!")

            # Others
            if config.FASTTRACK_COMMIT:
                # Commiting and pushing to Github...
                st.write("Commiting and pushing to Github...")
                github_link = fast_import.commit_and_push()
                st.success("Changes commited and pushed successfully!")
            else:
                github_link = ""

        # Show final success messages
        if config.DB_HOST == "localhost":
            url = f"http://localhost:3030/admin/datasets/{fast_import.dataset_id}"
        else:
            url = f"http://{config.DB_HOST}/admin/datasets/{fast_import.dataset_id}"
        st.success(f"The dataset was imported to the [database]({url})!")
        if config.FASTTRACK_COMMIT:
            st.success(f"See commit in [ETL repository]({github_link})")

        # Show generated files
        wizard_utils.preview_dag_additions(
            dag_content=dag_content,
            dag_path=DAG_FASTTRACK_PATH,
        )
        wizard_utils.preview_file(
            snapshot_path,
            language="yaml",
        )
        wizard_utils.preview_file(
            fast_import.step_path,
            language="python",
        )
        wizard_utils.preview_file(
            fast_import.metadata_path,
            "yaml",
        )

    else:
        st.error("ERROR 100: No fast_import object found. Please report.")
