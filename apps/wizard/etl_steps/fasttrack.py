"""Fast-track import."""
import datetime as dt
import urllib.error
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import streamlit as st
from owid.catalog import Source
from st_pages import add_indentation
from structlog import get_logger

import apps.fasttrack.csv as csv
import apps.fasttrack.sheets as sheets
from apps.wizard import utils as wizard_utils
from apps.wizard.etl_steps.fasttrack_utils import (
    _harmonize_countries,
    _infer_metadata,
    _last_updated_before_minutes,
    _load_existing_sheets_from_snapshots,
    _validate_data,
)

# Page config
st.set_page_config(page_title="Wizard (meadow)", page_icon="🪄")
add_indentation()


# CONFIG
CURRENT_DIR = Path(__file__).parent
# Config style
wizard_utils.config_style_html()
# Logger
log = get_logger()


def set_states(states_values: Dict[str, Any]):
    for key, value in states_values.items():
        print(key, value)
        st.session_state[key] = value


##########################################################
# MAIN ###################################################
##########################################################
# TITLE & description
st.title("Wizard  **:gray[Fast-Track]**")
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
IMPORT_GSHEET = "import_gsheet"
UPDATE_GSHEET = "update_gsheet"
LOCAL_CSV = "local_csv"
IMPORT_OPTIONS = {
    IMPORT_GSHEET: {
        "title": "New Google sheet",
        "description": "Import data from a Google sheet",
        "guidelines": {
            "heading": "❓ **How to import from a Google sheet**",
            "file_path": CURRENT_DIR / "markdown" / "fasttrack_gsheet_import.md",
        },
    },
    UPDATE_GSHEET: {
        "title": "Existing Google sheet",
        "description": "Update from a Google sheet (already imported in the database)",
        "guidelines": {
            "heading": "❓ **How to update from an existing sheet**",
            "file_path": CURRENT_DIR / "markdown" / "fasttrack_gsheet_update.md",
        },
    },
    LOCAL_CSV: {
        "title": "Local CSV",
        "description": "Import from a local CSV",
        "guidelines": {
            "heading": "❓ **How to import a local CSV**",
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
        with st.expander(option_params["guidelines"]["heading"], expanded=False):
            with open(option_params["guidelines"]["file_path"], "r") as f:
                st.markdown(f.read())


##########################################################
# Actually create & show form
##########################################################
with st.form("fasttrack-form"):
    # Import field
    if import_method == IMPORT_GSHEET:
        st.text_input(
            label="New Google Sheets URL",
            help="In the Google spreadsheet, click on `File -> Share -> Publish to Web` and share the entire document as csv.",
            key="dataset_uri",
        )
    elif import_method == UPDATE_GSHEET:
        options = _load_existing_sheets_from_snapshots()
        st.selectbox(
            label="Existing Google Sheets",
            options=options,
            format_func=lambda x: x["label"],
            help="Selected sheet will be used if you don't specify Google Sheets URL.",
            key="dataset_uri",
            on_change=set_states({"is_private": False}),
        )
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
    st.checkbox(
        label="Make dataset private (your metadata will be still public!)",
        value=st.session_state.get("is_private", False),
        key="is_private",
    )

    submitted = st.form_submit_button(
        "Submit",
        type="primary",
        use_container_width=True,
        # on_click=update_state,
    )


##########################################################
# SUBMISSION
##########################################################
if submitted:
    # Sanity check: dataset_uri is not empty?
    if st.session_state["dataset_uri"] in (None, ""):
        st.error("Please provide a valid dataset URI.")
        st.stop()

    #####################################################
    # IMPORT DATA
    #####################################################
    # 1/ LOCAL CSV FILE
    if import_method == LOCAL_CSV:
        # Get filename, show notification
        uploaded_file = st.session_state["dataset_uri"]

        # Read CSV file as a dataframe
        csv_df = pd.read_csv(uploaded_file)

        # Parse dataframe
        data = csv.parse_data_from_csv(csv_df)

        # Obtain dataset and other objects
        dataset_meta, variables_meta_dict, origin = csv.parse_metadata_from_csv(
            uploaded_file.name,
            csv_df.columns,
        )

        # Success message
        st.success("Data imported from CSV")

    # 2/ GOOGLE SHEET (New or existing)
    else:
        # Get filename, show notification
        sheets_url = st.session_state["dataset_uri"]
        if import_method == UPDATE_GSHEET:
            sheets_url = sheets_url["value"]

        # Show status progress as we import data
        with st.status("Importing data from Google Sheets...", expanded=True):
            st.info(
                """
                Note that Google Sheets refreshes its published version every 5 minutes, so you may need to wait a bit after you update your data.
                """
            )

            try:
                # Sanity check
                st.write("Sanity checks...")
                if "?output=csv" not in sheets_url:
                    st.exception(
                        sheets.ValidationError(
                            "URL does not contain `?output=csv`. Have you published it as CSV and not as HTML by accident?"
                        )
                    )
                    st.stop()

                # Import data from Google Sheets
                st.write(f"Importing [sheet]({sheets_url.replace('?output=csv', '')})...")
                google_sheets = sheets.import_google_sheets(sheets_url)
                # TODO: it would make sense to repeat the import until we're sure that it has been updated
                # we wouldn't risk importing data that is not up to date then
                # the question is how much can we trust the timestamp in the published version

                # Parse data into dataframe
                st.write("Parsing data...")
                data = sheets.parse_data_from_sheets(google_sheets["data"])

                # Obtain dataset and other objects
                st.write("Creating dataset...")
                dataset_meta, variables_meta_dict, origin = sheets.parse_metadata_from_sheets(
                    google_sheets["dataset_meta"],
                    google_sheets["variables_meta"],
                    google_sheets["sources_meta"],
                    google_sheets["origins_meta"],
                )

            except urllib.error.HTTPError:
                st.exception(
                    sheets.ValidationError(
                        "Sheet not found, have you copied the template? Creating new Google Sheets document or new "
                        "sheets with the same name in the existing document does not work."
                    )
                )
                st.stop()
            except sheets.ValidationError as e:
                st.exception(e)
                st.stop()
            finally:
                st.success(
                    f"Data imported (sheet refreshed {_last_updated_before_minutes(google_sheets['dataset_meta'])} minutes ago)"
                )

    #####################################################
    # PROCESS DATA
    #####################################################
    with st.status("Further processing..."):
        if st.session_state["infer_metadata"]:
            st.write("Inferring metadata...")
            data, variables_meta_dict = _infer_metadata(data, variables_meta_dict)
            # add unknown source if we have neither sources nor origins
            if not dataset_meta.sources and not origin:
                dataset_meta.sources = [
                    Source(
                        name="Unknown",
                        published_by="Unknown",
                        publication_year=dt.date.today().year,
                        date_accessed=str(dt.date.today()),
                    )
                ]

        # validation
        st.write("Validating data and metadata...")
        success = _validate_data(data, variables_meta_dict)
        if not success:
            st.stop()

        # NOTE: harmonization is not done in ETL, but here in fast-track for technical reasons
        # It's not yet clear what will authors prefer and how should we handle preprocessing from
        # raw data to data saved as snapshot
        st.write("Harmonizing countries...")
        data, unknown_countries = _harmonize_countries(data)
        if unknown_countries:
            st.error(f"Unknown countries: {unknown_countries}")

    # Want to continue even if unknown countries?
    if unknown_countries:
        st.text(
            "There are unknown countries. What do you want to do?",
        )
        keep = st.button("Keep unknown countries")
        drop = st.button("Drop unknown countries")

        if keep:
            st.write("Keeping unknown countries...")
        if drop:
            st.write("Dropping unknown countries...")
            data = data[~data["iso_code"].isin(unknown_countries)]
