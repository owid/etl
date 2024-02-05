"""Fast-track import."""
from pathlib import Path

import streamlit as st
from st_pages import add_indentation

# Page config
st.set_page_config(page_title="Wizard (meadow)", page_icon="🪄")
add_indentation()


# CONFIG
CURRENT_DIR = Path(__file__).parent

#########################################################
# MAIN ##################################################
#########################################################
# TITLE & description
st.title("Wizard  **:gray[Fast-Track]**")
st.markdown(
    """
            Fast-track is a tool for importing datasets from Google Sheets. The idea is to keep all data and metadata there, and use this interface to import or update the data in grapher database where it can be used to create charts. Fast-track also commits your work to [ETL repository](https://github.com/owid/etl) where you can further process your data with Python.
"""
)
# Warning (if needed)
# st.warning("This tool is still in beta. Please report any issues to @Mojmir.")

# IMPORT method: there are three different ways to import a dataset via Fast-Track:
# 1. Import from a Google sheet
# 2. Update from an existing Google sheet
# 3. Import from a local CSV
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
st.divider()

# Actually create form
with st.form("fasttrack-form"):
    # Import field
    if import_method == IMPORT_GSHEET:
        st.text_input(
            label="New Google Sheets URL",
            help="In the Google spreadsheet, click on `File -> Share -> Publish to Web` and share the entire document as csv.",
        )
    elif import_method == UPDATE_GSHEET:
        st.selectbox(
            label="Existing Google Sheets",
            options=["Sheet 1", "Sheet 2", "Sheet 3"],
            help="Selected sheet will be used if you don't specify Google Sheets URL.",
        )
    else:
        st.file_uploader(
            label="Upload Local CSV",
            type=["csv"],
            help="Upload a local CSV file to import data.",
        )
    # Other parameters
    st.checkbox(
        label="Infer missing metadata (instead of raising an error)",
        value=True,
    )
    st.checkbox(
        label="Make dataset private (your metadata will be still public!)",
        value=False,
    )

    submitted = st.form_submit_button(
        "Submit",
        type="primary",
        use_container_width=True,
        # on_click=update_state,
    )
