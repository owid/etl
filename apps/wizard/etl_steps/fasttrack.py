import streamlit as st
from st_pages import add_indentation

# Page config
st.set_page_config(page_title="Wizard (meadow)", page_icon="🪄")
add_indentation()


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
# st.warning("This tool is still in beta. Please report any issues to @Mojmir.")

# How will the import be?
import_method = st.radio(
    label="How do you want to import the dataset?",
    options=[
        "New Google sheet",
        "Existing Google sheet",
        "Local CSV",
    ],
    captions=[
        "Import data from a Google sheet",
        "Update from a Google sheet (already imported in the database)",
        "Import from a local CSV",
    ],
    help="Select the source of your data.",
    horizontal=True,
)

if import_method == "New Google sheet":
    with st.expander("❓ **How to import from a Google sheet**", expanded=False):
        st.markdown("Placeholder")
    st.divider()
    with st.form("Some form"):
        st.text_input(
            label="New Google Sheets URL",
            help="In the Google spreadsheet, click on `File -> Share -> Publish to Web` and share the entire document as csv.",
        )

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
elif import_method == "Existing Google sheet":
    with st.expander("❓ **How to update from an existing sheet**", expanded=False):
        st.markdown("Placeholder")
    st.divider()
    with st.form("Some form"):
        st.selectbox(
            label="Existing Google Sheets",
            options=["Sheet 1", "Sheet 2", "Sheet 3"],
            help="Selected sheet will be used if you don't specify Google Sheets URL.",
        )

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

elif import_method == "Local CSV":
    with st.expander("❓ **How to import a local CSV**", expanded=False):
        st.markdown("Placeholder")
    st.divider()

    with st.form("Some form"):
        st.file_uploader(
            label="Upload Local CSV",
            type=["csv"],
            help="Upload a local CSV file to import data.",
        )

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
