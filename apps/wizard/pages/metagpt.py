"""MetaGPT page.

Note that it still relies on apps/metagpt.
"""
import os
import tempfile
from pathlib import Path
from typing import List, cast

import streamlit as st
from st_pages import add_indentation
from streamlit_ace import st_ace

from apps.metagpt.cli import MetadataGPTUpdater
from etl.files import yaml_dump
from etl.paths import SNAPSHOTS_DIR, STEP_DIR


##################################################
# CONFIGURATION
##################################################
# Generic functions
def export_metadata_file() -> None:
    """Export file.

    metadata_new: new metadata
    overwrite: whether to overwrite the original file
    output_path: custom path to export the new metadata file
    filepath_original: original file path

    If overwrite is True, it replaces the original file (which should be `filepath`)
    Otherwise, it saves it under the
    """
    # Get output path of new metadata file

    ## Overwrite existing yaml file
    if st.session_state["overwrite"]:
        # output_file_path = filepath_original
        st.session_state["output_path"] = st.session_state["filepath_metadata"]
    ## Save new yaml file
    else:
        ## If no custom output path is provided, save it under the same directory as the original file, with prefix "gpt_"
        if st.session_state["output_path"] in [None, ""]:
            st.session_state["output_path"] = os.path.join(
                os.path.dirname(st.session_state["filepath_metadata"]),
                "gpt_" + os.path.basename(st.session_state["filepath_metadata"]),
            )
    # Save updated metadata
    print(f"Exporting new metadata to {st.session_state['output_path']}")
    with open(st.session_state["output_path"], mode="w") as file:
        # s = yaml_dump(st.session_state["metadata_new_updated"], width=float("inf"))
        file.write(st.session_state["metadata_new_updated"])


# Session state
st.session_state.run_gpt = st.session_state.get("run_gpt", False)
st.session_state.run_gpt_confirmed = st.session_state.get("run_gpt_confirmed", False)
st.session_state.gpt_updater = st.session_state.get("gpt_updater", None)
st.session_state.cost = st.session_state.get("cost", -1)


def run_gpt():
    st.session_state.run_gpt = True


def run_gpt_confirmed():
    st.session_state.run_gpt_confirmed = True
    st.session_state["run_gpt"] = False


def set_run_gpt_to_false():
    st.session_state["run_gpt"] = False
    st.session_state["run_gpt_confirmed"] = False
    st.session_state["show_gpt"] = False
    st.session_state["gpt_updater"] = None


# Page config
st.set_page_config(
    page_title="ETL Meta GPT",
    layout="wide",
    page_icon="🤖",
    initial_sidebar_state="collapsed",
)
st.title("🤖 ETL Meta GPT")
add_indentation()

# ACE config
# Show metadata in text editors
ACE_DEFAULT = {
    "placeholder": "Fill this with metadata content",
    "language": "yaml",
    "theme": "twilight",
    "min_lines": 0,
    "font_size": 14,
    "wrap": True,
    "auto_update": True,
}

# Paths config
## Paths to snapshot/grapher metadata directories
PATH_SNAPSHOT = str(SNAPSHOTS_DIR)
PATH_GARDEN = f"{STEP_DIR}/data/garden/"
PATH_GRAPHER = f"{STEP_DIR}/data/grapher/"
PREFIX_SNAPSHOT = "SNAPSHOT :: "
PREFIX_GARDEN = "GARDEN :: "
PREFIX_GRAPHER = "GRAPHER :: "


# Get available files
def get_paths(directory: str, extension: str) -> List[str]:
    """Get available files."""
    return list(sorted(str(path) for path in Path(directory).rglob(pattern=f"*.{extension}")))


paths_snapshot = get_paths(PATH_SNAPSHOT, "dvc")
paths_garden = get_paths(PATH_GARDEN, "yml")
paths_grapher = get_paths(PATH_GRAPHER, "yml")
paths_snapshot = [path.replace(PATH_SNAPSHOT, PREFIX_SNAPSHOT) for path in paths_snapshot]
paths_garden = [path.replace(PATH_GARDEN, PREFIX_GARDEN) for path in paths_garden]
paths_grapher = [path.replace(PATH_GRAPHER, PREFIX_GRAPHER) for path in paths_grapher]
paths = paths_snapshot + paths_garden + paths_grapher


# Function to convert displayed path to actual path
def get_actual_path(path: str) -> str:  # -> Any:
    """Display to actual path."""
    return (
        path.replace(PREFIX_SNAPSHOT, PATH_SNAPSHOT)
        .replace(PREFIX_GRAPHER, PATH_GRAPHER)
        .replace(PREFIX_GARDEN, PATH_GARDEN)
    )


##################################################
# BUILD PAGE
##################################################
# The page is divided into two columns
col11, col12 = st.columns(2)

# First row, first column: user selects the dataset
with col11:
    # Ask user to select metadata file
    metadata_file = st.selectbox("Select metadata file", paths, on_change=set_run_gpt_to_false)
    # Run GPT
    st.button("Run", type="primary", on_click=run_gpt)


# Second row
col21, col22 = st.columns(2)
## Second row, first column: show metadata file
with col21:
    # Load file
    st.session_state["filepath_metadata"] = get_actual_path(cast(str, metadata_file))
    with open(st.session_state["filepath_metadata"], "r") as f:
        file_content = f.read()
    # Show file to the user
    st_ace(file_content, **ACE_DEFAULT)


# If user clicks on 'Run', we estimate the cost of the query and ask user to confirm again.
# Only exception is if it is a snapshot file, where we just don't need user's approval.
if st.session_state["run_gpt"]:
    st.session_state.gpt_updater = MetadataGPTUpdater(st.session_state["filepath_metadata"])
    # Run in lazy mode to estimate the cost
    cost = st.session_state.gpt_updater.run(lazy=True)
    if cost:
        with col11:
            col11a, col11b = st.columns(2)
            st.toast(f"_Estimated_ cost is {cost:.3f} USD.", icon="💰")
            with col11a:
                st.info(f"💰 _Estimated_ cost is {cost:.3f} USD.")
            with col11b:
                st.button("Approve", type="primary", on_click=run_gpt_confirmed)
    else:
        assert (
            st.session_state.gpt_updater.channel == "snapshot"
        ), "Cost for non-snapshot steps should have been calculated!"
    # st.button("Do you want to proceed?", type="primary", on_click=run_gpt_confirmed)


# If approved by user, actually query OpenAI!
if isinstance(st.session_state.gpt_updater, MetadataGPTUpdater) and (
    (st.session_state["run_gpt_confirmed"]) or (st.session_state["gpt_updater"].channel == "snapshot")
):
    # Update metadata using GPT
    try:
        info_text = "Querying OpenAI..."
        with st.spinner(info_text):
            st.toast(info_text, icon="⏳")
            # Actually run
            st.session_state["cost"] = st.session_state.gpt_updater.run(lazy=False)
            # Get metadata
            st.session_state["metadata_new"] = st.session_state.gpt_updater.metadata_new_str
    except Exception as e:
        st.error("Metadata update process failed")
        st.exception(e)
        set_run_gpt_to_false()
    else:
        st.session_state["show_gpt"] = True


if st.session_state.get("show_gpt"):
    with col22:
        # Temporary export
        tf = tempfile.NamedTemporaryFile()
        with open(file=tf.name, mode="w") as f:
            f.write(yaml_dump(st.session_state["metadata_new"], strip_lines=True, width=float("inf")))  # type: ignore
        ## Show file to the user
        with open(tf.name, "r") as f:
            file_content = f.read()
        # file_content = yaml_dump(st.session_state["metadata_new"], strip_lines=True, width=float("inf"))
        st.session_state["metadata_new_updated"] = st_ace(
            file_content, **ACE_DEFAULT, key="modified"
        )  # gpt_updater.metadata_new_str
    with col12:
        with st.expander("Export metadata file", expanded=False):
            # Form to export
            with st.form("form_export"):
                st.session_state["overwrite"] = st.toggle(
                    "Overwrite", value=False, help="Check to overwrite the original metadata file with the new content."
                )
                st.session_state["output_path"] = st.text_input(
                    "Output path",
                    label_visibility="collapsed",
                    placeholder="path/to/file.yml",
                    help="Custom path to save the new metadata file.",
                )
                submit = st.form_submit_button(
                    "Export new file",
                    # on_click=export_metadata_file,
                )

            if submit:
                export_metadata_file()

        with st.expander("Cost details", expanded=False):
            st.success(f"GPT cost was of ${st.session_state.cost:.3f} USD.", icon="💰")

    st.session_state.run_gpt = False
    st.session_state.run_gpt_confirmed = False
