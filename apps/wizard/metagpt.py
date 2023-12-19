import os
import tempfile
from pathlib import Path
from typing import List, cast

import streamlit as st
import yaml
from st_pages import add_indentation
from streamlit_ace import st_ace

from apps.metagpt.cli import MetadataGPTUpdater


##################################################
# CONFIGURATION
##################################################
# Generic functions
def export_metadata_file(metadata_new, overwrite, output_dir, filepath) -> None:
    """Export file."""
    # Get output path
    if overwrite:
        output_file_path = filepath
    else:
        if output_dir in [None, ""]:
            output_dir = os.path.dirname(filepath)
        output_file_path = os.path.join(output_dir, "gpt_" + os.path.basename(filepath))
    # Save updated metadata
    print(f"Exporting shit to {output_file_path}")
    with open(output_file_path, "w") as file:
        file.write(metadata_new)


def set_run_gpt_to_false():
    st.session_state["run_gpt"] = False
    st.session_state["show_gpt"] = False


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
PATH_SNAPSHOT = "/home/lucas/repos/etl/snapshots/"
PATH_GRAPHER = "/home/lucas/repos/etl/etl/steps/data/grapher/"
PREFIX_SNAPSHOT = "SNAPSHOT :: "
PREFIX_GRAPHER = "GRAPHER :: "


# Get available files
def get_paths(directory: str, extension: str) -> List[str]:
    """Get available files."""
    return list(sorted(str(path) for path in Path(directory).rglob(pattern=f"*.{extension}")))


paths_snapshot = get_paths(PATH_SNAPSHOT, "dvc")
paths_grapher = get_paths(PATH_GRAPHER, "yml")
paths_snapshot = [path.replace(PATH_SNAPSHOT, PREFIX_SNAPSHOT) for path in paths_snapshot]
paths_grapher = [path.replace(PATH_GRAPHER, PREFIX_GRAPHER) for path in paths_grapher]
paths = paths_snapshot + paths_grapher


# Function to convert displayed path to actual path
def get_actual_path(path: str) -> str:  # -> Any:
    """Display to actual path."""
    return path.replace(PREFIX_SNAPSHOT, PATH_SNAPSHOT).replace(PREFIX_GRAPHER, PATH_GRAPHER)


##################################################
# BUILD PAGE
##################################################
# The page is divided into two columns
col11, col12 = st.columns(2)

with col11:
    # Ask user to select metadata file
    metadata_file = st.selectbox("Select metadata file", paths, on_change=set_run_gpt_to_false)
    # Run GPT
    st.session_state["run_gpt"] = st.button("Run", type="primary")


col21, col22 = st.columns(2)
with col21:
    # Load file
    filepath = get_actual_path(cast(str, metadata_file))
    with open(filepath, "r") as f:
        file_content = f.read()
    # Show file to the user
    st_ace(file_content, **ACE_DEFAULT)

if st.session_state["run_gpt"]:
    # Update metadata using GPT
    try:
        with st.spinner("Running GPT..."):
            # Start tool (initialise gpt client)
            gpt_updater = MetadataGPTUpdater(filepath)
            # Run update
            gpt_updater.run()
            # Get metadata
            st.session_state["metadata_new"] = gpt_updater.metadata_new_str
    except Exception as e:
        st.error(f"Metadata update process failed. Error: {str(e)}")
    else:
        st.session_state["show_gpt"] = True


if st.session_state.get("show_gpt"):
    with col22:
        # Temporary export
        tf = tempfile.NamedTemporaryFile()
        with open(file=tf.name, mode="w") as f:
            yaml.dump(st.session_state["metadata_new"], f, default_flow_style=False, sort_keys=False, indent=4)
        # Show file to the user
        with open(tf.name, "r") as f:
            file_content = f.read()
        metadata_new_updated = st_ace(file_content, **ACE_DEFAULT, key="modified")  # gpt_updater.metadata_new_str
    with col12:
        # Form to export
        with st.form("form_export"):
            output_dir = st.text_input("Output path")
            overwrite = st.toggle("Overwrite", value=False)
            st.form_submit_button(
                "Export new file",
                on_click=export_metadata_file,
                args=(metadata_new_updated, overwrite, output_dir, filepath),
            )
