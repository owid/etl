"""Python code to run the metadata playground app.

streamlit run app.py
"""
import subprocess
import webbrowser
from pathlib import Path

import streamlit as st
from streamlit_ace import st_ace

import etl.grapher_model as gm
from etl import paths
from etl import config

###################################################
# Initial configuration ###########################
###################################################
# Set page config
st.set_page_config(page_title="Metadata v2 preview", layout="wide", page_icon="🎨")
st.title("Metadata v2 preview")
DUMMY = st.selectbox('Choose ETL step to play with', ["dummy_full", "dummy"], index=0)

# Current directory
CURRENT_DIR = Path(__file__).parent.absolute()

# Load metadata from Snapshot
PATH_METADATA_SNAPSHOT = paths.SNAPSHOTS_DIR / "dummy" / "2020-01-01" / f"{DUMMY}.csv.dvc"
with open(PATH_METADATA_SNAPSHOT, 'r') as f:
    METADATA_SNAPSHOT_BASE = f.read()
SNAPSHOT_META_TOKEN_SPLIT = "outs:"
METADATA_SNAPSHOT_DISPLAY = METADATA_SNAPSHOT_BASE.split(SNAPSHOT_META_TOKEN_SPLIT)[0]
METADATA_SNAPSHOT_EXTRA = METADATA_SNAPSHOT_BASE.split(SNAPSHOT_META_TOKEN_SPLIT)[1]
# Load metadata from Garden
PATH_METADATA_GARDEN = paths.STEP_DIR / "data" / "garden" / "dummy" / "2020-01-01" / f"{DUMMY}.meta.yml"
with open(PATH_METADATA_GARDEN, 'r') as f:
    METADATA_GARDEN_BASE = f.read()
METADATA_GARDEN_DISPLAY = METADATA_GARDEN_BASE

# Catalog path
CATALOG_PATH = f"grapher/dummy/2020-01-01/{DUMMY}/{DUMMY}"


# Functions
def run_steps() -> None:
    """Run dummy steps from ETL.

    The environment is assumed to be set.
    """
    # env_path = paths.BASE_DIR / ".env.X"
    # subprocess.run(f"export $(cat {env_path} | xargs)", shell=True)
    subprocess.run(["poetry", "run", "etl", DUMMY, "--grapher"])


def get_data_page_url() -> str:
    """Get data page URL"""
    HOST = config.DB_HOST
    VARIABLE_ID = gm.Variable.load_from_catalog_path(CATALOG_PATH).id
    url = f"http://{HOST}/admin/datapage-preview/{VARIABLE_ID}"
    return url


def reset_metadata_files():
    """Reset metadata files"""
    # Reset YAML files
    with open(PATH_METADATA_SNAPSHOT, 'w') as f:
        f.write(METADATA_SNAPSHOT_BASE)
    with open(PATH_METADATA_GARDEN, 'w') as f:
        f.write(METADATA_GARDEN_BASE)


###################################################
# Show header #####################################
###################################################
URL_METADATA = "https://www.notion.so/owid/Metadata-guidelines-29ca6e19b6f1409ea6826a88dbb18bcc"

# Define columns
col1, col2 = st.columns(2)

# Some info, wrap option
with col1:
    # Link to metadata v2 docs
    st.markdown(f"📚 [Metadata v2 documentation]({URL_METADATA})")
    WRAP = st.checkbox("Wrap YAML enabled", value=False)

    # Explanation
    with st.expander("How does this work?"):
        st.markdown("""
            This tool lets you visualise how your metadata edits are reflected in a data page.

            You are shown the Snapshot and the Garden metadata (YAML files) to the left and right, respectively. Feel free to edit both.


            Once you are happy with your edits, click on the "Render data page" button. This will run the ETL steps for this fictional dataset and open the data page in a new tab.


            The data page shown corresponds the single indicator of the fictional dataset: `dummy_variable`.


            ❗ **Note that this tool works best with a staging server. [Learn how to create yours](https://www.notion.so/owid/Setting-up-a-staging-server-3e5a6591a23846ad83fba1ad6dfed4d4)!**
        """)


###################################################
# Show/Edit metadata YAML files ###################
###################################################
st.divider()

# Define columns
# tab_snapshot, tab_garden = st.tabs(["Snapshot", "Garden"])
col_snapshot, col_garden = st.columns(2)

# Show metadata in text editors
ACE_DEFAULT = {
    "placeholder": "Fill this with metadata content",
    "language": "yaml",
    "theme": "twilight",
    "min_lines": 0,
    "font_size": 14,
    "wrap": WRAP,
    "auto_update": True,
}
with col_snapshot:
    st.markdown("### Snapshot", help="Edit metadata from Snapshot")
    content_snapshot = st_ace(
        value=METADATA_SNAPSHOT_DISPLAY,
        **ACE_DEFAULT
    )

with col_garden:
    st.markdown("### Garden", help="Edit metadata from Garden")
    content_garden = st_ace(
        value=METADATA_GARDEN_DISPLAY,
        **ACE_DEFAULT
    )


###################################################
# Run steps & update data page ####################
###################################################
try:
    with col2:
        clicked = st.button("Render data page", use_container_width=False, type="primary")

        if clicked:
            # Update YAML files
            with open(PATH_METADATA_SNAPSHOT, 'w') as f:
                content_snapshot += f"{SNAPSHOT_META_TOKEN_SPLIT}{METADATA_SNAPSHOT_EXTRA}"
                f.write(content_snapshot)
            with open(PATH_METADATA_GARDEN, 'w') as f:
                f.write(content_garden)
            # Send toast
            st.toast("Running steps...", icon="⚙️")
            with st.spinner('Running ETL steps...'):
                # Run ETL steps
                run_steps()
            # Get URL of data page
            url = get_data_page_url()

            # Show url (toast + message)
            text_data_page_url = f"Done! [See data page]({url})"
            st.toast(text_data_page_url, icon="🎉")
            st.markdown(text_data_page_url)

            # Automatically open data page
            webbrowser.open_new_tab(url)

            reset_metadata_files()
except Exception as e:
    st.toast(f"[There was an error](#error).", icon="❌")
    reset_metadata_files()
    st.markdown("### Error")
    raise e
