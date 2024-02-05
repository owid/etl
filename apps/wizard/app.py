"""Entry page."""
from pathlib import Path

import streamlit as st
from st_pages import Page, Section, add_indentation, show_pages
from streamlit_extras.switch_page_button import switch_page

from apps.wizard import utils

# Get current directory
CURRENT_DIR = Path(__file__).parent
# Page config
st.set_page_config(page_title="Wizard", page_icon="🪄")
st.title("Wizard")


# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page(str(CURRENT_DIR / "home.py"), "Home", icon="🏠"),
        Section("Create new ETL steps"),
        Page(str(CURRENT_DIR / "templating/snapshot.py"), "Snapshot", icon="1️⃣"),
        Page(str(CURRENT_DIR / "templating/meadow.py"), "Meadow", icon="2️⃣"),
        Page(str(CURRENT_DIR / "templating/garden.py"), "Garden", icon="3️⃣"),
        Page(str(CURRENT_DIR / "templating/grapher.py"), "Grapher", icon="4️⃣"),
        Section("Other tools"),
        Page(str(CURRENT_DIR / "charts/__main__.py"), "Charts", icon="📊"),
        Page(str(CURRENT_DIR / "metagpt.py"), "MetaGPT", icon="🤖"),
        Page(str(CURRENT_DIR / "dataset_explorer.py"), "Dataset Explorer", icon="🕵️"),
        Page(str(CURRENT_DIR / "../staging_sync/app.py"), "Staging sync", icon="🔄"),
    ]
)

add_indentation()

if utils.AppState.args.phase == "all":  # type: ignore
    switch_page("Home")  # type: ignore
elif utils.AppState.args.phase == "metagpt":  # type: ignore
    switch_page("MetaGPT")  # type: ignore
elif utils.AppState.args.phase == "dataexp":  # type: ignore
    switch_page("Dataset Explorer")  # type: ignore
elif utils.AppState.args.phase in ["snapshot", "meadow", "garden", "grapher", "charts"]:  # type: ignore
    switch_page(utils.AppState.args.phase.title())  # type: ignore
