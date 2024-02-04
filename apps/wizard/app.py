"""Entry page."""
from pathlib import Path

import streamlit as st
from st_pages import Page, Section, add_indentation, show_pages
from streamlit_extras.switch_page_button import switch_page

from apps.wizard import utils
from apps.wizard.config import WIZARD_CONFIG

# Get current directory
CURRENT_DIR = Path(__file__).parent
# Page config
st.set_page_config(page_title="Wizard", page_icon="🪄")
st.title("Wizard")


# Initial apps (etl steps)
toc = [
    Page(str(CURRENT_DIR / "home.py"), "Home", icon="🏠"),
]

# ETL steps
toc.append(Section(WIZARD_CONFIG["etl"]["title"]))
for step in WIZARD_CONFIG["etl"]["steps"].values():
    toc.append(
        Page(
            path=str(CURRENT_DIR / step["entrypoint"]),
            name=step["title"],
            icon=step["emoji"],
        )
    )

# Other apps specified in the config
for section in WIZARD_CONFIG["sections"]:
    toc.append(Section(section["title"]))
    for app in section["apps"]:
        toc.append(
            Page(
                path=str(CURRENT_DIR / app["entrypoint"]),
                name=app["title"],
                icon=app["emoji"],
            )
        )

# Show table of content (apps)
show_pages(toc)

# Add indentation
add_indentation()

# Go to specific page if argument is passed
if utils.AppState.args.phase == "all":  # type: ignore
    switch_page("Home")  # type: ignore
elif utils.AppState.args.phase == "metagpt":  # type: ignore
    switch_page("MetaGPT")  # type: ignore
elif utils.AppState.args.phase == "dataexp":  # type: ignore
    switch_page("Dataset Explorer")  # type: ignore
elif utils.AppState.args.phase in ["snapshot", "meadow", "garden", "grapher", "charts"]:  # type: ignore
    switch_page(utils.AppState.args.phase.title())  # type: ignore
