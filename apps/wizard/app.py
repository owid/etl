import argparse
from pathlib import Path

import streamlit as st
from st_pages import Page, show_pages

from apps.wizard import utils

# Get current directory
CURRENT_DIR = Path(__file__).parent
# Page config
st.set_page_config(page_title="Wizard", page_icon="🪄")
st.title("Wizard")

print(utils.AppState.args)
# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page(CURRENT_DIR / "snapshot.py", "Snapshot", in_section=False),
        Page(CURRENT_DIR / "meadow.py", "Meadow", in_section=False),
        Page(CURRENT_DIR / "garden.py", "Garden", in_section=False),
        Page(CURRENT_DIR / "grapher.py", "Grapher", in_section=False),
    ]
)
