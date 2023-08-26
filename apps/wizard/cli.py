from pathlib import Path

import streamlit as st
from st_pages import Page, add_page_title, show_pages

# Get current directory
CURRENT_DIR = Path(__file__).parent

st.text("random")


# Optional -- adds the title and icon to the current page
add_page_title()

# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page(CURRENT_DIR / "snapshot.py", "Snapshot"),
        Page(CURRENT_DIR / "meadow.py", "Meadow"),
    ]
)
