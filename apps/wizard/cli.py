from pathlib import Path

import streamlit as st
from st_pages import Page, show_pages

# Get current directory
CURRENT_DIR = Path(__file__).parent
# Page config
st.set_page_config(page_title="Wizard", page_icon="🪄")
st.title("Wizard")


# Specify what pages should be shown in the sidebar, and what their titles and icons
# should be
show_pages(
    [
        Page(CURRENT_DIR / "snapshot.py", "1️⃣ Snapshot", in_section=True),
        Page(CURRENT_DIR / "meadow.py", "2️⃣ Meadow", in_section=True),
        Page(CURRENT_DIR / "garden.py", "3️⃣ Garden", in_section=True),
        Page(CURRENT_DIR / "grapher.py", "4️⃣ Grapher", in_section=True),
    ]
)
