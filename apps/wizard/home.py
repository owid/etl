"""Home page of wizard."""
from typing import List

import streamlit as st
from st_pages import add_indentation
from streamlit_card import card
from streamlit_extras.switch_page_button import switch_page

from apps.wizard.config import WIZARD_CONFIG

# Initial configuration (side bar menu, title, etc.)
add_indentation()
st.title("Wizard 🪄")
st.markdown(
    """
Wizard is a fundamental tool for data scientists at OWID to easily create ETL steps. Additionally, wizard provides a set of tools to explore and improve these steps.
"""
)

# Generic tools
## Default styling for the cards (Wizard apps are presented as cards)
default_styles = {
    "card": {
        "width": "150",
        "height": "100px",
        "padding": "0",
        "margin": "0",
        "font-size": ".8rem",
        # "font-family": "Helvetica",
    }
}


def create_card(title: str, image_url: str, text: str | List[str] = "") -> None:
    """Create card."""
    go_to_page = card(
        title=title,
        image=image_url,
        text=text,
        # text=f"Press {i + 1}",
        styles=default_styles,
        on_click=lambda: None,
    )
    if go_to_page:
        switch_page(title)


#########################
# ETL Steps
#########################
st.markdown(f"## {WIZARD_CONFIG['etl']['title']}")
st.markdown(WIZARD_CONFIG["etl"]["description"])
steps = WIZARD_CONFIG["etl"]["steps"]

# We present two channels to create an ETL step chain:
# 1. Classic: Snapshot -> Meadow -> Garden + Grapher
# 2. Fast Track: Fast Track + Grapher

# Create two columns, with ration 3:1 (right is reserved for Grapher card)
RATIO_TO_1 = 3
col1, col2 = st.columns([RATIO_TO_1, 1])

# First column for [Snapshot, Meadow, Garden] or [Fast Track]
with col1:
    # 1. CLASSIC
    pages = [
        {
            "title": steps[step]["title"],
            "image_url": steps[step]["image_url"],
        }
        for step in ["snapshot", "meadow", "garden"]
    ]
    columns = st.columns(len(pages))
    assert len(pages) == RATIO_TO_1, f"Number of pages should be valid for the ratio {RATIO_TO_1}:1"
    for i, page in enumerate(pages):
        with columns[i]:
            create_card(**page)
    # 2. FAST TRACK
    create_card(
        title=steps["fasttrack"]["title"],
        image_url=steps["fasttrack"]["image_url"],
    )

# Second column for [Grapher]
with col2:
    create_card(
        title=steps["grapher"]["title"],
        image_url=steps["grapher"]["image_url"],
    )

#########################
# OTHER TOOLS
#########################
for section in WIZARD_CONFIG["sections"]:
    st.markdown(f"## {section['title']}")
    st.markdown(section["description"])
    apps = section["apps"]
    columns = st.columns(len(apps))
    for i, app in enumerate(section["apps"]):
        text = [
            app["description"],
        ]
        # if "maintainer" in app:
        #     text.append(f"maintainer: {app['maintainer']}")
        with columns[i]:
            create_card(
                title=app["title"],
                image_url=app["image_url"],
                text=text,
            )
