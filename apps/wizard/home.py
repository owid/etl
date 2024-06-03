"""Home page of wizard."""
from copy import deepcopy
from typing import Any, Dict, List, Optional

import streamlit as st
from streamlit_card import card
from streamlit_extras.switch_page_button import switch_page

from apps.wizard.config import WIZARD_CONFIG
from apps.wizard.utils import st_page_link


def st_show_home():
    st_page_link(
        "expert",
        label="Questions about ETL or Grapher? Ask the expert!",
        help="Ask the expert any documentation question!",
        use_container_width=True,
        border=True,
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
            "font-family": "Helvetica",
        }
    }

    def create_card(
        title: str, image_url: str, text: str | List[str] = "", custom_styles: Optional[Dict[str, Any]] = None
    ) -> None:
        """Create card."""
        styles = deepcopy(default_styles)
        if custom_styles:
            styles["card"].update(custom_styles)
        go_to_page = card(
            title=title,
            image=image_url,
            text=text,
            # text=f"Press {i + 1}",
            styles=styles,
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

    # 1/ First row for [Snapshot, Meadow, Garden, Grapher]
    pages = [
        {
            "title": steps[step]["title"],
            "image_url": steps[step]["image_url"],
        }
        for step in ["snapshot", "meadow", "garden", "grapher"]
        if steps[step]["enable"]
    ]
    if len(pages) > 0:
        columns = st.columns(len(pages))
        for i, page in enumerate(pages):
            with columns[i]:
                create_card(**page)

    # 2 EXPRESS
    if steps["fasttrack"]["enable"]:
        col1, col2 = st.columns([1, 3])
        with col2:
            create_card(
                title=steps["express"]["title"],
                image_url=steps["express"]["image_url"],
                custom_styles={"height": "50px"},
            )

    # 3/ FAST TRACK
    if steps["fasttrack"]["enable"]:
        create_card(
            title=steps["fasttrack"]["title"],
            image_url=steps["fasttrack"]["image_url"],
            custom_styles={"height": "50px"},
        )

    #########################
    # Sections
    #########################
    for section in WIZARD_CONFIG["sections"]:
        apps = [app for app in section["apps"] if app["enable"]]
        if apps:
            st.markdown(f"## {section['title']}")
            st.markdown(section["description"])
            columns = st.columns(len(apps))
            for i, app in enumerate(apps):
                text = [
                    app["description"],
                ]
                # if "maintainer" in app:
                #     text.append(f"maintainer: {app['maintainer']}")
                if app["enable"]:
                    with columns[i]:
                        create_card(
                            title=app["title"],
                            image_url=app["image_url"],
                            text=text,
                        )
