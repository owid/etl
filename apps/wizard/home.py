"""Home page of wizard."""
from copy import deepcopy
from typing import Any, Dict, List, Optional

import streamlit as st
from streamlit_card import card

from apps.wizard.config import WIZARD_CONFIG
from apps.wizard.utils import st_page_link

st.set_page_config(
    page_title="Wizard: Home",
    page_icon="🪄",
)
st.text(f"streamlit {st.__version__}")


def st_show_home():
    # Page config
    st.title("Wizard 🪄")
    st.markdown(
        """
    Wizard is a fundamental tool for data scientists at OWID to easily create ETL steps. Additionally, wizard provides a set of tools to explore and improve these steps.
    """
    )

    # Expert link
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
        },
        "filter": {
            "background-color": "rgba(0, 0, 0, 0.5)"  # <- make the image not dimmed anymore
        },
    }

    def create_card(
        entrypoint: str,
        title: str,
        image_url: str,
        text: str | List[str] = "",
        custom_styles: Optional[Dict[str, Any]] = None,
        small: bool = False,
    ) -> None:
        """Create card."""
        styles = deepcopy(default_styles)
        if small:
            styles["card"]["height"] = "50px"

        if custom_styles:
            styles["card"].update(custom_styles)
        go_to_page = card(
            title=title,
            image=image_url,
            text=text,
            # text=f"Press {i + 1}",
            # text=["This is a test card", "This is a subtext"],
            styles=styles,
            on_click=lambda: None,
        )
        if go_to_page:
            st.switch_page(entrypoint)

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
            "entrypoint": steps[step]["entrypoint"],
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
                create_card(**page, small=True)

    # 2 EXPRESS
    if steps["fasttrack"]["enable"]:
        col1, col2 = st.columns([1, 3])
        with col2:
            create_card(
                entrypoint=steps["express"]["entrypoint"],
                title=steps["express"]["title"],
                image_url=steps["express"]["image_url"],
                custom_styles={"height": "50px"},
            )

    # 3/ FAST TRACK
    if steps["fasttrack"]["enable"]:
        create_card(
            entrypoint=steps["fasttrack"]["entrypoint"],
            title=steps["fasttrack"]["title"],
            image_url=steps["fasttrack"]["image_url"],
            custom_styles={"height": "50px"},
        )

    #########################
    # Sections
    #########################
    section_legacy = None
    for section in WIZARD_CONFIG["sections"]:
        apps = [app for app in section["apps"] if app["enable"]]

        # Skip legacy (show later)
        if section["title"] == "Legacy":
            section_legacy = section
            continue

        # Show section
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
                            entrypoint=app["entrypoint"],
                            title=app["title"],
                            image_url=app["image_url"],
                            text=text,
                        )
    # Show legacy
    if section_legacy:
        apps = [app for app in section_legacy["apps"] if app["enable"]]
        if apps:
            st.divider()
            st.markdown(f"## {section_legacy['title']}")
            st.warning(section_legacy["description"])
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
                            entrypoint=app["entrypoint"],
                            title=app["title"],
                            image_url=app["image_url"],
                            text=text,
                        )

    #########################
    # QUERY REDIRECTS
    #########################
    if "page" in st.query_params:
        for step_name, step_props in WIZARD_CONFIG["etl"]["steps"].items():
            if st.query_params["page"] == step_name:
                st.switch_page(step_props["entrypoint"])
        for section in WIZARD_CONFIG["sections"]:
            for app in section["apps"]:
                if st.query_params["page"] == app["alias"]:
                    st.switch_page(app["entrypoint"])


st_show_home()
