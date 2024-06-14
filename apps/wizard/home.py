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
    layout="wide",
)

MAX_COLS_PER_ROW = 3


def st_show_home():
    # Page config
    cols = st.columns([10, 3])
    with cols[0]:
        st.title("Wizard 🪄")
    with cols[1]:
        st.caption(f"streamlit {st.__version__}")
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
            "width": "100%",
            "height": "80px",
            "padding": "0px",
            "margin": "0px",
            "font-size": ".8rem",
            "font-family": "Helvetica",
        },
        "filter": {
            "background-color": "rgba(0, 0, 0, 0.55)"  # <- make the image not dimmed anymore
        },
        "text": {
            "font-size": "1rem",
            # "font-weight": "normal",
            "margin": "0px",
            "padding": "0px",
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
            print(f"----------- {entrypoint}")
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
            # "alias": step,
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
    # Determine number of rows
    sections = WIZARD_CONFIG["sections"]
    num_sections = len(sections)
    num_rows = num_sections // MAX_COLS_PER_ROW + 1

    for row in range(num_rows):
        cols = st.columns(MAX_COLS_PER_ROW)
        for i, section in enumerate(sections[row * MAX_COLS_PER_ROW : (row + 1) * MAX_COLS_PER_ROW]):
            with cols[i]:
                apps = [app for app in section["apps"] if app["enable"]]
                if apps:
                    st.markdown(f"## {section['title']}")
                    st.markdown(section["description"])
                    for app in apps:
                        text = [
                            app["description"],
                        ]
                        create_card(
                            entrypoint=app["entrypoint"],
                            title=app["title"],
                            image_url=app["image_url"],
                            text=text,
                        )
    st.divider()

    #########################
    # Legacy
    #########################
    if "legacy" in WIZARD_CONFIG:
        section_legacy = WIZARD_CONFIG["legacy"]
        apps = [app for app in section_legacy["apps"] if app["enable"]]
        if apps:
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
        if ("legacy" in WIZARD_CONFIG) and ("apps" in WIZARD_CONFIG["legacy"]):
            for app in WIZARD_CONFIG["legacy"]["apps"]:
                if st.query_params["page"] == app["alias"]:
                    st.switch_page(app["entrypoint"])


st_show_home()
