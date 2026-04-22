"""Home page of wizard."""

import streamlit as st

from apps.wizard.config import WIZARD_CONFIG
from apps.wizard.utils.components import st_wizard_card, st_wizard_page_link

st.set_page_config(
    page_title="Wizard: Home",
    page_icon="🪄",
    layout="wide",
)

MAX_COLS_PER_ROW = 3


def st_show_home():
    #########################
    # QUERY REDIRECTS
    # Check early to avoid rendering the home page before redirecting.
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

    # Page config
    container = st.container(border=False, horizontal=True, vertical_alignment="bottom")
    with container:
        st.title("Wizard 🪄")
        st_wizard_page_link(
            "expert",
            label=":rainbow[**Ask the Expert**]",
            help="Ask the expert any documentation question!",
            width="content",
            border=False,
        )
        st.caption(f"streamlit {st.__version__}", width="content")

    #########################
    # ETL Steps
    #########################
    st.markdown(f"## {WIZARD_CONFIG['etl']['title']}")
    st.markdown(WIZARD_CONFIG["etl"]["description"])
    steps = WIZARD_CONFIG["etl"]["steps"]

    # 1/ CLASSIC: Snapshot -> Data -> Collection (no captions, match original)
    if steps["fasttrack"]["enable"]:
        _render_cards_row(
            [steps["snapshot"], steps["data"], steps["collection"]],
            height=100,
            col_widths=[1, 2, 1],
            show_caption=False,
        )

    # 2/ FAST TRACK
    if steps["fasttrack"]["enable"]:
        col1, _ = st.columns([3, 1])
        with col1:
            _render_card(steps["fasttrack"], height=50, show_caption=False)

    #########################
    # Sections
    #########################
    sections = WIZARD_CONFIG["sections"]
    num_rows = len(sections) // MAX_COLS_PER_ROW + 1
    for row in range(num_rows):
        cols = st.columns(MAX_COLS_PER_ROW)
        for i, section in enumerate(sections[row * MAX_COLS_PER_ROW : (row + 1) * MAX_COLS_PER_ROW]):
            apps = [app for app in section["apps"] if app["enable"]]
            if not apps:
                continue
            with cols[i]:
                st.markdown(f"## {section['title']}")
                st.markdown(section["description"])
                for app in apps:
                    _render_card(app)

    #########################
    # Legacy
    #########################
    if "legacy" in WIZARD_CONFIG:
        legacy_apps = [app for app in WIZARD_CONFIG["legacy"]["apps"] if app["enable"]]
        if legacy_apps:
            st.warning(WIZARD_CONFIG["legacy"]["description"])
            _render_cards_row(legacy_apps)


def _render_card(item: dict, height: int = 80, show_caption: bool = True) -> None:
    """Render a single wizard card from a WIZARD_CONFIG app/step dict."""
    st_wizard_card(
        entrypoint=item["entrypoint"],
        title=item["title"],
        image_url=item["image_url"],
        caption=item.get("description", "") if show_caption else "",
        height=height,
    )


def _render_cards_row(
    items: list[dict],
    height: int = 80,
    col_widths: list[int] | None = None,
    show_caption: bool = True,
) -> None:
    """Render a row of wizard cards across Streamlit columns."""
    cols = st.columns(col_widths or len(items))
    for col, item in zip(cols, items):
        with col:
            _render_card(item, height=height, show_caption=show_caption)


# Show the home page
st_show_home()
