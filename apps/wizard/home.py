"""Home page of wizard.

An index of every app, grouped in the same sections as the sidebar. Unlike the sidebar, it also
shows what each app does, who maintains it, and which apps are unavailable in this environment.
"""

from collections.abc import Callable

import streamlit as st

from apps.wizard.config import WIZARD_CONFIG
from etl.config import ENV, OWID_ENV

st.set_page_config(
    page_title="Wizard: Home",
    page_icon="🪄",
    layout="wide",
)

# Number of section boxes per row.
MAX_COLS_PER_ROW = 3
# Badge color for each environment the wizard can run in.
ENV_COLORS = {"dev": "green", "staging": "orange", "production": "red"}


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

    #########################
    # HEADER
    #########################
    with st.container(horizontal=True, vertical_alignment="bottom", horizontal_alignment="distribute"):
        st.title("Wizard 🪄", width="content")
        with st.container(horizontal=True, vertical_alignment="center", width="content"):
            st.badge(ENV, icon=":material/dns:", color=ENV_COLORS.get(ENV, "gray"))
            st.badge(OWID_ENV.conf.DB_HOST, icon=":material/database:", color="gray", help="Grapher database host.")
            st.caption(f"streamlit {st.__version__}", width="content")

    #########################
    # CREATE (ETL steps)
    #########################
    etl = WIZARD_CONFIG["etl"]
    steps = list(etl["steps"].values())
    with st.container(border=True):
        _render_box_header(etl["title"], etl["description"], etl.get("icon"))
        for col, step in zip(st.columns(len(steps)), steps):
            with col:
                _render_app(step)

    #########################
    # SECTIONS, LINKS, LEGACY
    #########################
    boxes: list[Callable[[], None]] = []
    for section in WIZARD_CONFIG["sections"]:
        boxes.append(lambda section=section: _render_section(section))

    links = [e for e in WIZARD_CONFIG["main"].values() if str(e["entrypoint"]).startswith(("http://", "https://"))]
    if links:
        boxes.append(lambda: _render_links(links))

    legacy_apps = WIZARD_CONFIG.get("legacy", {}).get("apps", [])
    if legacy_apps:
        boxes.append(lambda: _render_legacy(legacy_apps))

    for i in range(0, len(boxes), MAX_COLS_PER_ROW):
        row = boxes[i : i + MAX_COLS_PER_ROW]
        for col, render in zip(st.columns(len(row), border=True), row):
            with col:
                render()


def _render_box_header(title: str, description: str, icon: str | None) -> None:
    st.subheader(title, icon=icon)
    st.caption(description)


def _render_section(section: dict) -> None:
    _render_box_header(section["title"], section["description"], section.get("icon"))
    for app in section["apps"]:
        _render_app(app)


def _render_links(links: list[dict]) -> None:
    _render_box_header("Links", "Other OWID tools and resources.", ":material/link:")
    for link in links:
        _render_app(link)


def _render_legacy(apps: list[dict]) -> None:
    _render_box_header("Legacy", WIZARD_CONFIG["legacy"]["description"], ":material/history:")
    for app in apps:
        _render_app(app)


def _maintainer_help(item: dict) -> str | None:
    maintainer = item.get("maintainer")
    if not maintainer:
        return None
    if isinstance(maintainer, list):
        maintainer = ", ".join(maintainer)
    return f"Maintainer: {maintainer}"


def _render_app(item: dict) -> None:
    """Render one app (or external link) from the wizard config: a full-width link with its description below.

    Apps disabled in this environment are shown greyed out, not hidden, so it is clear they exist and
    where they can be run.
    """
    title, icon, description = item["title"], item["icon"], item.get("description", "")
    with st.container(gap=None):
        if item.get("enable", True):
            st.page_link(
                item["entrypoint"], label=f"**{title}**", icon=icon, help=_maintainer_help(item), width="stretch"
            )
        else:
            st.markdown(f":gray[{icon} **{title}**]", help=f"Not available on `{ENV}`.")
        if description:
            st.caption(description)


# Show the home page
st_show_home()
