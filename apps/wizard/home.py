"""Home page of wizard.

A directory of every app, grouped in the same sections as the sidebar. Unlike the sidebar, it also
shows what each app does, who maintains it, and which apps are unavailable in this environment.
"""

import streamlit as st

from apps.wizard.config import WIZARD_CONFIG
from etl.config import ENV, OWID_ENV

st.set_page_config(
    page_title="Wizard: Home",
    page_icon="🪄",
    layout="wide",
)

# Badge color for each environment the wizard can run in.
ENV_COLORS = {"dev": "green", "staging": "orange", "production": "red"}
# Width (px) of each section tile. Tiles wrap, so a wide screen shows four per row, a laptop three, a phone one.
TILE_WIDTH = 260
# Width (px) of the link in each row: wide enough for the longest app name, so the help icons line up.
LINK_WIDTH = 200


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
    # DIRECTORY: one bordered tile per group (create, sections, links, legacy), as many per row as fit
    #########################
    with st.container(horizontal=True, gap="small"):
        for group in _groups():
            with st.container(border=True, width=TILE_WIDTH, height="stretch"):
                _render_group(group)


def _groups() -> list[dict]:
    """Every group of apps shown on the home page: the step-creation apps, the config sections, links, legacy."""
    etl = WIZARD_CONFIG["etl"]
    groups = [{"title": etl["title"], "description": etl["description"], "apps": list(etl["steps"].values())}]
    groups += [
        {"title": s["title"], "description": s["description"], "apps": s["apps"]} for s in WIZARD_CONFIG["sections"]
    ]
    links = [e for e in WIZARD_CONFIG["main"].values() if str(e["entrypoint"]).startswith(("http://", "https://"))]
    if links:
        groups.append({"title": "Links", "description": "Other OWID tools and resources.", "apps": links})
    legacy_apps = WIZARD_CONFIG.get("legacy", {}).get("apps", [])
    if legacy_apps:
        groups.append({"title": "Legacy", "description": WIZARD_CONFIG["legacy"]["description"], "apps": legacy_apps})
    return groups


def _render_group(group: dict) -> None:
    """Section title with its description underneath, then one row per app."""
    with st.container(gap="xsmall"):
        with st.container(gap=None):
            st.markdown(f"##### {group['title']}")
            st.caption(group["description"])
        for app in group["apps"]:
            _render_app(app)


def _help_text(item: dict) -> str:
    """Tooltip of an app: its description, whether it is available here, and who maintains it."""
    parts = [item.get("description", "")]
    if not item.get("enable", True):
        parts.append(f"Not available on `{ENV}`.")
    maintainer = item.get("maintainer")
    if maintainer:
        if isinstance(maintainer, list):
            maintainer = ", ".join(maintainer)
        parts.append(f"Maintainer: {maintainer}")
    return "\n\n".join(p for p in parts if p)


def _render_app(item: dict) -> None:
    """One row: the link (or a disabled button when the app is unavailable here) and a help icon with the details."""
    title, icon = item["title"], item["icon"]
    with st.container(horizontal=True, vertical_alignment="center", gap=None):
        if item.get("enable", True):
            st.page_link(item["entrypoint"], label=f"**{title}**", icon=icon, width=LINK_WIDTH)
        else:
            # A disabled tertiary button looks like a greyed-out page link (same padding, icon and font). It keeps
            # its natural width inside a fixed-width box so the label stays left-aligned and the help icons line up.
            with st.container(width=LINK_WIDTH):
                st.button(
                    f"**{title}**",
                    icon=icon,
                    type="tertiary",
                    disabled=True,
                    key=f"home-unavailable-{item['entrypoint']}",
                )
        # Description, availability and maintainer live in the tooltip, so rows stay one line at any width.
        st.markdown("", help=_help_text(item), width="content")


# Show the home page
st_show_home()
