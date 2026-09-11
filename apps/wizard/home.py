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
# Width (px) of the link in each row: wide enough for the longest app name, so the descriptions line up.
# On narrow screens the description wraps onto the next line instead of being clipped.
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
    # DIRECTORY: two page columns of groups (create, sections, links, legacy)
    #########################
    left, right = _split_balanced(_groups())
    for col, column_groups in zip(st.columns(2, gap="medium"), (left, right)):
        with col:
            for group in column_groups:
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


def _split_balanced(groups: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split groups into two columns of roughly equal height (one row per app plus a header per group)."""
    weights = [len(g["apps"]) + 1.5 for g in groups]
    total, acc, cut = sum(weights), 0.0, len(groups)
    for i, w in enumerate(weights):
        acc += w
        if acc >= total / 2:
            cut = i + 1
            break
    return groups[:cut], groups[cut:]


def _render_group(group: dict) -> None:
    """Section title with its description on the same line, then one row per app."""
    with st.container(gap="xsmall"):
        with st.container(horizontal=True, vertical_alignment="bottom", gap="small"):
            st.markdown(f"##### {group['title']}", width="content")
            st.caption(group["description"], width="content")
        for app in group["apps"]:
            _render_app(app)


def _maintainer_help(item: dict) -> str | None:
    maintainer = item.get("maintainer")
    if not maintainer:
        return None
    if isinstance(maintainer, list):
        maintainer = ", ".join(maintainer)
    return f"Maintainer: {maintainer}"


def _render_app(item: dict) -> None:
    """One row: the link (or a disabled button when the app is unavailable here) and the description."""
    title, icon = item["title"], item["icon"]
    # The row wraps: on wide screens the description sits next to the link, on narrow ones it drops below it.
    with st.container(horizontal=True, vertical_alignment="center", gap="small"):
        if item.get("enable", True):
            st.page_link(
                item["entrypoint"], label=f"**{title}**", icon=icon, help=_maintainer_help(item), width=LINK_WIDTH
            )
        else:
            # A disabled tertiary button looks like a greyed-out page link (same padding, icon and font).
            st.button(
                f"**{title}**",
                icon=icon,
                type="tertiary",
                disabled=True,
                help=f"Not available on `{ENV}`.",
                key=f"home-unavailable-{item['entrypoint']}",
                width=LINK_WIDTH,
            )
        if item.get("description"):
            st.caption(item["description"])


# Show the home page
st_show_home()
