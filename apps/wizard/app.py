"""Entry page.

This is the page that is loaded when the app is started. It redirects to the home page, unless an argument is passed. E.g. `etlwiz charts` will redirect to the charts page."""
from pathlib import Path

import streamlit as st
from st_pages import Page, Section, add_indentation, show_pages

from apps.wizard.config import WIZARD_CONFIG
from apps.wizard.home import st_show_home

# Logo
# st.logo("docs/assets/logo.png")

print("------------app")
print(st.query_params)


# Get current directory
CURRENT_DIR = Path(__file__).parent
# Page config
st.set_page_config(page_title="Wizard: Home", page_icon="🪄")
st.title("Wizard 🪄")
st.markdown(
    """
Wizard is a fundamental tool for data scientists at OWID to easily create ETL steps. Additionally, wizard provides a set of tools to explore and improve these steps.
"""
)

###########################################
# DEFINE TOC
###########################################
# Initial apps (etl steps)
toc = []
for step in WIZARD_CONFIG["main"].values():
    toc.append(
        Page(
            path=str(CURRENT_DIR / step["entrypoint"]),
            name=step["title"],
            icon=step["emoji"],
        )
    )

# ETL steps
toc.append(Section(WIZARD_CONFIG["etl"]["title"]))
for step in WIZARD_CONFIG["etl"]["steps"].values():
    if step["enable"]:
        toc.append(
            Page(
                path=str(CURRENT_DIR / step["entrypoint"]),
                name=step["title"],
                icon=step["emoji"],
            )
        )

# Other apps specified in the config
for section in WIZARD_CONFIG["sections"]:
    apps = [app for app in section["apps"] if app["enable"]]
    if apps:
        toc.append(Section(section["title"]))
        for app in apps:
            toc.append(
                Page(
                    path=str(CURRENT_DIR / app["entrypoint"]),
                    name=app["title"],
                    icon=app["emoji"],
                )
            )

# Show table of content (apps)
show_pages(toc)
add_indentation()

###########################################
# Home app
###########################################
st_show_home()


# # EXPERIMENTAL
# # Get query parameters from the URL
# # query_params = st.query_params
###########################################
# PAGE REDIRECT VIA QUERY PARAMS
###########################################
if "page" in st.query_params:
    for step_name, step_props in WIZARD_CONFIG["etl"]["steps"].items():
        if st.query_params["page"] == step_name:
            st.switch_page(step_props["entrypoint"])
    for section in WIZARD_CONFIG["sections"]:
        for app in section["apps"]:
            if st.query_params["page"] == app["alias"]:
                st.switch_page(app["entrypoint"])

# # Go to specific page if argument is passed
# ## Home
# if utils.AppState.args.phase == "all":  # type: ignore
#     switch_page("Home")  # type: ignore
# ## ETL step
# for step_name, step_props in WIZARD_CONFIG["etl"]["steps"].items():
#     if utils.AppState.args.phase == step_name:  # type: ignore
#         switch_page(step_props["title"])  # type: ignore
# ## Section
# for section in WIZARD_CONFIG["sections"]:
#     for app in section["apps"]:
#         if utils.AppState.args.phase == app["alias"]:  # type: ignore
#             switch_page(app["title"])  # type: ignore
