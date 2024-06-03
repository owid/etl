"""Entry page.

This is the page that is loaded when the app is started. It redirects to the home page, unless an argument is passed. E.g. `etlwiz charts` will redirect to the charts page."""
from pathlib import Path

import streamlit as st

from apps.wizard.config import WIZARD_CONFIG

# Logo
# st.logo("docs/assets/logo.png")
st.set_page_config(
    layout="wide",
)
print("------------app")
st.write(st.__version__)


# Get current directory
CURRENT_DIR = Path(__file__).parent

###########################################
# DEFINE PAGES
###########################################
pages = {}


# Initial apps (etl steps)
pages_ = []
for step in WIZARD_CONFIG["main"].values():
    pages_.append(
        st.Page(
            page=str(CURRENT_DIR / step["entrypoint"]),
            title=step["title"],
            icon=step["emoji"],
            url_path=step["title"].lower(),
            default=step["title"] == "Home",
        )
    )
pages["Overview"] = pages_

# ETL steps
pages_ = []
for step in WIZARD_CONFIG["etl"]["steps"].values():
    if step["enable"]:
        pages_.append(
            st.Page(
                page=str(CURRENT_DIR / step["entrypoint"]),
                title=step["title"],
                icon=step["emoji"],
                url_path=step["alias"],
            )
        )
pages[WIZARD_CONFIG["etl"]["title"]] = pages_

# Other apps specified in the config
for section in WIZARD_CONFIG["sections"]:
    apps = [app for app in section["apps"] if app["enable"]]
    if apps:
        pages_ = []
        for app in apps:
            pages_.append(
                st.Page(
                    page=str(CURRENT_DIR / app["entrypoint"]),
                    title=app["title"],
                    icon=app["emoji"],
                    url_path=app["alias"],
                )
            )
        pages[section["title"]] = pages_

# Show table of content (apps)
page = st.navigation(pages)
if page is not None:
    page.run()
else:
    st.error("Pages could not be loaded!")

###########################################
# Home app
###########################################
# st_show_home()


# # EXPERIMENTAL
# # Get query parameters from the URL
# # query_params = st.query_params


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
