"""Entry page.

This is the page that is loaded when the app is started. It redirects to the home page, unless an argument is passed. E.g. `etlwiz charts` will redirect to the charts page.

NOTE: This only works with >1.35 (nightly) version of Streamlit.
"""

import streamlit as st

from apps.wizard import utils
from apps.wizard.config import WIZARD_CONFIG
from etl.paths import DOCS_DIR

# Profiler: Start profiler if debug mode is enabled
PROFILER = None
args = utils.parse_args_from_cmd()
if args.debug:
    PROFILER = utils.start_profiler()

###########################################
# DEFINE PAGES
###########################################
pages = {}


# Initial apps (etl steps)
pages_ = []
for step in WIZARD_CONFIG["main"].values():
    pages_.append(
        st.Page(
            page=str(step["entrypoint"]),
            title=step["title"],
            icon=step["icon"],
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
                page=str(step["entrypoint"]),
                title=step["title"],
                icon=step["icon"],
                url_path=step["alias"],
            )
        )
pages[WIZARD_CONFIG["etl"]["title"]] = pages_

# Sections
for section in WIZARD_CONFIG["sections"]:
    apps = [app for app in section["apps"] if app["enable"]]
    if apps:
        pages_ = []
        for app in apps:
            pages_.append(
                st.Page(
                    page=str(app["entrypoint"]),
                    title=app["title"],
                    icon=app["icon"],
                    url_path=app["alias"],
                )
            )
        pages[section["title"]] = pages_

# Legacy
if ("legacy" in WIZARD_CONFIG) and ("apps" in WIZARD_CONFIG["legacy"]):
    pages_ = []
    for app in WIZARD_CONFIG["legacy"]["apps"]:
        if app["enable"]:
            pages_.append(
                st.Page(
                    page=str(app["entrypoint"]),
                    title=app["title"],
                    icon=app["icon"],
                    url_path=app["alias"],
                )
            )
    pages["Legacy"] = pages_

###########################################
# RUN PAGES
###########################################
# Create navigation
page = st.navigation(pages)

# Run navigation
if page is not None:
    page.run()
else:
    st.error("Pages could not be loaded!")


# LOGO
st.logo(
    str(DOCS_DIR / "assets/wizard-logo3.png"),
    size="large",
    # link="https://google.com",  # TODO: would be cool if we could link to an internal page (and not only external). Check streamlit issues, and consider creating one.
)

# Stop profiler if applicable
if args.debug and (PROFILER is not None):
    PROFILER.stop()
