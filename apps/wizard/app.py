"""Entry page.

This is the page that is loaded when the app is started. It builds the multi-page navigation from
`apps/wizard/config/config.yml` and runs the selected page (Home by default).
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

# Enable Sentry if SENTRY_DSN is in .env
utils.enable_sentry_for_streamlit()


###########################################
# DEFINE PAGES
###########################################
def _page(item: dict, url_path: str | None = None, default: bool = False) -> st.Page:
    """Navigation entry for a config item: an external URL becomes a plain link, a script a page."""
    entrypoint = str(item["entrypoint"])
    if entrypoint.startswith(("http://", "https://")):
        return st.Page(page=entrypoint, title=item["title"], icon=item["icon"])
    return st.Page(page=entrypoint, title=item["title"], icon=item["icon"], url_path=url_path, default=default)


pages = {}

# Overview: home page and top-level links
pages["Overview"] = [
    _page(item, url_path=item["title"].lower(), default=item["title"] == "Home")
    for item in WIZARD_CONFIG["main"].values()
]

# ETL steps
pages[WIZARD_CONFIG["etl"]["title"]] = [
    _page(step, url_path=step["alias"]) for step in WIZARD_CONFIG["etl"]["steps"].values() if step["enable"]
]

# Sections
for section in WIZARD_CONFIG["sections"]:
    apps = [app for app in section["apps"] if app["enable"]]
    if apps:
        pages[section["title"]] = [_page(app, url_path=app["alias"]) for app in apps]

# Legacy
if ("legacy" in WIZARD_CONFIG) and ("apps" in WIZARD_CONFIG["legacy"]):
    pages["Legacy"] = [_page(app, url_path=app["alias"]) for app in WIZARD_CONFIG["legacy"]["apps"] if app["enable"]]

###########################################
# RUN PAGES
###########################################
# Create navigation
page = st.navigation(
    pages,
    expanded=False,
    position="sidebar",
)

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
