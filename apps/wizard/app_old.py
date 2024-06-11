# """Entry page.

# This is the page that is loaded when the app is started. It redirects to the home page, unless an argument is passed. E.g. `etlwiz charts` will redirect to the charts page."""
# from pathlib import Path

# import streamlit as st

# # from st_pages import Page, Section, show_pages
# from apps.wizard.config import WIZARD_CONFIG
# from apps.wizard.utils import AppState

# # Logo
# # st.logo("docs/assets/logo.png")

# # Get current directory
# CURRENT_DIR = Path(__file__).parent
# # Page config
# st.set_page_config(page_title="Wizard", page_icon="🪄")
# st.title("Wizard")

# # Initial apps (etl steps)
# toc = []
# for step in WIZARD_CONFIG["main"].values():
#     toc.append(
#         Page(
#             path=str(CURRENT_DIR / step["entrypoint"]),
#             name=step["title"],
#             icon=step["icon"],
#         )
#     )

# # ETL steps
# toc.append(Section(WIZARD_CONFIG["etl"]["title"]))
# for step in WIZARD_CONFIG["etl"]["steps"].values():
#     if step["enable"]:
#         toc.append(
#             Page(
#                 path=str(CURRENT_DIR / step["entrypoint"]),
#                 name=step["title"],
#                 icon=step["icon"],
#             )
#         )

# # Other apps specified in the config
# for section in WIZARD_CONFIG["sections"]:
#     apps = [app for app in section["apps"] if app["enable"]]
#     if apps:
#         toc.append(Section(section["title"]))
#         for app in apps:
#             toc.append(
#                 Page(
#                     path=str(CURRENT_DIR / app["entrypoint"]),
#                     name=app["title"],
#                     icon=app["icon"],
#                 )
#             )

# # Show table of content (apps)
# show_pages(toc)

# # Add indentation
# # add_indentation()

# ###########################################################################
# # FROM TERMINAL COMMAND
# # Go to specific page if argument is passed
# # TODO: when switching to native MPA v2, this might lead to errors
# ###########################################################################
# if AppState.args.phase == "all":  # type: ignore
#     st.switch_page("home.py")  # type: ignore
# ## ETL step
# for step_name, step_props in WIZARD_CONFIG["etl"]["steps"].items():
#     if AppState.args.phase == step_name:  # type: ignore
#         st.switch_page(step_props["entrypoint"])  # type: ignore
# ## Section
# for section in WIZARD_CONFIG["sections"]:
#     for app in section["apps"]:
#         if AppState.args.phase == app["alias"]:  # type: ignore
#             st.switch_page(app["entrypoint"])  # type: ignore
