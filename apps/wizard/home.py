"""Home page of wizard."""
import streamlit as st
from st_pages import add_indentation
from streamlit_card import card

# from streamlit_extras import load_key_css
from streamlit_extras.keyboard_text import key, load_key_css
from streamlit_extras.keyboard_url import keyboard_to_url
from streamlit_extras.switch_page_button import switch_page

add_indentation()

st.markdown(
    """
Wizard is a fundamental tool in the workflow of data scientists at OWID. It is used to easily create ETL steps, which are then run by the ETL pipeline to generate datasets in Grapher.

Additionally, it also builds on top of ETL tools to make them more accessible.
"""
)

#########################
# ETL Steps
#########################
st.markdown("## ETL steps")
st.markdown(
    """
Create an ETL step.
"""
)
pages = [
    {
        "title": "Snapshot",
        "image": "https://cdn.pixabay.com/photo/2014/10/16/09/15/lens-490806_1280.jpg",
    },
    {
        "title": "Meadow",
        "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5e/Blumenwiese_bei_Obermaiselstein05.jpg/1024px-Blumenwiese_bei_Obermaiselstein05.jpg",
    },
    {
        "title": "Garden",
        "image": "https://upload.wikimedia.org/wikipedia/commons/2/27/Butchart_gardens.JPG",
    },
    {
        "title": "Grapher",
        "image": "https://pbs.twimg.com/media/EbHwdjwUcAEfen4?format=jpg&name=large",
    },
]
columns = st.columns(len(pages))
load_key_css()
for i, page in enumerate(pages):
    # go_to_page = st.button(f"➡️  {page}")
    with columns[i]:
        go_to_page = card(
            **page,
            text=f"Press {i + 1}",
            styles={
                "card": {
                    "width": "150",
                    "height": "100px",
                    "padding": "0",
                    "margin": "0",
                    "font-size": ".8rem",
                }
            },
            on_click=lambda: None,
        )
        keyboard_to_url(key=str(i + 1), url=page["title"])
    if go_to_page:
        switch_page(page["title"])

#########################
# OTHER TOOLS
#########################
st.markdown("## Other tools")
st.markdown(
    """
Other helpfull tools in the ETL ecosystem.
"""
)
pages = [
    {
        "title": "Charts",
        "image": "https://camo.githubusercontent.com/38a295d2c16cd880446f874a786e94fb168d6aadfdcbc4c8b7dd45c8337b6d1d/68747470733a2f2f6f7572776f726c64696e646174612e6f72672f677261706865722f6578706f7274732f6c6966652d657870656374616e63792e737667",
        "key": "C",
    },
    {
        "title": "MetaGPT",
        "image": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/04/ChatGPT_logo.svg/1024px-ChatGPT_logo.svg.png",
        "key": "G",
    },
    {
        "title": "Dataset Explorer",
        "image": "https://cdn.pixabay.com/photo/2017/08/30/01/05/milky-way-2695569_1280.jpg",
        "key": "D",
    },
]
columns = st.columns(len(pages))
# keys = "qwertasdfgzxcvb".upper()
for i, page in enumerate(pages):
    k = page.pop("key")
    with columns[i]:
        go_to_page = card(
            **page,
            text=f"Press {k}",
            styles={
                "card": {
                    "width": "150",
                    "height": "100px",
                    "padding": "0",
                    "margin": "0",
                    "font-size": ".8rem",
                }
            },
            on_click=lambda: None,
        )
    keyboard_to_url(key=str(k), url=page["title"])
    if go_to_page:
        switch_page(page["title"])
