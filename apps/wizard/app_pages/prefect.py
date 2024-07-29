"""Redirect to Prefect UI."""
import streamlit as st

from etl.config import OWID_ENV


def redirect_to(url: str):
    st.write(f'<meta http-equiv="refresh" content="0; url={url}">', unsafe_allow_html=True)
    st.stop()


redirect_to(OWID_ENV.prefect_url)
