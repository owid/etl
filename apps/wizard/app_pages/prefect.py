"""Redirect to Prefect UI."""
import requests
import streamlit as st

from etl.config import OWID_ENV


def redirect_to(url: str):
    try:
        response = requests.head(url, allow_redirects=True)
        if response.status_code == 200:
            st.write(f'<meta http-equiv="refresh" content="0; url={url}">', unsafe_allow_html=True)
            st.stop()
        else:
            st.error(f"Error: The URL returned status code {response.status_code}")
    except requests.RequestException:
        st.error("Prefect UI is not running. Run `make prefect-ui` to start it.")


redirect_to(OWID_ENV.prefect_url)
