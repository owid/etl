"""Helper tool to create map brackets for all indicators in an indicator-based explorer.

"""
import streamlit as st

# PAGE CONFIG
st.set_page_config(
    page_title="Wizard: Explorer editor",
    page_icon="🪄",
)
st.title(":material/explore: Explorer Editor")


with st.container(border=True):
    st.subheader("IDs to Paths")
    st.markdown("Migrate all references to indicator IDs for their corresponding indicator paths.")

    uploaded_files = st.file_uploader(
        label="Upload Explorer config file",
        type="tsv",
    )
