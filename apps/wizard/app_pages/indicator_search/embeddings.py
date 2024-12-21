from typing import Any, Dict

import streamlit as st

from apps.wizard.app_pages.insight_search import embeddings as emb


# TODO: caching isn't working properly when on different devices
# @st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_indicators_embeddings(_model, indicators: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the name and description into a single string
        indicators_texts = [indicator["name"] + " " + indicator["description"] for indicator in indicators]

        return emb.get_embeddings(_model, indicators_texts)  # type: ignore
