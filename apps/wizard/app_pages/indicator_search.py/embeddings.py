from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import streamlit as st
from sentence_transformers import SentenceTransformer
from tqdm.auto import tqdm


@st.cache_data(show_spinner=False)
def get_model():
    "Load the pre-trained model."
    with st.spinner("Loading model..."):
        model = SentenceTransformer("all-MiniLM-L6-v2")
    return model


# TODO: how does caching work if we supply model?
@st.cache_data(show_spinner=False)
def get_insights_embeddings(model, insights: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the title, body and authors of each insight into a single string.
        insights_texts = [
            insight["title"] + " " + insight["raw_text"] + " " + " ".join(insight["authors"]) for insight in insights
        ]

        # Run embedding generation in parallel.
        def _encode_text(text):
            return model.encode(text, convert_to_tensor=True)

        with ThreadPoolExecutor() as executor:
            embeddings = list(tqdm(executor.map(_encode_text, insights_texts), total=len(insights_texts)))

    return embeddings
