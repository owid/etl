import os
import pickle
import time
from typing import Any, Dict, Optional

import streamlit as st
import torch
from joblib import Memory
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger
from tqdm.auto import tqdm

from apps.wizard.app_pages.insight_search import embeddings as emb
from apps.wizard.app_pages.insight_search.embeddings import get_model, get_sorted_documents_by_similarity
from etl.paths import CACHE_DIR


# TODO: caching isn't working properly when on different devices
# @st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_indicators_embeddings(_model, indicators: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the name and description into a single string
        indicators_texts = [indicator["name"] + " " + indicator["description"] for indicator in indicators]

        return emb.get_embeddings(_model, indicators_texts)
