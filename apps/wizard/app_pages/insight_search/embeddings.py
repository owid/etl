import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import streamlit as st
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger
from tqdm.auto import tqdm

# Initialize log.
log = get_logger()


@st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_model():
    "Load the pre-trained model."
    with st.spinner("Loading model..."):
        model = SentenceTransformer("all-MiniLM-L6-v2")
    return model


# cache every text to disk for faster startup
@st.cache_data(show_spinner=False, persist="disk")
def _encode_text(_model, insight_text: str):
    return _model.encode(insight_text, convert_to_tensor=True)


@st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_insights_embeddings(_model, insights: list[Dict[str, Any]]) -> list:
    with st.spinner("Generating embeddings..."):
        # Combine the title, body and authors of each insight into a single string.
        insights_texts = [
            insight["title"] + " " + insight["raw_text"] + " " + " ".join(insight["authors"]) for insight in insights
        ]

        # Run embedding generation sequentially.
        log.info("get_insights_embeddings.start", n_embeddings=len(insights))
        t = time.time()
        # TODO: it's unclear to me why using threads should speed it up since it is CPU bound
        with ThreadPoolExecutor() as executor:
            embeddings = list(
                tqdm(executor.map(lambda text: _encode_text(_model, text), insights_texts), total=len(insights_texts))
            )

        log.info("get_insights_embeddings.end", t=time.time() - t)

    return embeddings


def get_sorted_documents_by_similarity(
    model, input_string: str, insights: list[Dict[str, str]], embeddings: list
) -> list[Dict[str, Any]]:
    """Ingests an input string and a list of documents, returning the list of documents sorted by their semantic similarity to the input string."""
    _insights = insights.copy()

    # Encode the input string and the document texts.
    input_embedding = model.encode(input_string, convert_to_tensor=True)

    # Compute the cosine similarity between the input and each document.
    def _get_score(a, b):
        score = util.pytorch_cos_sim(a, b).item()
        score = (score + 1) / 2
        return score

    similarities = [_get_score(input_embedding, doc_embedding) for doc_embedding in embeddings]  # type: ignore

    # Attach the similarity scores to the documents.
    for i, doc in enumerate(_insights):
        doc["similarity"] = similarities[i]  # type: ignore

    # Sort the documents by descending similarity score.
    sorted_documents = sorted(_insights, key=lambda x: x["similarity"], reverse=True)

    return sorted_documents
