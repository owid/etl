import time
from dataclasses import dataclass
from typing import Optional

import streamlit as st
import torch
from sentence_transformers import SentenceTransformer, util
from structlog import get_logger

from apps.wizard.app_pages.insight_search import embeddings as emb
from apps.wizard.app_pages.similar_charts.data import Chart

DEVICE = "cpu"

# Initialize log.
log = get_logger()


class ScoringModel:
    model: SentenceTransformer
    chart_ids: list[int]
    embeddings: dict[str, torch.Tensor]

    def __init__(self, model: SentenceTransformer, weights: Optional[dict[str, float]]) -> None:
        self.model = model
        self.weights = weights

    def fit(self, charts: list[Chart]):
        self.chart_ids = [c.chart_id for c in charts]

        # Create an embedding for each chart.
        self.embeddings["title"] = get_chart_embeddings(
            self.model, [i.title for i in charts], model_name="sim_charts_title"
        )
        self.embeddings["subtitle"] = get_chart_embeddings(
            self.model, [i.subtitle for i in charts], model_name="sim_charts_subtitle"
        )

    def set_weights(self, weights: dict[str, float]):
        self.weights = weights

    def similarity(self, chart: Chart) -> dict[int, float]:
        log.info("calculate_similarity.start", n_docs=len(self.chart_ids))
        t = time.time()

        title_embedding = self.model.encode(chart.title, convert_to_tensor=True, device=DEVICE)
        # TODO: Missing subtitle should be treated differently
        subtitle_embedding = self.model.encode(chart.subtitle or "", convert_to_tensor=True, device=DEVICE)

        title_scores = _get_score(title_embedding, self.embeddings["title"])
        subtitle_scores = _get_score(subtitle_embedding, self.embeddings["subtitle"])

        # Attach the similarity scores to the documents.
        ret = {}
        for i, chart_id in enumerate(self.chart_ids):
            total_w = st.session_state.w_title + st.session_state.w_subtitle
            # TODO: Use vector computation
            similarity = (
                st.session_state.w_title * title_scores[i] + st.session_state.w_subtitle * subtitle_scores[i]
            ) / total_w
            ret[chart_id] = similarity

        log.info("calculate_similarity.end", t=time.time() - t)

        return ret

    def similarity_components(self, chart: Chart):
        pass


# Compute the cosine similarity between the input and each document.
def _get_score(input_embedding, embeddings, typ="cosine"):
    if typ == "cosine":
        score = util.pytorch_cos_sim(embeddings, input_embedding)
        score = (score + 1) / 2
    elif typ == "euclidean":
        # distance = torch.cdist(embeddings, input_embedding)
        score = util.euclidean_sim(embeddings, input_embedding)
        score = 1 / (1 - score)  # Normalize to [0, 1]
    else:
        raise ValueError(f"Invalid similarity type: {typ}")

    return score.cpu().numpy()[:, 0]


# TODO: memoization would be very expensive
@st.cache_data(show_spinner=False, max_entries=1)
def get_chart_embeddings(_model, _indicators_texts: list[str], model_name: str) -> torch.Tensor:
    with st.spinner("Generating embeddings..."):
        return emb.get_embeddings(_model, _indicators_texts, model_name=model_name)  # type: ignore
