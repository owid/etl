import json
import time
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer
from structlog import get_logger

from apps.utils.gpt import GPTQuery, OpenAIWrapper
from apps.wizard.app_pages.similar_charts.data import Chart
from apps.wizard.utils import embeddings as emb
from etl.db import read_sql

DEVICE = "cpu"

# Initialize log.
log = get_logger()


# These are the default thresholds for the different scores.
DEFAULT_WEIGHTS = {
    "title": 0.4,
    "subtitle": 0.1,
    "tags": 0.1,
    "pageviews": 0.3,
    "share_indicator": 0.1,
}

PREFIX_SYSTEM_PROMPT = """
You are an expert in recommending visual data insights.
Your task: From a given chosen chart and a list of candidate charts, recommend up to 5 charts that are most relevant.

Requirements:
"""

DEFAULT_SYSTEM_PROMPT = """
- Relevance should be based on thematic or conceptual similarity, but **avoid charts with very similar titles**.
- If fewer than 5 good matches are found, select only those that are truly relevant.
- Ensure diversity among the chosen charts.
- Provide concise reasoning for each recommendation.
""".strip()

SUFFIX_SYSTEM_PROMPT = """
The response should be in valid JSON format, mapping chart_slugs to their reasons for selection.
"""


class ScoringModel:
    charts: list[Chart]

    # Embeddings for chart titles and subtitles
    model: SentenceTransformer
    emb_title: emb.EmbeddingsModel
    emb_subtitle: emb.EmbeddingsModel

    # Weights for the different scores
    weights: dict[str, float]

    def __init__(self, model: SentenceTransformer, weights: Optional[dict[str, float]] = None) -> None:
        self.model = model
        self.weights = weights or DEFAULT_WEIGHTS.copy()

    def fit(self, charts: list[Chart]):
        self.charts = charts

        # Get embeddings for title and subtitle
        self.emb_title = emb.EmbeddingsModel(self.model, model_name="sim_charts_title")
        self.emb_title.fit(charts, text=lambda d: d.title)

        self.emb_subtitle = emb.EmbeddingsModel(self.model, model_name="sim_charts_subtitle")
        self.emb_subtitle.fit(charts, text=lambda d: d.subtitle)

    def set_weights(self, weights: dict[str, float]):
        self.weights = weights

    def similarity(self, chart: Chart) -> dict[int, float]:
        assert self.weights is not None, "Weights must be set before calling similarity"

        scores = self.similarity_components(chart)

        # Get weights and normalize them
        # w = pd.Series(self.weights)
        # w = w / w.sum()

        # Calculate total score
        return scores.sum(axis=1).to_dict()

    def similar_chart_by_title(self, title: str) -> int:
        title_scores = self.emb_title.calculate_similarity(title)
        d = dict(zip([c.chart_id for c in self.charts], title_scores))

        # Return chart_id with the highest score
        return max(d, key=d.get)  # type: ignore

    def similarity_components(self, chart: Chart) -> pd.DataFrame:
        log.info("similarity_components.start", n_docs=len(self.charts))
        t = time.time()

        title_scores = self.emb_title.calculate_similarity(chart.title)
        subtitle_scores = self.emb_subtitle.calculate_similarity(chart.subtitle or "")

        q = """
        select
            chartId
        from chart_dimensions as cd
        where variableId in (
        select variableId from chart_dimensions where chartId = %s
        )
        """
        charts_sharing_indicator = set(read_sql(q, params=(chart.chart_id,))["chartId"])

        # Attach the similarity scores to the documents.
        ret = []
        for i, c in enumerate(self.charts):
            ret.append(
                {
                    "chart_id": c.chart_id,
                    "title": title_scores[i],
                    "subtitle": subtitle_scores[i],
                    # score 1 if there is at least one tag in common, 0 otherwise
                    "tags": float(bool(set(c.tags) & set(chart.tags))),
                    "pageviews": c.views_365d or 0,
                    "share_indicator": float(c.chart_id in charts_sharing_indicator),
                }
            )

        ret = pd.DataFrame(ret).set_index("chart_id")

        assert ret.index.duplicated().sum() == 0

        # Empty subtitles are given a score of 0
        if chart.subtitle == "":
            ret["subtitle"] = 0

        # Scale pageviews to [0, 1]
        ret["pageviews"] = np.log(ret["pageviews"] + 1)
        ret["pageviews"] = (ret["pageviews"] - ret["pageviews"].min()) / (
            ret["pageviews"].max() - ret["pageviews"].min()
        )

        # Get weights and normalize them
        w = pd.Series(self.weights)
        w = w / w.sum()

        # Multiply scores by weights
        ret = (ret * w).fillna(0)

        # Reorder
        ret = ret[["title", "subtitle", "tags", "share_indicator", "pageviews"]]

        log.info("similarity_components.end", t=time.time() - t)

        return ret


@st.cache_data(show_spinner=True, persist="disk")
def gpt_diverse_charts(
    chosen_chart: Chart, _charts: list[Chart], _n: int = 30, system_prompt=DEFAULT_SYSTEM_PROMPT
) -> dict[str, str]:
    """Get diverse charts using GPT-4o. Return a dictionary with chart slugs as keys and reasons as values."""
    n = _n
    charts = _charts

    system_prompt = "\n".join([PREFIX_SYSTEM_PROMPT, system_prompt, SUFFIX_SYSTEM_PROMPT])

    user_prompt = f"""
    Please consider the chosen chart and the following {n} candidate charts.

    Identify 5 of the most relevant candidates according to the system's requirements.
    Output a JSON object with the chosen chart_slugs as keys and a brief reason for selection as values.

    Chosen chart:
    {json.dumps({"title": chosen_chart.title, "subtitle": chosen_chart.subtitle, "chart_slug": chosen_chart.slug}, indent=2)}

    Preselected charts:
    {json.dumps([{"title": c.title, "subtitle": c.subtitle, "chart_slug": c.slug} for c in charts[:n]], indent=2)}
    """

    api = OpenAIWrapper()
    query = GPTQuery(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
    )
    log.info("add_gpt_diversity.start")
    t = time.time()
    response = api.query_gpt(query=query, model="gpt-4o", response_format={"type": "json"})
    assert response
    log.info("add_gpt_diversity.end", cost=response.cost, t=time.time() - t)

    js = json.loads(response.choices[0].message.content.replace("```json", "").replace("```", ""))  # type: ignore

    return js
