import time

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from structlog import get_logger

from apps.wizard.app_pages.related_charts.data import Chart
from apps.wizard.utils import embeddings as emb
from etl.db import read_sql

DEVICE = "cpu"

# Initialize log.
log = get_logger()


class ScoringModel:
    charts: list[Chart]

    # Embeddings for chart titles and subtitles
    model: SentenceTransformer
    emb_title: emb.EmbeddingsModel
    emb_subtitle: emb.EmbeddingsModel

    def __init__(self, model: SentenceTransformer, coviews_regularization: float = 0) -> None:
        self.model = model
        self.coviews_regularization = coviews_regularization

    def fit(self, charts: list[Chart]) -> None:
        self.charts = charts

        # Get embeddings for title and subtitle
        self.emb_title = emb.EmbeddingsModel(self.model, model_name="sim_charts_title")
        self.emb_title.fit(charts, text=lambda d: d.title)

        self.emb_subtitle = emb.EmbeddingsModel(self.model, model_name="sim_charts_subtitle")
        self.emb_subtitle.fit(charts, text=lambda d: d.subtitle)

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
                    "title_score": title_scores[i],
                    "subtitle_score": subtitle_scores[i],
                    # score 1 if there is at least one tag in common, 0 otherwise
                    "tags_score": float(bool(set(c.tags) & set(chart.tags))),
                    "share_indicator": float(c.chart_id in charts_sharing_indicator),
                    "pageviews": c.views_365d or 0,
                    "coviews": c.coviews or 0,
                }
            )

        ret = pd.DataFrame(ret).set_index("chart_id")

        assert ret.index.duplicated().sum() == 0

        # Empty subtitles are given a score of 0
        if chart.subtitle == "":
            ret["subtitle"] = 0

        ret["pageviews_score"] = score_pageviews(ret["pageviews"])
        ret["coviews_score"] = score_coviews(
            ret["coviews"], ret["pageviews"], regularization=self.coviews_regularization
        )
        ret["jaccard_score"] = score_jaccard(ret["coviews"], ret["pageviews"], chart.views_365d or 0)

        # Reorder
        ret = ret[
            [
                "title_score",
                "subtitle_score",
                "tags_score",
                "share_indicator",
                "pageviews_score",
                "coviews_score",
                "jaccard_score",
            ]
        ]

        log.info("similarity_components.end", t=time.time() - t)

        return ret


def score_pageviews(pageviews: pd.Series) -> pd.Series:
    """Log transform pageviews and scale them to [0, 1]. Chart with the most pageviews gets score 1 and
    chart with the least pageviews gets score 0.
    """
    pageviews = np.log(pageviews + 1)  # type: ignore
    return (pageviews - pageviews.min()) / (pageviews.max() - pageviews.min())


def score_coviews(coviews: pd.Series, pageviews: pd.Series, regularization: float) -> float:
    """Score coviews. First, get ratio of coviews to pageviews. Add regularization term to pageviews
    to penalize charts with high pageviews that tend to show up, despite being not very relevant.
    Then, normalize the score to [0, 1].
    """
    # p = coviews / (pageviews + lam)
    # return (p - p.min()) / (p.max() - p.min())
    p = coviews - regularization * pageviews
    return p / p.max()


def score_jaccard(coviews: pd.Series, pageviews: pd.Series, chosen_pageviews: float) -> pd.Series:
    """Score coviews using Jaccard similarity. Normalize the score to [0, 1]."""
    return coviews / (pageviews + chosen_pageviews - coviews)
