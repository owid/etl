import datetime as dt
from typing import Optional

import click
import pandas as pd
import structlog
from rich_click.rich_command import RichCommand
from sqlalchemy import text
from tqdm.auto import tqdm

from apps.wizard.app_pages.similar_charts import data, scoring
from etl import config
from etl.db import get_engine

config.enable_bugsnag()
log = structlog.get_logger()


def load_data(chart_slug: Optional[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load chart data and coview sessions DataFrame.

    Returns:
        charts: DataFrame indexed by slug, containing metadata (like chart_id, views_365d, etc.)
        coviews_df: DataFrame with MultiIndex (slug1, slug2)
                    and columns ['coviews', 'pageviews'].
    """
    log.info("Loading chart data...")
    charts = data.get_raw_charts().set_index("slug", drop=False)

    # If chart_slug is provided, verify it's in charts
    if chart_slug and chart_slug not in charts.index:
        log.warning("Chart slug not found in data. Exiting.", chart_slug=chart_slug)
        return pd.DataFrame(), pd.DataFrame()

    log.info("Loading coview sessions...")
    coviews_df = data.get_coviews_sessions(
        after_date=str(dt.date.today() - dt.timedelta(days=365)), min_sessions=3
    ).to_frame(name="coviews")

    # If a single chart slug is given, filter for that slug1 only
    if chart_slug:
        coviews_df = coviews_df[coviews_df.index.get_level_values("slug1") == chart_slug]

    # Filter out any coviews rows whose slug1 isn't in our charts
    coviews_df = coviews_df[coviews_df.index.get_level_values("slug1").isin(charts.index)]

    # Add pageviews of slug2 to coviews dataframe
    coviews_df["pageviews"] = charts["views_365d"].reindex(coviews_df.index.get_level_values("slug2")).values

    return charts, coviews_df


def compute_recommendations(
    charts: pd.DataFrame,
    coviews_df: pd.DataFrame,
    chart_slug: Optional[str],
    top: int,
    regularization: float,
) -> pd.DataFrame:
    """
    Given charts and coview data, compute a DataFrame of recommended pairs:
    chosen_chart, related_chart, chartId, relatedChartId, etc.

    The 'score' is computed as:
        score = coviews - regularization * pageviews

    Args:
        charts: DataFrame of chart metadata.
        coviews_df: DataFrame with columns ['coviews', 'pageviews'].
        chart_slug: Optional single slug to process; otherwise compute for all slugs.
        top: How many top-related charts to retrieve for each slug.
        regularization: Factor to penalize high-view charts.

    Returns:
        A DataFrame of recommended chart pairs (chosen_chart, related_chart, score, etc.).
    """
    # If we failed to load data (e.g., an invalid slug), return empty
    if charts.empty or coviews_df.empty:
        return pd.DataFrame()

    # Compute the score
    coviews_df["score"] = coviews_df["coviews"] - regularization * coviews_df["pageviews"]

    # If a single chart slug is requested, ensure we only keep those rows
    if chart_slug:
        if chart_slug not in coviews_df.index.get_level_values("slug1"):
            log.info("No coview data for this chart slug.", chart_slug=chart_slug)
            return pd.DataFrame()
        coviews_df = coviews_df.loc[[chart_slug]]

    recommended_rows = []

    # Group by 'slug1' so each group has all charts related to that slug1
    grouped = coviews_df.groupby(level="slug1", sort=False)
    log.info("Calculating related charts...")

    for slug1, group in tqdm(grouped, desc="Calculating related charts"):
        top_related = group.sort_values("score", ascending=False).head(top)
        for related_slug, score in zip(top_related.index.get_level_values("slug2"), top_related["score"]):
            recommended_rows.append({"chosen_chart": slug1, "related_chart": related_slug, "score": score})

    if not recommended_rows:
        return pd.DataFrame()

    # Build the recommendations DataFrame
    recommended_df = pd.DataFrame(recommended_rows)
    recommended_df["chartId"] = recommended_df["chosen_chart"].map(charts["chart_id"])
    recommended_df["relatedChartId"] = recommended_df["related_chart"].map(charts["chart_id"])
    recommended_df["label"] = "good"
    recommended_df["reviewer"] = "production"

    # Warn if some related_chart slugs can't be mapped to chartIds
    ix_missing = recommended_df["relatedChartId"].isnull()
    if ix_missing.any():
        log.warning("Chart ID not found for some related chart slugs.", n_missing=ix_missing.sum())
        recommended_df = recommended_df[~ix_missing]

    return recommended_df


def write_recommendations(
    engine, recommended_df: pd.DataFrame, charts: pd.DataFrame, chart_slug: Optional[str]
) -> None:
    """
    Writes the recommended DataFrame to the 'related_charts' table in the database.
    If 'chart_slug' is specified, only deletes existing rows for that slug before inserting.
    Otherwise, clears all 'production' rows first.
    """
    if recommended_df.empty:
        log.info("No related charts found. Nothing to write.")
        return

    with engine.begin() as conn:
        if chart_slug:
            log.info("Deleting existing 'production' reviews for this chart.", chart_slug=chart_slug)
            conn.execute(
                text("""
                    DELETE FROM related_charts
                    WHERE reviewer = 'production' AND chartId = :chartId
                """),
                {"chartId": charts.loc[chart_slug, "chart_id"]},
            )
        else:
            log.info("Deleting all existing 'production' reviews.")
            conn.execute(text("DELETE FROM related_charts WHERE reviewer = 'production'"))

        log.info("Inserting new related chart records.", rows=len(recommended_df))
        recommended_df[["chartId", "relatedChartId", "label", "reviewer", "score"]].to_sql(
            "related_charts", con=conn, if_exists="append", index=False
        )


@click.command(name="related-charts", cls=RichCommand, help=__doc__)
@click.option(
    "--chart-slug",
    type=str,
    help="Get related charts only for the chart with this slug.",
)
@click.option(
    "--top",
    type=int,
    default=6,
    help="Pick the top N related charts.",
)
@click.option(
    "--regularization", type=float, default=0.001, help="Factor by which to penalize charts with high pageviews."
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    help="If set, no changes will be written to the database.",
)
def cli(chart_slug: Optional[str], top: int, regularization: float, dry_run: bool) -> None:
    """
    Generates a table of related charts (by coviews) and optionally writes them
    to the database. If a single chart slug is provided, only that chart’s
    related charts will be generated.
    """
    engine = get_engine()

    # 1. Load data (no score calculated here)
    charts, coviews_df = load_data(chart_slug)

    # 2. Compute recommendations (score is applied here)
    recommended_df = compute_recommendations(charts, coviews_df, chart_slug, top, regularization)

    if recommended_df.empty:
        log.info("No recommendations generated. Exiting.")
        return

    # 3. Dry-run check
    if dry_run:
        log.info("Dry run mode enabled. No changes will be written to the database.")
        log.info("Recommended DataFrame preview:", data=recommended_df.head())
        return

    # 4. Otherwise, write to DB
    write_recommendations(engine, recommended_df, charts, chart_slug)
    log.info("Related charts updated successfully.")


if __name__ == "__main__":
    cli()
