"""Script to generate a quarterly analytics report for a data producer."""

from datetime import datetime
from typing import Dict

import click
import pandas as pd
import requests
from rich_click.rich_command import RichCommand
from structlog import get_logger

from apps.utils.google import GoogleDoc, GoogleDrive
from etl.analytics import (
    get_chart_views_by_chart_id,
    get_post_views_by_chart_id,
    get_visualizations_using_data_by_producer,
)
from etl.data_helpers.misc import humanize_number
from etl.db import get_engine

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()

# Folder ID for reports.
FOLDER_ID = "1SySOSNXgNLEJe2L1k7985p-zSeUoU4kN"

# Document ID of template.
TEMPLATE_ID = "149cLrJK9VI-BNjnM-LxnWgbQoB7mwjSpgjO497rzxeU"

# Common definitions of quarters.
QUARTERS = {
    1: {"name": "first", "min_date": "01-01", "max_date": "03-31"},
    2: {"name": "second", "min_date": "04-01", "max_date": "06-30"},
    3: {"name": "third", "min_date": "07-01", "max_date": "09-30"},
    4: {"name": "fourth", "min_date": "10-01", "max_date": "12-31"},
}


def get_chart_title_from_url(chart_url: str) -> str:
    response = requests.get(f"{chart_url}.metadata.json")
    title = response.json()["chart"]["title"]
    return title


def run_sanity_checks(df_charts: pd.DataFrame, df_posts: pd.DataFrame, df_insights: pd.DataFrame) -> None:
    error = "Expected no duplicates in df_producer. If there are, drop duplicates (and check if that's expected)."
    assert df_charts[df_charts.duplicated(subset=["chart_id"])].empty, error

    error = "Unexpected post type."
    assert set(df_posts["post_type"]) <= set(["article", "topic-page", "linear-topic-page", "data-insight"]), error

    error = "Expected no duplicates in df_articles. If there are, drop duplicates (and check if that's expected)."
    assert df_posts[df_posts.duplicated(subset=["url"])].empty, error

    error = "Expected no duplicates in df_insights. If there are, drop duplicates (and check if that's expected)."
    assert df_insights[df_insights.duplicated(subset=["url"])].empty, error


def gather_producer_analytics(producer: str, min_date: str, max_date: str) -> Dict[str, pd.DataFrame]:
    # Get charts using data from the current data producer.
    df_producer_charts = get_visualizations_using_data_by_producer(producers=[producer])

    # Remove duplicate rows.
    # NOTE: This happens, for example, when a chart uses multiple snapshots of the same producer (so they are different origins for the same producer), e.g. chart 488 has two origins with producer "Global Carbon Project".
    df_producer_charts = df_producer_charts.drop_duplicates(subset=["chart_id"]).reset_index(drop=True)

    # List IDs of charts using data from the current data producer.
    producer_chart_ids = sorted(set(df_producer_charts["chart_id"]))

    # Get views for those charts.
    df_charts = get_chart_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)

    # Include chart titles.
    df_charts = df_charts.merge(df_producer_charts[["chart_id", "chart_title"]], how="left", on="chart_id").rename(
        columns={"chart_title": "title"}
    )

    # Include a column to signal if a chart was featured in the homepage.
    df_charts["featured_on_homepage"] = False

    # Get posts showing charts using data from the current data producer.
    # NOTE: Include DIs as part of posts (for the total view count). But then create a separate dataframe for DIs.
    df_posts = get_post_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)

    # This dataframe may contain the homepage among the list of posts.
    homepage_mask = df_posts["post_type"] == "homepage"
    # Remove the homepage from the list of posts, but add a column in the charts dataframe, to signal that the chart was featured in the homepage.
    if homepage_mask.any():
        df_charts.loc[
            df_charts["chart_id"].isin(sorted(set(df_posts[homepage_mask]["chart_id"]))), "featured_on_homepage"
        ] = True
        df_posts = df_posts.drop(homepage_mask[homepage_mask].index).reset_index(drop=True)  # type: ignore
    # Keep only the information about posts.
    df_posts = (
        df_posts.drop_duplicates(subset=["post_url"])
        .rename(columns={"post_title": "title", "post_url": "url"})
        .drop(columns=["chart_url", "chart_id"])
        .reset_index(drop=True)
    )

    # Create a separate dataframe for DIs published during the quarter.
    df_insights = df_posts[
        (df_posts["post_type"].isin(["data-insight"]))
        & (df_posts["post_publication_date"] >= min_date)
        & (df_posts["post_publication_date"] <= max_date)
    ].reset_index(drop=True)

    # Sanity checks.
    run_sanity_checks(df_charts=df_charts, df_posts=df_posts, df_insights=df_insights)

    # Create a dictionary with all analytics.
    analytics = {"charts": df_charts, "posts": df_posts, "insights_in_quarter": df_insights}

    return analytics


def insert_list_with_links_in_gdoc(google_doc: GoogleDoc, df: pd.DataFrame, placeholder: str) -> None:
    # For chart lists, get the index of the position where it should be introduced.
    insert_index = google_doc.find_marker_index(marker=placeholder)

    edits = []
    end_index = insert_index
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        title = row["title"]
        url = row["url"]
        views = f"{row['views']:,}"

        numbered_title = f"{i}. {title}"
        line = f"{numbered_title} – {views} views\n"

        # Add text for charts that have been featured on our homepage.
        if row["featured_on_homepage"]:
            line += "\n    This chart has also been featured on our homepage."

        # Insert line of text.
        edits.append({"insertText": {"location": {"index": end_index}, "text": line}})
        # Apply link to just the title (excluding "1. ").
        title_start = end_index + len(f"{i}. ")
        title_end = title_start + len(title)
        edits.append(
            {
                "updateTextStyle": {
                    "range": {"startIndex": title_start, "endIndex": title_end},
                    "textStyle": {"link": {"url": url}},
                    "fields": "link",
                }
            }
        )
        end_index += len(line)

    # Apply edits to insert list in the right place.
    google_doc.edit(requests=edits)

    # Remove the original placeholder text.
    google_doc.replace_text(mapping={placeholder: ""})


def create_report(producer: str, quarter: int, year: int, analytics: Dict[str, pd.DataFrame]) -> None:
    ####################################################################################################################
    # Gather inputs for report.

    # Create a dataframe of the top charts.
    df_top_charts = (
        analytics["charts"]
        .sort_values("views", ascending=False)[["url", "views", "title", "featured_on_homepage"]]
        .reset_index(drop=True)
        .iloc[0:10]
    )
    # Create a dataframe of the top posts (articles, topic pages and DIs).
    df_top_posts = (
        analytics["posts"]
        .sort_values(["views"], ascending=False)
        .reset_index(drop=True)
        .iloc[0:10]
        .assign(**{"featured_on_homepage": False})
    )
    # Create a dataframe of the top DIs (althought it will likely be the same as all DIs).
    df_top_insights = (
        analytics["insights_in_quarter"]
        .sort_values(["views"], ascending=False)
        .reset_index(drop=True)
        .iloc[0:10]
        .assign(**{"featured_on_homepage": False})
    )

    # Create the required numeric inputs for the document.
    n_charts = len(analytics["charts"])
    n_articles = len(analytics["posts"])
    n_insights = len(analytics["insights_in_quarter"])
    n_chart_views = analytics["charts"]["views"].sum()
    n_post_views = analytics["posts"]["views"].sum()
    # We have the average number of daily views for each chart. But we now want the macroaverage number of daily views.
    n_daily_chart_views = n_chart_views / analytics["charts"]["n_days"].max()
    n_daily_post_views = n_post_views / analytics["posts"]["n_days"].max()

    # Humanize numbers.
    n_charts_humanized = humanize_number(n_charts)
    n_posts_humanized = humanize_number(n_articles)
    n_chart_views_humanized = humanize_number(n_chart_views)
    n_daily_chart_views_humanized = humanize_number(n_daily_chart_views)
    n_post_views_humanized = humanize_number(n_post_views)
    n_daily_post_views_humanized = humanize_number(n_daily_post_views)
    n_insights_humanized = humanize_number(n_insights)
    max_date_humanized = datetime.strptime(f"{year}-{QUARTERS[quarter]['max_date']}", "%Y-%m-%d").strftime("%B %d, %Y")
    quarter_date_humanized = f"the {QUARTERS[quarter]['name']} quarter of {year}"

    # Report title.
    report_title = f"{year}-Q{quarter} Our World in Data analytics report for {producer}"

    ####################################################################################################################
    # Prepare executive summary.

    executive_summary_intro = f"""As of {max_date_humanized}, Our World in Data features your data in"""
    if n_charts == 0:
        raise AssertionError("Expected at least one chart to report.")
    elif n_charts == 1:
        executive_summary_intro += f""" {n_charts_humanized} chart"""
    else:
        executive_summary_intro += f""" {n_charts_humanized} charts"""
    if n_articles == 0:
        raise AssertionError("Expected at least one article to report.")

    plural_articles = "s" if n_articles > 1 else ""
    plural_insights = "s" if n_insights > 1 else ""
    if n_insights == 0:
        executive_summary_intro += f""" and {n_posts_humanized} article{plural_articles}."""
    else:
        executive_summary_intro += f""", {n_posts_humanized} article{plural_articles}, and {n_insights_humanized} data insight{plural_insights}."""

    ####################################################################################################################
    # Create a new google doc.

    # Initialize a google drive object.
    google_drive = GoogleDrive()
    # google_drive.list_files_in_folder(folder_id=FOLDER_ID)

    # Duplicate template report.
    report_id = google_drive.copy(file_id=TEMPLATE_ID, body={"name": report_title})

    # Initialize a google doc object.
    google_doc = GoogleDoc(doc_id=report_id)

    ####################################################################################################################
    # Replace simple placeholders.

    replacements = {
        r"{{producer}}": producer,
        r"{{year}}": str(year),
        r"{{quarter}}": str(quarter),
        r"{{executive_summary_intro}}": executive_summary_intro,
        r"{{n_charts_humanized}}": n_charts_humanized,
        r"{{n_posts_humanized}}": n_posts_humanized,
        r"{{n_post_views_humanized}}": n_post_views_humanized,
        r"{{quarter_date_humanized}}": quarter_date_humanized,
        r"{{n_chart_views_humanized}}": n_chart_views_humanized,
        r"{{n_daily_chart_views_humanized}}": n_daily_chart_views_humanized,
        r"{{n_daily_post_views_humanized}}": n_daily_post_views_humanized,
    }
    google_doc.replace_text(mapping=replacements)

    ####################################################################################################################
    # Populate pages of top charts, top posts, and top insights.

    top_chart_url = df_top_charts.iloc[0]["url"] + ".png"
    google_doc.insert_image(image_url=top_chart_url, placeholder=r"{{top_chart_image}}", width=320)
    insert_list_with_links_in_gdoc(google_doc, df=df_top_charts, placeholder=r"{{top_charts_list}}")
    insert_list_with_links_in_gdoc(google_doc, df=df_top_posts, placeholder=r"{{top_posts_list}}")
    if not df_top_insights.empty:
        # Get the index of the position of the data_insights placeholder.
        insert_index = google_doc.find_marker_index(marker=r"{{data_insights}}")

        if len(df_top_insights) == 1:
            text = f"""During {quarter_date_humanized}, the following data insight was also published:
            """
        else:
            text = f"""During {quarter_date_humanized}, the following data insights were also published:
            """
        edits = [{"insertText": {"location": {"index": insert_index}, "text": text}}]
        google_doc.edit(requests=edits)
        insert_list_with_links_in_gdoc(google_doc, df=df_top_insights, placeholder=r"{{data_insights}}")


@click.command(name="create_data_producer_report", cls=RichCommand, help=__doc__)
@click.option(
    "--producer",
    type=str,
    # multiple=True,
    # default=None,
    help="Producer name(s).",
)
@click.option(
    "--quarter",
    type=int,
    help="Quarter (1, 2, 3, or 4).",
)
@click.option(
    "--year",
    type=int,
    default=datetime.today().year,
    help="Year.",
)
def run(producer, quarter, year):
    min_date = f"{year}-{QUARTERS[quarter]['min_date']}"
    max_date = f"{year}-{QUARTERS[quarter]['max_date']}"

    # Gather producer analytics.
    analytics = gather_producer_analytics(producer=producer, min_date=min_date, max_date=max_date)

    ####################################################################################################################
    # Generate report.
    create_report(producer=producer, quarter=quarter, year=year, analytics=analytics)


if __name__ == "__main__":
    run()
