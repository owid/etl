"""Script to generate a quarterly analytics report for a data producer.

TODO: This is work in progress. Clean up code and move to producer analytics (and/or consider creating appropriate tables in analytics or metabase).

"""

from datetime import datetime

import click
import requests
from rich_click.rich_command import RichCommand
from structlog import get_logger

from apps.utils.google import GoogleDoc, GoogleDrive
from apps.wizard.app_pages.producer_analytics.data_io import get_producers_per_chart
from etl.analytics import get_chart_views_by_chart_id, get_post_views_by_chart_id
from etl.data_helpers.misc import round_to_sig_figs
from etl.db import get_engine

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()

# Folder ID for reports.
FOLDER_ID = "1SySOSNXgNLEJe2L1k7985p-zSeUoU4kN"

# Document ID of template.
TEMPLATE_ID = "149cLrJK9VI-BNjnM-LxnWgbQoB7mwjSpgjO497rzxeU"


# TODO: Move to data_helpers.
def humanize_number(number, sig_figs=2):
    if isinstance(number, int) and (number < 11):
        humanized = {
            0: "zero",
            1: "one",
            2: "two",
            3: "three",
            4: "four",
            5: "five",
            6: "six",
            7: "seven",
            8: "eight",
            9: "nine",
            10: "ten",
        }[number]
    else:
        scale_factors = {
            "quadrillion": 1e15,
            "trillion": 1e12,
            "billion": 1e9,
            "million": 1e6,
        }
        for scale_name, threshold in scale_factors.items():
            if number >= threshold:
                value = round_to_sig_figs(number / threshold, sig_figs)
                break
        else:
            value = round_to_sig_figs(number, sig_figs)
            scale_name = ""

        # Format number with commas.
        value_str = f"{value:,}"

        # Remove trailing zeros.
        if ("." in value_str) and (len(value_str.split(".")[0]) >= sig_figs):
            value_str = value_str.split(".")[0]

        # Combine number and scale.
        humanized = f"{value_str} {scale_name}".strip()

    return humanized


def get_chart_title_from_url(chart_url):
    response = requests.get(f"{chart_url}.metadata.json")
    title = response.json()["chart"]["title"]
    return title


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
    quarters = {
        1: {"name": "first", "min_date": "01-01", "max_date": "03-31"},
        2: {"name": "second", "min_date": "04-01", "max_date": "06-30"},
        3: {"name": "third", "min_date": "07-01", "max_date": "09-30"},
        4: {"name": "fourth", "min_date": "10-01", "max_date": "12-31"},
    }
    min_date = f"{year}-{quarters[quarter]['min_date']}"
    max_date = f"{year}-{quarters[quarter]['max_date']}"

    # Gather all necessary analytics data.
    # TODO: Refactor, clean up, and explain code.
    df_producer_charts = get_producers_per_chart(excluded_steps=[])
    df_producer_charts = df_producer_charts[df_producer_charts["producer"] == producer].reset_index(drop=True)
    producer_chart_ids = sorted(set(df_producer_charts["chart_id"]))
    df_producer = get_chart_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)
    assert df_producer[
        df_producer.duplicated(subset=["chart_id"])
    ].empty, "Expected no duplicates in df_producer. If there are, drop duplicates (and check if that's expected)."
    df_top_charts = df_producer.sort_values("views", ascending=False)[["url", "views"]].reset_index(drop=True).head(10)
    # Fetch titles from chart API.
    df_top_charts["title"] = df_top_charts["url"].apply(get_chart_title_from_url)

    df_content = get_post_views_by_chart_id(chart_ids=producer_chart_ids, date_min=min_date, date_max=max_date)
    df_content = (
        df_content.drop_duplicates(subset=["post_url"])
        .rename(columns={"post_title": "title", "post_url": "url"})
        .reset_index(drop=True)
    )

    df_articles = df_content[df_content["post_type"].isin(["article", "topic-page", "linear-topic-page"])].reset_index(
        drop=True
    )
    assert df_articles[
        df_articles.duplicated(subset=["url"])
    ].empty, "Expected no duplicates in df_articles. If there are, drop duplicates (and check if that's expected)."
    df_articles = df_articles.sort_values(["views"], ascending=False).reset_index(drop=True)
    df_top_articles = df_articles.head(10)

    df_insights = df_content[df_content["post_type"].isin(["data-insight"])].reset_index(drop=True)
    assert df_insights[
        df_insights.duplicated(subset=["url"])
    ].empty, "Expected no duplicates in df_insights. If there are, drop duplicates (and check if that's expected)."
    # TODO: Check if insights should be only the ones published in the quarter.
    df_insights = (
        df_insights[
            (df_insights["post_publication_date"] >= min_date) & (df_insights["post_publication_date"] <= max_date)
        ]
        .sort_values(["views"], ascending=False)
        .reset_index(drop=True)
    )
    df_top_insights = df_insights.head(10)

    # Create the required numeric inputs for the document.
    n_charts = len(df_producer)
    n_articles = len(df_articles)
    n_insights = len(df_insights)
    n_chart_views = df_producer["views"].sum()
    n_article_views = df_articles["views"].sum()
    # We have the average number of daily views for each chart. But we now want the macroaverage number of daily views.
    n_daily_chart_views = n_chart_views / df_producer["n_days"].max()
    n_daily_article_views = n_article_views / df_articles["n_days"].max()

    # Humanize numbers.
    n_charts_humanized = humanize_number(n_charts)
    n_articles_humanized = humanize_number(n_articles)
    n_chart_views_humanized = humanize_number(n_chart_views)
    n_daily_chart_views_humanized = humanize_number(n_daily_chart_views)
    n_article_views_humanized = humanize_number(n_article_views)
    n_daily_article_views_humanized = humanize_number(n_daily_article_views)
    n_insights_humanized = humanize_number(n_insights)
    max_date_humanized = datetime.strptime(max_date, "%Y-%m-%d").strftime("%B %d, %Y")
    quarter_date_humanized = f"the {quarters[quarter]['name']} quarter of {year}"

    ####################################################################################################################
    # Prepare text for report.
    # TODO: Handle plurals and cases where no DIs are published in the quarter. For now, manually edit the text.

    # Report title.
    report_title = f"{year}-Q{quarter} Our World in Data analytics report for {producer}"

    # Executive summary.
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
        executive_summary_intro += f""" and {n_articles_humanized} article{plural_articles}."""
    else:
        executive_summary_intro += f""", {n_articles_humanized} article{plural_articles}, and {n_insights_humanized} data insight{plural_insights}."""

    ####################################################################################################################

    # Initialize a google drive object.
    google_drive = GoogleDrive()
    # google_drive.list_files_in_folder(folder_id=FOLDER_ID)

    # Duplicate template report.
    report_id = google_drive.copy(file_id=TEMPLATE_ID, body={"name": report_title})

    # Initialize a google doc object.
    google_doc = GoogleDoc(doc_id=report_id)

    # Replace simple placeholders.
    replacements = {
        r"{{producer}}": producer,
        r"{{year}}": str(year),
        r"{{quarter}}": str(quarter),
        r"{{executive_summary_intro}}": executive_summary_intro,
        r"{{n_charts_humanized}}": n_charts_humanized,
        r"{{n_articles_humanized}}": n_articles_humanized,
        r"{{n_article_views_humanized}}": n_article_views_humanized,
        r"{{quarter_date_humanized}}": quarter_date_humanized,
        r"{{n_chart_views_humanized}}": n_chart_views_humanized,
        r"{{n_daily_chart_views_humanized}}": n_daily_chart_views_humanized,
        r"{{n_daily_article_views_humanized}}": n_daily_article_views_humanized,
    }
    google_doc.replace_text(mapping=replacements)

    def insert_list(google_doc, df, placeholder):
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

    top_chart_url = df_top_charts.iloc[0]["url"] + ".png"
    google_doc.insert_image(image_url=top_chart_url, placeholder=r"{{top_chart_image}}", width=320)
    insert_list(google_doc, df=df_top_charts, placeholder=r"{{top_charts_list}}")
    insert_list(google_doc, df=df_top_articles, placeholder=r"{{top_articles_list}}")
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
        insert_list(google_doc, df=df_top_insights, placeholder=r"{{data_insights}}")


if __name__ == "__main__":
    run()
