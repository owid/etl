"""Play around with producer analytics."""

from datetime import datetime

import pandas as pd
import requests
from structlog import get_logger

from apps.utils.google import GoogleDocHandler
from apps.wizard.app_pages.producer_analytics.data_io import get_analytics
from etl.config import OWID_ENV
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


def run():
    # Inputs:
    year = 2025
    quarter = 1
    producer = "Global Carbon Project"

    # TODO: This is work in progress. Clean up code and move to producer analytics (and/or consider creating appropriate tables in analytics or metabase).
    quarters = {
        1: {"name": "first", "min_date": "01-01", "max_date": "03-31"},
        2: {"name": "second", "min_date": "04-01", "max_date": "06-30"},
        3: {"name": "third", "min_date": "07-01", "max_date": "09-30"},
        4: {"name": "fourth", "min_date": "10-01", "max_date": "12-31"},
    }
    min_date = f"{year}-{quarters[quarter]['min_date']}"
    max_date = f"{year}-{quarters[quarter]['max_date']}"
    df_producer_analytics = get_analytics(min_date, max_date, excluded_steps=[])

    # TODO: The following contains only grapher charts. Would it be possible to include explorers?
    df_producer = df_producer_analytics[df_producer_analytics["producer"] == producer].reset_index(drop=True)

    df_top_charts = (
        df_producer.sort_values("views_custom", ascending=False)[["chart_url", "views_custom"]]
        .rename(columns={"chart_url": "url", "views_custom": "views"})
        .reset_index(drop=True)
        .head(10)
    )
    # Fetch titles from chart API.
    df_top_charts["title"] = df_top_charts["url"].apply(get_chart_title_from_url)

    # I need to find references of charts in articles.
    # NOTE: A priori, the query could be as simple as follows:
    # query = """SELECT pg.slug, pg.type, pg.published, pg.authors, pgl.target, pgl.linkType, pgl.componentType
    # FROM posts_gdocs pg
    # INNER JOIN posts_gdocs_links pgl
    # on pg.id = pgl.sourceId
    # WHERE pg.published = 1
    # """
    # But with that we would not be able to link articles and DIs that use narrative charts.
    # Instead, the following query links articles and DIs to the parent chart of narrative charts.
    query = """SELECT
        pg.slug AS post_slug,
        pg.type,
        pg.published,
        pg.authors,
        pg.publishedAt,
        JSON_UNQUOTE(JSON_EXTRACT(pg.content, '$.title')) AS title,
        pgl.target,
        pgl.linkType,
        pgl.componentType,
        cc.slug AS parent_chart_slug
    FROM
        posts_gdocs pg
    INNER JOIN
        posts_gdocs_links pgl ON pg.id = pgl.sourceId
    LEFT JOIN
        chart_views cv ON pgl.target = cv.name
    LEFT JOIN
        charts c ON cv.parentChartId = c.id
    LEFT JOIN
        chart_configs cc ON c.configId = cc.id
    WHERE
        pg.published = 1"""

    df_links = OWID_ENV.read_sql(query)
    # Create article urls and data insight urls.
    OWID_BASE_URL = "https://ourworldindata.org/"
    url_start = {
        # Content "type":
        "article": OWID_BASE_URL,
        "linear-topic-page": OWID_BASE_URL,
        "topic-page": OWID_BASE_URL,
        "data-insight": OWID_BASE_URL + "data-insights/",
        # 'about-page',
        # 'fragment',
        # 'author',
        # 'homepage',
        # Cited object "linkType":
        "grapher": OWID_BASE_URL + "grapher/",
        # Chart views, in theory, refer to narrative charts, which don't have a public URL.
        # They are handled separately.
        # NOTE: there are chart views for non-narrative charts, so there may be other cases I'm not considering.
        # "chart-view": OWID_BASE_URL + "grapher/",
        "explorer": OWID_BASE_URL + "explorers/",
        # 'gdoc',
        "url": "",
    }
    # Transform slugs or articles, topic pages, and data insights into urls.
    df_links["content_url"] = df_links["type"].map(url_start) + df_links["post_slug"]
    # Transform slugs of grapher charts into urls.
    # If there is a parent chart id, use that, otherwise, use the target chart.
    # NOTE: For now, call this column 'chart_url', to match that column of df_producers. But if explorers are included, rename it.
    # df_links["chart_url"] = df_links["linkType"].map(url_start) + df_links["target"]
    df_links["chart_url"] = df_links["linkType"].map(url_start) + df_links["parent_chart_slug"].fillna(
        df_links["target"]
    )
    df_links = df_links.drop(columns=["parent_chart_slug"])

    # Find list of DIs that don't have a grapher url.
    # This yields a list of 27 DIs. They don't have a grapher-url because
    # _df = df_links[(df_links["type"]=="data-insight")].groupby("slug").agg({"componentType": lambda x: "front-matter" in x.unique(), "authors": "first", "content_url": "first"})
    # _df = _df[~_df["componentType"]].reset_index(drop=True).drop(columns="componentType").assign(**{"wrong_url": ""})
    # # The field grapher-url should lead to a grapher or explorer. Find cases where that's not fulfilled (I see that they are often leading to an admin or a staging-site url).
    # _df_added = df_links[(df_links["componentType"]=="front-matter") & (~df_links["chart_url"].str.contains(r"\/grapher\/|\/explorers\/", na=False, regex=True))][["authors", "content_url", "chart_url"]].drop_duplicates().rename(columns={"chart_url": "wrong_url"}).reset_index(drop=True)
    # _df = _df.merge(_df_added, on=["authors", "content_url", "wrong_url"], how="outer")

    # Remove rows without content or charts.
    df_links = df_links.dropna(subset=["content_url", "chart_url"], how="any").reset_index(drop=True)

    # This table shows all written content (articles, topic pages and data insights) that either cites or displays a certain chart (or explorer, eventually). We have two options:
    # 1) We count charts that are cited or displayed.
    # 2) We only count charts that are displayed.
    # I think that we probably 2) is better: we should link an article to a data producer if it displays a chart (or explorer) that uses data from that producer.
    # If the article simply cites a chart from that producer, I think views of that article should not be counted as views of the producer's content.
    # So, let's ensure that we count only charts that are embedded, not just cited.
    # To do that, I see the following options for componentType:
    ALLOWED_COMPONENT_TYPES = [
        # All-charts blocks embedded in topic pages (and exceptionally one article: https://ourworldindata.org/human-development-index )
        "all-charts",
        # Embedded grapher charts.
        "chart",
        # Charts embedded in a special way for the SDG tracker.
        "chart-story",
        # Charts embedded as key insights of topic pages.
        "key-insights",
        # This refers to embedded narrative charts.
        "narrative-chart",
        # This seems to refer to cited links, so we decided to ignore them.
        # 'span-link',
        # This refers to videos (which are just a few, and we have no way to link to data producers).
        # 'video',
        # This is used for the grapher-url defined in the metadata of data insights.
        # We will use this field to connect data insights to the original grapher chart.
        # TODO: The grapher-url field is not always filled out. If the static chart was manually created, it makes sense, but otherwise, it should probably exist. Maybe even if it comes from a static chart, it's good to have a grapher-url to "the closest" chart. That way we can connect to data providers and topic. For now, make a list of DIs that don't have this field, and ask around if it should always be filled out.
        # NOTE: Data insights using a static chart (that didn't come from any grapher chart) will not be considered.
        "front-matter",
        # This is used for links that appear in a special box.
        # When it's linked to a chart, it shows a very small thumbnail, so we can exclude it.
        # 'prominent-link',
        # Content shown (as a thumbnail) in the Research & Writing tab of topic pages.
        # Given that it simply shows a dummy thumbnail, exclude it.
        # 'research-and-writing',
        # This seems to be just links that appear as "RELATED TOPICS" section of topic pages.
        # They don't show any data from data producers, so exclude them.
        # 'topic-page-intro',
    ]
    # By eye, I can tell that 'span-link' refers to links in the text (sometimes those links are grapher charts), and 'chart' or 'chart-view' refer to embedded grapher charts.
    # As an exmple, see
    # df_links[(df_links["content_url"]=="https://ourworldindata.org/what-is-foreign-aid")]
    # which has both grapher charts cited and embedded.
    # TODO: We are ignoring possible static charts showing data from the producer. We should find out a way to count them too.
    # Find all articles, topic pages and data insights that display grapher charts for the given producer.
    df_content = (
        df_links[
            (df_links["componentType"].isin(ALLOWED_COMPONENT_TYPES))
            & (df_links["chart_url"].isin(df_producer["chart_url"]))
        ][["type", "content_url", "publishedAt", "title"]]
        .rename(columns={"publishedAt": "publication_date"})
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df_content["publication_date"] = df_content["publication_date"].dt.date.astype(str)

    # This gives us all articles, topic pages and data insights published.
    # df_content

    # Create a list with all content urls (charts and articles) that displays data from a data producer.
    content_urls = sorted(set(df_producer["chart_url"]) | set(df_content["content_url"]))

    # Now get only data insights published during the relevant period.
    df_content[
        (df_content["type"] == "data-insight")
        & (df_content["publication_date"] >= min_date)
        & (df_content["publication_date"] <= max_date)
    ]

    ####################################################################################################################
    # Gather analytics for the writtent content in this dataframe, i.e. number of views in articles, topic pages (and possibly DIs).
    query = f"""
    SELECT url, SUM(views) as views,
    FROM prod_google_analytics4.views_by_day_page
    WHERE url in {tuple(content_urls)}
        and day >= '{min_date}'
        and day <= '{max_date}'
    GROUP BY url
    """
    # Execute the query.
    df_views = pd.read_gbq(query, project_id="owid-analytics")
    df_views = df_views.merge(
        df_content[["content_url", "title"]].rename(columns={"content_url": "url"}), on="url", how="left"
    )

    df_articles = (
        df_views[
            df_views["url"].isin(set(df_content[df_content["type"].isin(["article", "topic-page"])]["content_url"]))
        ]
        .sort_values(["views"], ascending=False)
        .reset_index(drop=True)
    )
    df_top_articles = df_articles.head(10)

    df_insights = (
        df_views[
            df_views["url"].isin(
                set(
                    df_content[
                        (df_content["type"].isin(["data-insight"]))
                        & (df_content["publication_date"] >= min_date)
                        & (df_content["publication_date"] <= max_date)
                    ]["content_url"]
                )
            )
        ]
        .sort_values(["views"], ascending=False)
        .reset_index(drop=True)
    )
    df_top_insights = df_insights.head(10)

    ####################################################################################################################
    # The following code was discarded.
    # We decided that citations need to be inspected by a human, instead of fetched automatically.
    # # From Metabase policy_mentions (selecting the right date range):
    # # I manually selected 2025 Q1, and downloaded the file.
    # df_policy = pd.read_csv("~/Downloads/query_result_2025-04-18T13_25_42.754831862Z.csv")

    # # Compile the regex pattern once
    # regex_pattern = re.compile(r'https:\/\/ourworldindata\.org\/[^\s"\'\)\]\.,;:]+')
    # df_policy["owid_urls"] = df_policy["matched_mentions"].apply(lambda x: regex_pattern.findall(x or ""))
    # # Create one row per mention.
    # df_policy = df_policy.explode("owid_urls").dropna(subset="owid_urls").reset_index(drop=True)

    # # Clean owid urls.
    # df_policy["owid_urls"] = [url.split("?")[0] for url in df_policy["owid_urls"]]

    # # Identify policy mentions of the content (charts and articles) that display data from the data producer.
    # df_policy = df_policy[df_policy["owid_urls"].isin(content_urls)].reset_index(drop=True)

    # # It seems there are duplicates, remove them.
    # df_policy = df_policy.drop_duplicates(subset=["url", "pdf_url"]).reset_index(drop=True)

    # This returns a list of policy documents that cite one of our articles, that displays data from a data producer.

    ####################################################################################################################

    # Create the required numeric inputs for the document.
    n_charts = len(df_producer)
    n_articles = len(df_content[df_content["type"].isin(["article", "topic-page"])])
    n_insights = len(df_content[df_content["type"].isin(["data-insight"])])
    n_chart_views = df_producer["views_custom"].sum()
    n_article_views = df_views["views"].sum()
    n_days_in_quarter = (datetime.strptime(max_date, "%Y-%m-%d") - datetime.strptime(min_date, "%Y-%m-%d")).days + 1
    n_daily_chart_views = n_chart_views / n_days_in_quarter
    n_daily_article_views = n_article_views / n_days_in_quarter

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

    # TODO: Make into a function.
    # def create_report(producer: str, quarter: int, year: int):
    report_title = f"{year}-Q{quarter} Our World in Data analytics report for {producer}"
    # Initialize a google doc object.
    google_doc = GoogleDocHandler()
    # google_doc.list_files_in_folder(folder_id=FOLDER_ID)
    # Duplicate template report.
    # NOTE: Unclear why sometimes we need to use "title" and sometimes "name".
    report_id = google_doc.copy(doc_id=TEMPLATE_ID, body={"name": report_title})

    # Replace simple placeholders.
    replacements = {
        r"{{producer}}": "Global Carbon Project",
        r"{{year}}": "2025",
        r"{{quarter}}": "Q1",
        r"{{executive_summary_intro}}": executive_summary_intro,
        r"{{n_charts_humanized}}": n_charts_humanized,
        r"{{n_articles_humanized}}": n_articles_humanized,
        r"{{n_article_views_humanized}}": n_article_views_humanized,
        r"{{quarter_date_humanized}}": quarter_date_humanized,
        r"{{n_chart_views_humanized}}": n_chart_views_humanized,
        r"{{n_daily_chart_views_humanized}}": n_daily_chart_views_humanized,
        r"{{n_daily_article_views_humanized}}": n_daily_article_views_humanized,
    }
    google_doc.replace_text(doc_id=report_id, mapping=replacements)

    def insert_list(google_doc, df, placeholder):
        # For chart lists, get the index of the position where it should be introduced.
        insert_index = google_doc.find_marker_index(doc_id=report_id, marker=placeholder)

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
        google_doc.edit(doc_id=report_id, requests=edits)

        # Remove the original placeholder text.
        google_doc.replace_text(doc_id=report_id, mapping={placeholder: ""})

    def insert_image(google_doc, image_url, placeholder, width=350):
        # Get the index of the position where the image should be inserted.
        insert_index = google_doc.find_marker_index(doc_id=report_id, marker=placeholder)

        edits = [
            {
                "insertInlineImage": {
                    "location": {"index": insert_index},
                    "uri": image_url,
                    "objectSize": {
                        # "height": {
                        #     "magnitude": 200,
                        #     "unit": "PT"
                        # },
                        "width": {"magnitude": width, "unit": "PT"}
                    },
                }
            },
            {
                "updateParagraphStyle": {
                    "range": {"startIndex": insert_index, "endIndex": insert_index + 1},
                    "paragraphStyle": {"alignment": "CENTER"},
                    "fields": "alignment",
                }
            },
        ]

        # Apply both image insert and alignment.
        google_doc.edit(doc_id=report_id, requests=edits)

        # Remove the original placeholder text.
        google_doc.replace_text(doc_id=report_id, mapping={placeholder: ""})

    top_chart_url = df_top_charts.iloc[0]["url"] + ".png"
    insert_image(google_doc, image_url=top_chart_url, placeholder=r"{{top_chart_image}}", width=320)
    insert_list(google_doc, df=df_top_charts, placeholder=r"{{top_charts_list}}")
    insert_list(google_doc, df=df_top_articles, placeholder=r"{{top_articles_list}}")
    insert_list(google_doc, df=df_top_insights, placeholder=r"{{top_insights_list}}")


if __name__ == "__main__":
    run()
