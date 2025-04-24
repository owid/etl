"""Play around with producer analytics."""

import re
from datetime import datetime

import pandas as pd
import requests
from IPython.display import HTML, display
from structlog import get_logger

from apps.wizard.app_pages.producer_analytics.data_io import get_analytics
from etl.config import OWID_ENV
from etl.data_helpers.misc import round_to_sig_figs
from etl.db import get_engine

# Initialize logger.
log = get_logger()

# Initialize database engine.
engine = get_engine()


# TODO: Move to data_helpers.
def humanize_number(number, sig_figs=2):
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
        .reset_index(drop=True)
        .head(10)
    )
    # Fetch titles from chart API.
    df_top_charts["chart_title"] = df_top_charts["chart_url"].apply(get_chart_title_from_url)

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
        JSON_UNQUOTE(JSON_EXTRACT(pg.content, '$.title')) AS article_title,
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
        ][["type", "content_url", "publishedAt", "article_title"]]
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
        df_content[["content_url", "article_title"]].rename(columns={"content_url": "url"}), on="url", how="left"
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
    # From Metabase policy_mentions (selecting the right date range):
    # I manually selected 2025 Q1, and downloaded the file.
    # TODO: Gather this programmatically from policy_mentions
    df_policy = pd.read_csv("~/Downloads/query_result_2025-04-18T13_25_42.754831862Z.csv")

    # Compile the regex pattern once
    regex_pattern = re.compile(r'https:\/\/ourworldindata\.org\/[^\s"\'\)\]\.,;:]+')
    df_policy["owid_urls"] = df_policy["matched_mentions"].apply(lambda x: regex_pattern.findall(x or ""))
    # Create one row per mention.
    df_policy = df_policy.explode("owid_urls").dropna(subset="owid_urls").reset_index(drop=True)

    # Clean owid urls.
    df_policy["owid_urls"] = [url.split("?")[0] for url in df_policy["owid_urls"]]

    # Identify policy mentions of the content (charts and articles) that display data from the data producer.
    df_policy = df_policy[df_policy["owid_urls"].isin(content_urls)].reset_index(drop=True)

    # It seems there are duplicates, remove them.
    df_policy = df_policy.drop_duplicates(subset=["url", "pdf_url"]).reset_index(drop=True)

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
    n_policy_papers = len(df_policy)
    # TODO: Figure out a way to get this number automatically.
    # For now, gather these manually:
    # * Print df_insights, and, for each one (or at least the most popular one):
    # * Identify the insight in https://admin.owid.io/admin/data-insights
    # * Identify (and download) the corresponding static chart in https://admin.owid.io/admin/images
    # * Scroll through insta and select any post where Data Source is the current producer.
    #   Stop scrolling beyond the current quarter.
    # * Add up likes and comments of that post, and write it here.
    n_max_social_media_interactions = 7100

    # Humanize numbers.
    n_charts_humanized = humanize_number(n_charts)
    n_articles_humanized = humanize_number(n_articles)
    n_chart_views_humanized = humanize_number(n_chart_views)
    n_daily_views_humanized = humanize_number(n_daily_chart_views)
    n_policy_papers_humanized = humanize_number(n_policy_papers)
    n_max_social_media_interactions_humanized = humanize_number(n_max_social_media_interactions)
    max_date_humanized = datetime.strptime(max_date, "%Y-%m-%d").strftime("%B %d, %Y")
    quarter_date_humanized = f"the {quarters[quarter]['name']} quarter of {year}"

    ####################################################################################################################
    # Prepare text for report.
    # TODO: For now, the content is manually copy/pasted. Eventually, we'll have to figure out how to generate a gdoc.
    # NOTE: paste only the content, and keep the titles untouched.
    # TODO: Handle plurals and cases where no DIs are published in the quarter. For now, manually edit the text.

    # Executive summary.
    intro = f"""As of {max_date_humanized}, Our World in Data features your data in {n_charts_humanized} charts, {n_articles_humanized} articles and {n_insights} data insights.<br><br>
    During {quarter_date_humanized}, those charts generated <b>{n_chart_views_humanized} views</b>. This amounts to an average of <b>{n_daily_views_humanized} views per day</b>.<br><br>"""
    if n_policy_papers == 1:
        intro += f"""During this period, that content has also been cited by <b>{n_policy_papers_humanized} policy document</b>.<br><br>"""
    elif n_policy_papers > 1:
        intro += f"""During this period, that content has also been cited by <b>{n_policy_papers_humanized} policy documents</b>.<br><br>"""
    if n_max_social_media_interactions > 0:
        intro += f"""We also share content across various social media platforms, where we've built a cumulative audience of over 100,000 followers. Our most popular post featuring your data accumulated a total of <b>{n_max_social_media_interactions_humanized} interactions</b>.<br>"""
    display_format_prefix = """<div style="font-family: 'Lato', sans-serif; font-size: 13px; color: black;">"""
    display(HTML(display_format_prefix + intro + "</div>"))

    ####################################################################################################################

    # Top performing charts.
    chart_list_html = ""

    # Build list of clickable titles.
    chart_list_html = "\n".join(
        [
            f"""<li>
            <a href="{row['chart_url']}" target="_blank" style="color: #1155cc; text-decoration: none;">
                {row['chart_title']}
            </a> – {row['views_custom']:,} views
        </li>"""
            for _, row in df_top_charts.iterrows()
        ]
    )
    top_charts = f"""
    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        Your data is featured in {n_charts_humanized} charts at Our World in Data.
    </p>

    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        During {quarter_date_humanized}, these charts received:
    </p>

    <ul style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: -8px; margin-bottom: 16px;">
        <li>A total of {n_chart_views:,} views.</li>
        <li>An average of {int(n_daily_chart_views):,} daily views.</li>
    </ul>

    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        The charts that received most views were:
    </p>

    <ol style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: 8px;">
        {chart_list_html}
    </ol>
    """
    display(HTML(top_charts))
    # Then manually add the png of the top chart.

    ####################################################################################################################

    # Top performing articles.
    article_list_html = ""

    # Build list of clickable titles.
    article_list_html = "\n".join(
        [
            f"""<li>
            <a href="{row['url']}" target="_blank" style="color: #1155cc; text-decoration: none;">
                {row['article_title']}
            </a> – {row['views']:,} views
        </li>"""
            for _, row in df_top_articles.iterrows()
        ]
    )
    top_articles = f"""
    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        Your data is featured in {n_articles_humanized} articles at Our World in Data.
    </p>

    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        During {quarter_date_humanized}, these articles received:
    </p>

    <ul style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: -8px; margin-bottom: 16px;">
        <li>A total of {n_article_views:,} views.</li>
        <li>An average of {int(n_daily_article_views):,} daily views.</li>
    </ul>

    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        The articles that received most views were:
    </p>

    <ol style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: 8px;">
        {article_list_html}
    </ol>
    """
    display(HTML(top_articles))

    ####################################################################################################################

    # Top performing data insights.
    insight_list_html = ""

    # Build list of clickable titles.
    insight_list_html = "\n".join(
        [
            f"""<li>
            <a href="{row['url']}" target="_blank" style="color: #1155cc; text-decoration: none;">
                {row['article_title']}
            </a>
        </li>"""
            for _, row in df_top_insights.iterrows()
        ]
    )
    top_insights = f"""
    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        During {quarter_date_humanized}, the top performing data insights in terms of views are the following:
    </p>

    <ol style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: 8px;">
        {insight_list_html}
    </ol>
    """
    display(HTML(top_insights))

    ####################################################################################################################

    # Mentions in policy documents.
    policy_list_html = ""

    # Build list of clickable titles.
    policy_list_html = "\n".join(
        [
            f"""<li>
            <a href="{row['url']}" target="_blank" style="color: #1155cc; text-decoration: none;">
                {row['title']}
            </a>
        </li>"""
            for _, row in df_policy.iterrows()
        ]
    )
    policy_mentions = f"""
    <p style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000;">
        During {quarter_date_humanized}, your data has been mentioned in {len(df_policy)} policy documents:
    </p>

    <ol style="font-family: 'Lato', sans-serif; font-size: 13px; color: #000000; margin-top: 8px;">
        {policy_list_html}
    </ol>
    """
    display(HTML(policy_mentions))

    ####################################################################################################################


if __name__ == "__main__":
    run()
