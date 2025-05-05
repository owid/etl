"""Module for helper functions related to analytics (e.g. chart views).

TODO: We currently have many functions reading analytics (from MySQL and GBQ) in different places. We should gather those functions in this module.
"""

import re
import urllib.error
import urllib.parse
from datetime import datetime
from typing import List, Optional

import pandas as pd
from structlog import get_logger

from etl.config import OWID_ENV

# Initialize logger.
log = get_logger()

# First day when we started collecting chart render views.
# TODO: Find out this exact date.
DATE_MIN = "2025-01-01"
# Current date.
DATE_MAX = str(datetime.today().date())
# Base url for Datasette csv queries.
ANALYTICS_CSV_URL = "http://analytics.owid.io/analytics.csv"

# Maximum number of rows that a single Datasette csv call can return.
MAX_DATASETTE_N_ROWS = 10000


def _try_to_execute_datasette_query(sql_url: str, warn: bool = False) -> pd.DataFrame:
    try:
        df = pd.read_csv(sql_url)
        return df
    except urllib.error.HTTPError as e:
        if e.code == 414:
            raise ValueError("HTTP 414: Query too long. Consider simplifying or batching the request.")
        else:
            raise


def clean_sql(sql: str) -> str:
    """
    Normalize an SQL string for use in Datasette URL queries.
    """
    return " ".join(sql.strip().rstrip(";").split())


def read_datasette(
    sql: str, datasette_csv_url: str = ANALYTICS_CSV_URL, chunk_size: int = MAX_DATASETTE_N_ROWS
) -> pd.DataFrame:
    """
    Execute a query in the Datasette semantic layer.
    """
    # Check if the query contains a LIMIT clause.
    limit_match = re.search(r"\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\b", sql, re.IGNORECASE)

    # Clean the query.
    sql_clean = clean_sql(sql)

    if limit_match:
        # If a LIMIT clause already exists, check if it's larger than the limit.
        limit_value = int(limit_match.group(1))
        if limit_value > MAX_DATASETTE_N_ROWS:
            raise ValueError(
                f"Query LIMIT ({limit_value}) exceeds Datasette's maximum row limit ({MAX_DATASETTE_N_ROWS}). Either use a lower value for the limit, or set no limit (and pagination will be used)."
            )
        else:
            # Given that there is a LIMIT clause, and the value is small, execute the query as-is.
            full_url = f"{datasette_csv_url}?" + urllib.parse.urlencode({"sql": sql_clean, "_size": "max"})
            # Fetch data as a dataframe, or raise an error (e.g. if query is too long).
            df = _try_to_execute_datasette_query(sql_url=full_url, warn=True)
    else:
        # If there is no LIMIT clause, paginate using LIMIT/OFFSET.
        offset = 0
        dfs = []
        while True:
            # Prepare query for this chunk.
            full_url = f"{datasette_csv_url}?" + urllib.parse.urlencode(
                {"sql": f"{sql_clean} LIMIT {chunk_size} OFFSET {offset}", "_size": "max"}
            )
            # Fetch data for current chunk.
            df_chunk = _try_to_execute_datasette_query(sql_url=full_url)
            if len(df_chunk) == chunk_size:
                # Add data for current chunk to the list.
                dfs.append(df_chunk)
                # Update offset.
                offset += chunk_size
            else:
                # If fewer rows than the maximum (or even zero rows) are fetched, this must be the last chunk.
                dfs.append(df_chunk)
                break

        if len(dfs) == 0:
            # If no data was fetched, return an empty dataframe.
            df = pd.DataFrame()
        else:
            # Concatenate all chunks of data.
            df: pd.DataFrame = pd.concat(dfs, ignore_index=True)  # type: ignore

    return df


def get_chart_events_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    min_date: str = DATE_MIN,
    max_date: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch chart view events from Datasette, optionally filtered by chart IDs and minimum date.

    TODO: This function may not be necessary. Consider deleting.
    """
    where_clauses = []
    if min_date:
        where_clauses.append(f"v.day > '{min_date}'")
    if max_date:
        where_clauses.append(f"v.day <= '{max_date}'")
    if chart_ids:
        id_list = ", ".join(str(cid) for cid in chart_ids)
        where_clauses.append(f"c.chart_id IN ({id_list})")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        c.chart_id,
        c.url,
        c.published_at,
        v.day,
        v.events
    FROM charts c
    JOIN grapher_views_detailed v ON c.url = v.grapher
    {where_sql}
    ORDER BY v.events DESC;
    """
    df = read_datasette(query)

    return df


def get_chart_views_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of chart views (renders) per chart from Datasette, optionally filtered by chart IDs and date range.

    """
    where_clauses = []
    if date_min:
        where_clauses.append(f"v.day >= '{date_min}'")
    if date_max:
        where_clauses.append(f"v.day <= '{date_max}'")
    if chart_ids:
        id_list = ", ".join(str(cid) for cid in chart_ids)
        where_clauses.append(f"c.chart_id IN ({id_list})")

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        c.chart_id,
        c.url,
        c.published_at,
        SUM(v.events) AS views
    FROM charts c
    JOIN grapher_views_detailed v ON c.url = v.grapher
    {where_sql}
    GROUP BY c.chart_id, c.url, c.published_at
    ORDER BY views DESC
    """

    df_views = read_datasette(query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    # This will be either the first date since we have analytics (DATE_MIN) or the publication date of the chart (if more recent than that).
    published_at = pd.to_datetime(df_views["published_at"])
    start_date = published_at.where(published_at > pd.to_datetime(date_min), pd.to_datetime(date_min))

    # Add a column with the number of days that the views are referring to.
    df_views["n_days"] = (pd.Timestamp.today().normalize() - start_date).dt.days

    # Add a column for the average number of daily views.
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views


def get_chart_views_last_n_days(
    chart_ids: Optional[List[int]] = None,
    n_days: int = 30,
) -> pd.DataFrame:
    """
    Fetch number of chart views per chart for the last n_days.

    """
    # Calculate date range.
    date_max = str(datetime.today().date())
    date_min = str((datetime.today() - pd.Timedelta(days=n_days)).date())

    # Get views.
    df_views = get_chart_views_by_chart_id(chart_ids=chart_ids, date_min=date_min, date_max=date_max)

    return df_views


def get_article_views_by_url(
    urls: Optional[List[str]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of article views per URL from Datasette.
    """
    where_clauses = []
    if date_min:
        where_clauses.append(f"day >= '{date_min}'")
    if date_max:
        where_clauses.append(f"day <= '{date_max}'")
    if urls:
        url_list = ", ".join(f"'{url}'" for url in urls)
        where_clauses.append(f"url IN ({url_list})")
    # Exclude pages corresponding to grapher charts and explorers.
    where_clauses.append("url NOT LIKE '%/grapher/%'")
    where_clauses.append("url NOT LIKE '%/explorers/%'")
    # Exclude url with spaces or empty.
    where_clauses.append("url NOT LIKE '% %'")
    where_clauses.append("url IS NOT NULL")
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
    SELECT
        url,
        SUM(views) AS views
    FROM views_detailed
    {where_sql}
    GROUP BY url
    ORDER BY views DESC
    """
    df_views = read_datasette(query)

    n_days = (pd.to_datetime(date_max) - pd.to_datetime(date_min)).days
    df_views["n_days"] = n_days
    df_views["views_daily"] = df_views["views"] / n_days

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views


def get_article_views_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
):
    """Given a list of chart ids, get all article URLs (and their views) that display that chart.

    TODO: I suppose the logic of this function can be simplified, it's quite convoluted.

    """
    # Firstly, I will connect all gdocs with grapher charts.
    # TODO: Fix the following queries to account for redirected slugs.
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
        cc.slug AS chart_slug,
        CAST(JSON_UNQUOTE(JSON_EXTRACT(cc.full, '$.id')) AS UNSIGNED) AS chart_id
    FROM
        posts_gdocs pg
    JOIN
        posts_gdocs_links pgl ON pg.id = pgl.sourceId
    JOIN
        chart_configs cc ON cc.slug = pgl.target
    WHERE
        pg.published = 1
        AND JSON_EXTRACT(cc.full, '$.id') IS NOT NULL"""
    if chart_ids:
        id_list = ", ".join(str(cid) for cid in chart_ids)
        query += f" AND JSON_EXTRACT(cc.full, '$.id') IN ({id_list})"
    df_chart_links = OWID_ENV.read_sql(query)

    # When a gdoc cites a narrative chart, we want to link it to its parent chart. To do that, we need a different query.
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
        cc.slug AS chart_slug,
        cv.id AS chart_id
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
        pg.published = 1
    AND cv.id IS NOT NULL
    """
    if chart_ids:
        id_list = ", ".join(str(cid) for cid in chart_ids)
        query += f" AND cv.id IN ({id_list})"
    df_narrative_chart_links = OWID_ENV.read_sql(query)

    # Combine both tables.
    if not df_narrative_chart_links.empty:
        df_links = pd.concat([df_chart_links, df_narrative_chart_links])
    else:
        df_links = df_chart_links

    # Construct URLs for all the different contents.
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
        "chart-view": OWID_BASE_URL + "grapher/",
        # "explorer": OWID_BASE_URL + "explorers/",
        # 'gdoc',
        # "url": "",
    }
    # Transform slugs or articles, topic pages, and data insights into urls.
    df_links["content_url"] = df_links["type"].map(url_start) + df_links["post_slug"]
    # In the case of homepage references, post_slug is "owid-homepage", which is not a real slug.
    # The url in that case should just be the homepage.
    df_links.loc[df_links["type"] == "homepage", "content_url"] = OWID_BASE_URL
    # Transform slugs of grapher charts into urls.
    # If there is a parent chart id, use that, otherwise, use the target chart.
    df_links["chart_url"] = df_links["linkType"].map(url_start) + df_links["chart_slug"].fillna(df_links["target"])

    # Remove rows without content or charts.
    df_links = df_links.dropna(subset=["content_url", "chart_url"], how="any").reset_index(drop=True)

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
        # This seems to refer to cited links, so ignore them.
        # 'span-link',
        # This refers to videos (which are just a few).
        # 'video',
        # This is used for the grapher-url defined in the metadata of data insights.
        # We will use this field to connect data insights to the original grapher chart.
        # NOTE: Data insights using a static chart (that didn't come from any grapher chart) will not be considered.
        "front-matter",
        # This is used for links that appear in a special box.
        # When it's linked to a chart, it shows a very small thumbnail, so we can exclude it.
        # 'prominent-link',
        # Content shown (as a thumbnail) in the Research & Writing tab of topic pages.
        # Given that it simply shows a dummy thumbnail, exclude it.
        # 'research-and-writing',
        # This seems to be just links that appear as "RELATED TOPICS" section of topic pages.
        # They don't show any data, so exclude them.
        # 'topic-page-intro',
    ]
    # By eye, I can tell that 'span-link' refers to links in the text (sometimes those links are grapher charts), and 'chart' or 'chart-view' refer to embedded grapher charts.
    # As an example, see
    # df_links[(df_links["content_url"]=="https://ourworldindata.org/what-is-foreign-aid")]
    # which has both grapher charts cited and embedded.
    # Find all articles, topic pages and data insights that display grapher charts.
    df_content = (
        df_links[(df_links["componentType"].isin(ALLOWED_COMPONENT_TYPES))][
            ["type", "content_url", "publishedAt", "title", "chart_id", "chart_url"]
        ]
        .rename(columns={"publishedAt": "publication_date"})
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df_content["publication_date"] = df_content["publication_date"].dt.date.astype(str)

    # TODO: Ideally, we would get analytics only for the relevant article urls, but the query is too long for Datasette and fails. Generalize this, either by reading from metabase API or from Duck DB.
    # Gather analytics for the writtent content in this dataframe, i.e. number of views in articles, topic pages (and possibly DIs).
    try:
        df_article_views = get_article_views_by_url(
            urls=list(set(df_content["content_url"])), date_min=date_min, date_max=date_max
        )
    except urllib.error.HTTPError:
        df_article_views = get_article_views_by_url(urls=None, date_min=date_min, date_max=date_max)

    # Combine data.
    df_views = (
        df_content[["content_url", "title", "chart_id", "chart_url"]]
        .rename(columns={"content_url": "url"})
        .merge(df_article_views, on="url", how="left")
    )

    return df_views


def get_article_views_last_n_days(
    chart_ids: Optional[List[int]] = None,
    n_days: int = 30,
) -> pd.DataFrame:
    """
    Fetch number of article views per chart for the last n_days.

    """
    # Calculate date range.
    date_max = str(datetime.today().date())
    date_min = str((datetime.today() - pd.Timedelta(days=n_days)).date())

    # Get views.
    df_views = get_article_views_by_chart_id(chart_ids=chart_ids, date_min=date_min, date_max=date_max)

    return df_views
