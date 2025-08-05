"""Module for helper functions related to analytics (e.g. chart views).

TODO: We currently have many functions reading analytics (from MySQL and GBQ) in different places. We should gather those functions in this module. For example, Anomalist should read from this module.
"""

import json
import re
import urllib.error
import urllib.parse
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Union

import pandas as pd
import requests
from structlog import get_logger

from etl.config import (
    FORCE_DATASETTE,
    METABASE_API_KEY,
    METABASE_SEMANTIC_LAYER_DATABASE_ID,
    METABASE_URL,
    OWID_ENV,
)

# Initialize logger.
log = get_logger()

# First day when we started collecting chart render views.
DATE_MIN = "2024-11-01"
# Current date.
DATE_MAX = str(datetime.today().date())
# Base url for Datasette csv queries.
ANALYTICS_CSV_URL = "http://analytics.owid.io/analytics.csv"

# Maximum number of rows that a single Datasette csv call can return.
MAX_DATASETTE_N_ROWS = 10000

# Base OWID URL, used to find views in articles and topic pages.
OWID_BASE_URL = "https://ourworldindata.org/"

# Base URL for grapher charts.
GRAPHERS_BASE_URL = OWID_BASE_URL + "grapher/"

# Complete list of component types in the posts_gdocs_links.
# Each component type corresponds to a way in which a gdoc can be linked to another piece of content (e.g. a grapher chart, or an explorer).
COMPONENT_TYPES_ALL = [
    # Gdoc (usually of a topic page, but also possibly articles, e.g. https://ourworldindata.org/human-development-index ) cites a grapher chart as part of the all-charts block.
    # NOTE: Only charts that are specifically cited as top charts will be included.
    # To link gdocs of topic pages with charts that appear in the all-charts block, we need to use tags.
    "all-charts",
    # Gdoc (usually of an article) embeds a grapher chart.
    "chart",
    # Gdoc (only the SDG tracker) embeds grapher charts in a special story style.
    "chart-story",
    # Gdoc of homepage links to a few explorers (showing a thumbnail, no data).
    "explorer-tiles",
    # Gdoc of data insights cites the grapher-url in the metadata.
    # We can use this field to connect data insights to the original grapher chart.
    # NOTE: Not all data insights can be linked to a grapher chart (either because grapher-url is not defined, or because it uses a custom static visualization).
    "front-matter",
    # Gdoc of homepage cites other content (usually other gdocs).
    "homepage-intro",
    # Gdoc of homepage cites key indicators (usually grapher charts).
    "key-indicator",
    # Gdoc of a topic page cites key insights (usually grapher charts).
    "key-insights",
    # Gdoc (usually of an article) cites a narrative chart.
    "narrative-chart",
    # Gdoc of team page cites a person.
    "person",
    # Gdoc of homepage or author page cites topics to be shown in a "row of pills".
    "pill-row",
    # Gdoc (usually of articles) cites content (usually URLs, but also grapher charts) that appear in a special box.
    # NOTE: When it's linked to a chart, the box displays a small thumbnail of the grapher chart.
    "prominent-link",
    # Gdoc (usually topic pages) cite other content (usually articles).
    "research-and-writing",
    # Gdoc (of any kind of content) cites a URL (usually an external URL, a grapher chart, or an explorer).
    "span-link",
    # Gdoc of a topic page cites other content (usually related topics).
    "topic-page-intro",
    # Gdoc (articles about our site, or data insights) cite a video.
    "video",
]
# Component types to consider when linking gdocs with views (of charts, explorers, or narrative charts).
COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS = [
    # NOTE: 'all-charts' only includes top charts in the all-charts block. To link charts with topic pages, we need to use tags.
    # 'all-charts',
    "chart",
    "chart-story",
    # 'explorer-tiles',
    "front-matter",
    # 'homepage-intro',
    "key-indicator",
    "key-insights",
    "narrative-chart",
    # 'person',
    # 'pill-row',
    # 'prominent-link',
    # 'research-and-writing',
    # NOTE: Grapher charts are often cited as URLs. It's unclear whether we want to count these references. But I'd say that in most cases, we'd prefer to ignore these. For example, when counting views of articles that use charts, we should count articles that display the chart, but not articles that simply cite the URL of the chart. Therefore, for now, ignore 'span-link'.
    # 'span-link',
    # 'topic-page-intro',
    # 'video',
]
# Dictionary that maps the type of a gdoc post (as defined in the posts_gdocs DB table) to its base URL; adding the post's slug to this base URL gives the complete URL to the corresponding post.
POST_TYPE_TO_URL = {
    "about-page": OWID_BASE_URL,
    "article": OWID_BASE_URL,
    "author": f"{OWID_BASE_URL}team/",
    "data-insight": f"{OWID_BASE_URL}data-insights/",
    # Fragments are used just a handful of times, and seems to be used for data pages FAQ.
    # It's not clear to me how to link them to specific posts, so, ignore them for now.
    "fragment": None,
    # Gdocs of type 'homepage' have an arbitrary slug "owid-homepage". They will be manually fixed later on.
    "homepage": None,
    "linear-topic-page": OWID_BASE_URL,
    "topic-page": OWID_BASE_URL,
}
# Dictionary that maps a link type (as defined in the posts_gdocs_links DB table) to the base url; adding the target to this base url gives a full URL to the linked content (e.g. a grapher chart).
POST_LINK_TYPES_TO_URL = {
    "grapher": OWID_BASE_URL + "grapher/",
    # Narrative charts, which don't have a public URL.
    # They are handled separately.
    "narrative-charts": OWID_BASE_URL + "grapher/",
    "explorer": OWID_BASE_URL + "explorers/",
    "gdoc": "https://docs.google.com/document/d/",
    # A "url" link type links to an arbitrary URL.
    # NOTE: In a handful of cases, there are links of type "url" and component type "chart". It's not clear to me why they are not of link types "grapher" with type "chart". But what's clear is that, when the link type is "grapher" the target corresponds to a slug, and when the link type is "url" the target is a full URL to a grapher chart. So, simply map these urls to their targets without modification.
    "url": "",
}


def _try_to_execute_datasette_query(sql_url: str, warn: bool = False) -> pd.DataFrame:
    try:
        df = pd.read_csv(sql_url)
        return df
    except urllib.error.HTTPError as e:
        if e.code == 414:
            raise ValueError("HTTP 414: Query too long. Consider simplifying or batching the request.")
        else:
            raise


def clean_sql_query(sql: str) -> str:
    """
    Normalize an SQL string for use in Datasette URL queries.

    Parameters
    ----------
    sql : str
        SQL query to clean.
    Returns
    -------
    str
        Cleaned SQL query.

    """
    return " ".join(sql.strip().rstrip(";").split())


def read_datasette(
    sql: str, datasette_csv_url: str = ANALYTICS_CSV_URL, chunk_size: int = MAX_DATASETTE_N_ROWS
) -> pd.DataFrame:
    """
    Execute a query in the Datasette semantic layer.

    Parameters
    ----------
    sql : str
        SQL query to execute.
    datasette_csv_url : str
        URL of the Datasette CSV endpoint.
    chunk_size : int
        Number of rows to fetch in each chunk.
        The default is 10,000 (which is the maximum number of rows that Datasette can return in a single request).
        If the query contains a LIMIT clause, it should be smaller than chunk_size (otherwise, an error is raised).
        If the query does not contain a LIMIT clause, the query is paginated using LIMIT and OFFSET.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    """
    # Check if the query contains a LIMIT clause.
    limit_match = re.search(r"\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?\b", sql, re.IGNORECASE)

    # Clean the query.
    sql_clean = clean_sql_query(sql)

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

        # Concatenate all chunks of data.
        df = _safe_concat(dfs)

    return df


def read_metabase(sql: str, force_datasette: bool = FORCE_DATASETTE) -> pd.DataFrame:
    """Retrieve data from the Metabase API using an arbitrary sql query.

    NOTE: This function has been adapted from this example in the analytics repo:
    https://github.com/owid/analytics/blob/main/tutorials/metabase_data_download.py

    Parameters
    ----------
    sql : str
        SQL query to execute.
    force_datasette : bool, optional
        If True, use Datasette instead of Metabase. This is a fallback if Metabase API credentials are not available.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the results of the query.

    """
    # Prepare the header and body of the request to send to the Metabase API.
    headers = {
        "x-api-key": METABASE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Accept": "application/json",
    }
    body = {
        "query": {
            # Database corresponding to the Semantic Layer (DuckDB).
            "database": METABASE_SEMANTIC_LAYER_DATABASE_ID,
            "type": "native",
            "native": {"query": re.sub(r"\s+", " ", sql.strip())},
        }
    }

    # Note (copied from Bobbie in the analytics repo):
    # Despite the documentation (https://www.metabase.com/docs/latest/api#tag/apidataset/POST/api/dataset/{export-format}),
    # I cannot get the /api/dataset/csv endpoint to work when sending a dict (or json.dumps(dict)) to the POST body,
    # so I instead urlencode the body. The url encoding is a little awkward – we cannot simply use urllib.parse.urlencode(body)
    # b/c python dict single quotes need to be changed to double quotes. But we can't naively change all single quotes to
    # double quotes b/c the sql query might include single quotes (and DuckDB doesn't allow double quotes). So the line below
    # executes the url encoding without replacing any quotes within the sql query.
    urlencoded = "&".join([f"{k}={urllib.parse.quote_plus(json.dumps(v))}" for k, v in body.items()])

    ####################################################################################################################
    if force_datasette:
        log.warning(
            "Missing Metabase credentials. Add them to your .env file to avoid this warning. For now, Datasette will be used."
        )
        return read_datasette(sql=sql)
    ####################################################################################################################

    # Send request.
    response = requests.post(
        f"{METABASE_URL}/api/dataset/csv",
        headers=headers,
        data=urlencoded,
        timeout=30,
    )
    if not response.ok:
        raise RuntimeError(f"Metabase API request failed with status code {response.status_code}: {response.text}")

    # Create a dataframe with the returned data.
    df = pd.read_csv(BytesIO(response.content))

    return df


def get_number_of_days(
    date_min: str,
    date_max: str,
    table_name: str,
    day_column_name: str = "day",
    published_at: Optional[pd.Series] = None,
) -> Union[pd.Series, int]:
    """
    Calculate the number of days for which the views are counted.

    This will be the range of dates between date_start and date_end (both included), where:
    * date_start is the maximum between publication date (if given), the start date of analytics data (DATE_MIN), and the given date_min.
    * date_end is the minimum between the latest date informed in the relevant table (which this function finds out with a query) and the given date_max.
    If date_start is after date_end, the number of days is set to zero.

    The result is a Series with the number of days for each chart (if publication date is given), or a single integer (if no publication date is given).

    Parameters
    ----------
    published_at : pd.Series, optional
        Series of publication dates (if given).
    date_min : str
        Minimum date to consider.
    date_max : str
        Maximum date to consider.
    table_name : str
        Name of the DB table to query for the maximum informed date.
    day_column_name : str
        Name of the column in the DB table that contains the date.

    Returns
    -------
    pd.Series or int
        Number of days for which the views are counted.

    """
    if published_at is None:
        # If no publication date is given, we need to find the maximum between the minimum date and the start date of analytics data.
        date_start = max(pd.to_datetime(date_min), pd.to_datetime(DATE_MIN))
    else:
        # If a publication date is given, we need to find the maximum between the publication date, the minimum date, and the start date of analytics data.
        date_start = (
            pd.to_datetime(published_at).clip(lower=pd.to_datetime(date_min)).clip(lower=pd.to_datetime(DATE_MIN))
        )

    # There is always a lag in analytics, so we need to find out the maximum date informed in the analytics data.
    query = f"SELECT MAX({day_column_name}) AS date_max FROM {table_name}"
    date_max_informed = read_metabase(sql=query)["date_max"].item()
    date_end = min(pd.to_datetime(date_max_informed), pd.to_datetime(date_max))

    if isinstance(date_start, pd.Series) or isinstance(date_end, pd.Series):
        # Add a column with the number of days that the views are referring to.
        # Add 1 to include the end date in the count.
        n_days = (date_end - date_start).dt.days + 1
        # Set to 0 for cases where date_start > date_end.
        n_days = n_days.where(date_end >= date_start, 0)
    else:
        # If date_start and date_end are not series, simply calculate the number of days.
        # Add 1 to include the end date in the count.
        n_days = max(0, (date_end - date_start).days + 1)

    return n_days


def get_chart_views_per_day_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch chart view events from Metabase, optionally filtered by chart IDs and minimum date.

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    date_min : str, optional
        Minimum date to filter the results. If None, no minimum date is applied.
    date_max : str, optional
        Maximum date to filter the results. If None, no maximum date is applied.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the chart view events.

    """
    where_clauses = []
    if date_min:
        where_clauses.append(f"v.day > '{date_min}'")
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
        v.day,
        SUM(v.events) AS events
    FROM charts c
    JOIN grapher_views_detailed v ON c.url = v.grapher
    {where_sql}
    GROUP BY c.chart_id, c.url, v.day
    ORDER BY c.chart_id, v.day ASC;
    """
    df = read_metabase(sql=query)

    return df


def get_chart_views_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of chart views (renders) per chart from Metabase, optionally filtered by chart IDs and date range.

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    date_min : str, optional
        Minimum date to filter the results. If None, no minimum date is applied.
    date_max : str, optional
        Maximum date to filter the results. If None, no maximum date is applied.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the number of chart views per chart.

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
    df_views = read_metabase(sql=query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    df_views["n_days"] = get_number_of_days(
        published_at=df_views["published_at"],
        date_min=date_min,
        date_max=date_max,
        table_name="grapher_views_detailed",
        day_column_name="day",
    )

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

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    n_days : int
        Number of days to look back for views. Default is 30 days.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the number of chart views per chart for the last n_days.

    """
    # Calculate date range.
    date_max = str(datetime.today().date())
    date_min = str((datetime.today() - pd.Timedelta(days=n_days)).date())

    # Get views.
    df_views = get_chart_views_by_chart_id(chart_ids=chart_ids, date_min=date_min, date_max=date_max)

    return df_views


def get_post_views_by_url(
    urls: Optional[List[str]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of posts views (including articles, topic pages, and data insights) for a list of URLs from Metabase.

    URLs corresponding to grapher charts and explorers are excluded from the results.

    Parameters
    ----------
    urls : list of str, optional
        List of URLs to filter the results. If None, all URLs are included.
    date_min : str, optional
        Minimum date to filter the results. If None, no minimum date is applied.
    date_max : str, optional
        Maximum date to filter the results. If None, no maximum date is applied.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the number of GDoc views per URL.

    """
    # Impose a specific list of post views, that excludes grapher charts and explorers.
    # NOTE: For some reason, the types defined in the metabase pages table do not coincide with the ones in post_gdocs.
    post_types = [
        # Articles.
        "article",
        # Author pages.
        "author",
        # Exclude type 'chart', which is used for grapher charts.
        # 'chart',
        # Country pages.
        "country",
        # Data insights.
        "data-insight",
        # Exclude type 'explorer', which is used for data explorers.
        # 'explorer',
        # There is only one page with this type.
        "teaching",
        # Topic pages.
        "topic-page",
        # The type 'util' is used for a variety of things, including FAQs and latest.
        # It also includes the homepage, so we need to keep it.
        "util",
    ]
    post_types_str = ", ".join(f"'{post_type}'" for post_type in post_types)
    # Prepare query.
    query = f"""
    SELECT
        url,
        SUM(views) AS views
    FROM views_detailed
    JOIN pages USING(url)
    WHERE day >= '{date_min}'
    AND day <= '{date_max}'
    """
    if urls:
        url_list = ", ".join(f"'{url}'" for url in urls)
        query += f" AND url IN ({url_list})"
    query += f"""
    AND type in ({post_types_str})
    AND url IS NOT NULL
    GROUP BY url
    ORDER BY views DESC
    """
    df_views = read_metabase(sql=query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    df_views["n_days"] = get_number_of_days(
        date_min=date_min,
        date_max=date_max,
        table_name="views_detailed",
        day_column_name="day",
    )

    # Add a column for the average number of daily views.
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views


def _get_post_references_of_charts_and_redirected_charts(
    chart_ids: Optional[List[int]] = None, component_types: Optional[List[str]] = None
) -> pd.DataFrame:
    # Prepare list of component types to consider.
    if component_types is None:
        # If not specified, assume a specific list (defined above).
        component_types = COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS
    component_types_str = ", ".join(f"'{chart_id}'" for chart_id in component_types)

    # Prepare query.
    # The following query is a bit complex, but it can be explained as follows:
    # * First, we define a temporary table "redirect_targets", with the content of ther chart_slug_redirects DB table. For convenience, we call the chart_id column "redirected_chart_id".
    # * We then define the "main_query", which is the union of two subqueries:
    #   * A query that searches for citations of existing chart slugs in gdoc posts.
    #   * A query that searches for citations of redirected chart slugs in gdoc posts.
    # * Finally, we get only the distinct rows from the main query (which may not be strictly necessary).
    query = f"""
    WITH redirect_targets AS (
        SELECT
            cr.chart_id AS redirected_chart_id,
            cr.slug AS chart_slug
        FROM chart_slug_redirects cr
    ),
    main_query AS (
        SELECT DISTINCT
            c.id AS chart_id,
            pg.content ->> '$.title' AS post_title,
            pg.slug AS post_slug,
            pg.type AS post_type,
            cc.slug AS chart_slug,
            pgl.linkType AS link_type,
            pgl.componentType AS component_type,
            pg.publishedAt AS post_publication_date
        FROM
            posts_gdocs pg
            JOIN posts_gdocs_links pgl ON pg.id = pgl.sourceId
            JOIN chart_configs cc ON pgl.target = cc.slug
            JOIN charts c ON c.configId = cc.id
        WHERE
            pgl.componentType IN ({component_types_str})
            AND pg.published = 1
        UNION
        SELECT DISTINCT
            rt.redirected_chart_id AS chart_id,
            pg.content ->> '$.title' AS post_title,
            pg.slug AS post_slug,
            pg.type AS post_type,
            rt.chart_slug AS chart_slug,
            pgl.linkType AS link_type,
            pgl.componentType AS component_type,
            pg.publishedAt AS post_publication_date
        FROM
            posts_gdocs pg
            JOIN posts_gdocs_links pgl ON pg.id = pgl.sourceId
            JOIN redirect_targets rt ON pgl.target = rt.chart_slug
        WHERE
            pgl.componentType IN ({component_types_str})
            AND pg.published = 1
    )
    SELECT DISTINCT *
    FROM main_query
    """

    # Specify chart ids to consider (otherwise all charts will be considered).
    if chart_ids is not None:
        chart_ids_str = ", ".join(f"{chart_id}" for chart_id in chart_ids)
        query += f"""
    WHERE chart_id IN ({chart_ids_str})
    """

    # Sort query results conveniently.
    query += """
    ORDER BY chart_id ASC
    """

    # Execute query and create a dataframe.
    df = OWID_ENV.read_sql(sql=query)

    return df


def _get_post_references_of_charts_via_narrative_charts(chart_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """Get posts (including articles, topic pages, and data insights) that use narrative chart, and link them to the original (parent) chart."""
    # Prepare query.
    query = """SELECT
        c.id AS chart_id,
        pg.content ->> '$.title' AS post_title,
        pg.slug AS post_slug,
        pg.type AS post_type,
        cc.slug AS chart_slug,
        pgl.linkType AS link_type,
        pgl.componentType AS component_type,
        pg.publishedAt AS post_publication_date,
        nc.id AS narrative_chart_id,
        pgl.target AS narrative_chart_slug
    FROM posts_gdocs pg
    JOIN posts_gdocs_links pgl ON pg.id = pgl.sourceId
    LEFT JOIN narrative_charts nc ON pgl.target = nc.name
    LEFT JOIN charts c ON nc.parentChartId = c.id
    LEFT JOIN chart_configs cc ON c.configId = cc.id
    WHERE pg.published = 1
    AND nc.id IS NOT NULL
    """
    if chart_ids:
        chart_ids_str = ", ".join(str(cid) for cid in chart_ids)
        query += f" AND c.id IN ({chart_ids_str})"

    # Execute query and create a dataframe.
    df = OWID_ENV.read_sql(query)

    return df


def get_topic_tags_for_chart_ids(
    chart_ids: Optional[List[int]] = None, only_topics_with_all_charts_block: bool = False
) -> pd.DataFrame:
    """Get topic tags, and their corresponding posts (usually topic pages), for a list of chart ids.

    Optionally (if only_topics_with_all_charts_block), return only those whose corresponding page (usually a topic page, but it can also be an article) contains an all-charts block. This allows us to find all pages that display charts as part of the all-charts block.

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    only_topics_with_all_charts_block : bool, optional
        If True, return only those topics whose corresponding posts (usually topic pages) contain an all-charts block.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the topic tags for the given chart IDs.

    """
    # Prepare query.
    # NOTE: It seems that topic_slug is always identical to post_slug (which is not the case for title), however, just in case, keep them separately.
    query = """SELECT
        c.id AS chart_id,
        cc.slug AS chart_slug,
        t.id AS topic_id,
        t.name AS topic_name,
        t.slug AS topic_slug,
        pg.type AS post_type,
        pg.content ->> '$.title' AS post_title,
        pg.slug AS post_slug,
        pg.publishedAt AS post_publication_date,
        ct.keyChartLevel AS key_chart_level
    FROM chart_tags ct
    JOIN charts c ON ct.chartId = c.id
    JOIN chart_configs cc ON c.configId = cc.id
    JOIN tags t ON ct.tagId = t.id
    JOIN posts_gdocs pg ON pg.slug = t.slug
    WHERE t.slug IS NOT NULL
    AND ct.keyChartLevel > 0
    """
    if only_topics_with_all_charts_block:
        # Get only those pages that contain an all-charts block.
        query += r""" AND pg.content LIKE '%%\"all-charts\"%%'"""
    if chart_ids:
        # Optionally reduce the query to a list of chart ids.
        chart_ids_str = ", ".join(str(cid) for cid in chart_ids)
        query += f" AND c.id IN ({chart_ids_str})"

    # Execute query and construct a dataframe.
    df = OWID_ENV.read_sql(query)

    return df


def _safe_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate DataFrames, ignoring empty ones."""
    # Filter out empty DataFrames.
    dfs_to_concat = [df_ for df_ in dfs if not df_.empty]
    columns = list(dict.fromkeys(col for df_ in dfs for col in df_.columns))

    # Concatenate only if there are non-empty DataFrames
    if dfs_to_concat:
        df = pd.concat(dfs_to_concat, ignore_index=True)
    else:
        df = pd.DataFrame(columns=columns)

    return df


def get_post_references_of_charts(
    chart_ids: Optional[List[int]] = None,
    component_types: Optional[List[str]] = None,
    include_parents_of_narrative_charts: bool = True,
    include_references_of_all_charts_block: bool = True,
) -> pd.DataFrame:
    """Get posts (including articles, topic pages, and data insights) that use charts, given a list of chart ids.

    A chart may be used by a gdoc in different ways: it can be embedded, cited as a URL, etc. The argument component_types defines which ways to consider (e.g. 'chart' corresponds to embedded charts).

    The main query used in this function was adapted from owid-grapher/db/model/Post.ts (getGdocsPostReferencesByChartId). That is the query that determines the articles and topic pages that reference a given chart id. The resulting list is what appears in the Refs tab of the chart admin.
    However, that query had some limitations (see owid-grapher issue https://github.com/owid/owid-grapher/issues/4859).

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    component_types : list of str, optional
        List of component types to filter the results. If None, all component types are included.
        The complete list of component types is defined in the COMPONENT_TYPES_ALL variable.
    include_parents_of_narrative_charts : bool, optional
        If True, include references to narrative charts whose parents are charts among those in chart IDs.
    include_references_of_all_charts_block : bool, optional
        If True, include references to charts in the all-charts block of topic pages.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the URLs of posts that display the given chart IDs.

    """
    # Find all gdocs that cite chart slugs, including old (redirected) chart slugs.
    df = _get_post_references_of_charts_and_redirected_charts(chart_ids=chart_ids, component_types=component_types)

    if include_parents_of_narrative_charts:
        # If a gdoc uses a narrative chart, we want to identify the parent chart, and, if that parent chart is among the given chart_ids, include those gdocs.
        df_narrative_charts = _get_post_references_of_charts_via_narrative_charts(chart_ids=chart_ids)
        df = _safe_concat(dfs=[df, df_narrative_charts])

    if include_references_of_all_charts_block:
        df_all_charts_block = get_topic_tags_for_chart_ids(
            chart_ids=chart_ids, only_topics_with_all_charts_block=True
        ).drop(columns=["key_chart_level", "topic_id", "topic_name", "topic_slug"])
        # Add component_type and lint_type, for consistency.
        df_all_charts_block["component_type"] = "all-charts"
        df_all_charts_block["link_type"] = "grapher"
        df = _safe_concat(dfs=[df, df_all_charts_block])

    # Transform slugs of the gdoc posts (articles, topic pages, and data insights) into full urls.
    df["post_url"] = df["post_type"].map(POST_TYPE_TO_URL) + df["post_slug"]
    # In the case of gdocs of type "homepage", the post_slug seems to always be "owid-homepage", which is not a real slug. Fix those cases.
    # NOTE: Ensure the homepage URL does not have a trailing slash (otherwise it will not be found in Metabase).
    df.loc[df["post_type"] == "homepage", "post_url"] = OWID_BASE_URL.rstrip("/")

    # Transform slugs of the target content (usually grapher charts or explorers) into urls.
    df["chart_url"] = df["link_type"].map(POST_LINK_TYPES_TO_URL) + df["chart_slug"]

    # Adapt publication date format.
    df["post_publication_date"] = pd.to_datetime(df["post_publication_date"]).dt.date.astype(str)

    # Delete rows without a valid post url.
    # This may happen to fragments, since didn't know how to map them into a url.
    df = df.dropna(subset=["post_url"]).reset_index(drop=True)

    return df


def get_post_views_by_chart_id(
    chart_ids: Optional[List[int]] = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
    include_parents_of_narrative_charts: bool = True,
    include_references_of_all_charts_block: bool = True,
):
    """Given a list of chart ids, get all URLs of posts (including articles, topic pages, and data insights) that display that chart, and their views.

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    date_min : str, optional
        Minimum date to filter the results. If None, no minimum date is applied.
    date_max : str, optional
        Maximum date to filter the results. If None, no maximum date is applied.
    include_parents_of_narrative_charts : bool, optional
        If True, include references to narrative charts whose parents are charts among those in chart IDs.
    include_references_of_all_charts_block : bool, optional
        If True, include references to charts in the all-charts block of topic pages.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the URLs of posts that display the given chart IDs, along with their views.

    """
    # Get a dataframe connecting chart ids with post urls that refer to those charts.
    df_content = get_post_references_of_charts(
        chart_ids=chart_ids,
        include_parents_of_narrative_charts=include_parents_of_narrative_charts,
        include_references_of_all_charts_block=include_references_of_all_charts_block,
    )

    # Gather analytics for the gdocs, e.g. number of views in articles and topic pages.
    df_article_views = get_post_views_by_url(
        urls=list(set(df_content["post_url"])), date_min=date_min, date_max=date_max
    )

    # Combine data.
    df_views = df_content[
        ["post_url", "post_title", "post_type", "post_publication_date", "chart_id", "chart_url"]
    ].merge(df_article_views.rename(columns={"url": "post_url"}), on="post_url", how="left")

    # TODO: Find out why some urls don't have views, e.g. 'https://ourworldindata.org/neurodevelopmental-disorders' (which is now called 'https://ourworldindata.org/mental-health'). Maybe we should account for posts redirects (in the same way we do for charts).
    # For now, remove rows with no data for views.
    df_views = df_views.dropna(subset=["views"]).reset_index(drop=True)
    df_views = df_views.astype({"views": int, "n_days": int})

    return df_views


def get_post_views_last_n_days(
    chart_ids: Optional[List[int]] = None,
    n_days: int = 30,
    include_parents_of_narrative_charts: bool = True,
    include_references_of_all_charts_block: bool = True,
) -> pd.DataFrame:
    """
    Fetch number of post views (including articles, topic pages, and data insights) for the last n_days.

    NOTE: Given that there is a lag in analytics, the number of days considered will be smaller than n_days. For this reason, the returned dataframe will contain a column "n_days" with the number of days for which the views are counted.

    Parameters
    ----------
    chart_ids : list of int, optional
        List of chart IDs to filter the results. If None, all charts are included.
    n_days : int
        Number of days to look back for views. Default is 30 days.
    include_parents_of_narrative_charts : bool, optional
        If True, include references to narrative charts whose parents are charts among those in chart IDs.
    include_references_of_all_charts_block : bool, optional
        If True, include references to charts in the all-charts block of topic pages.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the number of post views for the last n_days.

    """
    # Calculate date range.
    date_max = str(datetime.today().date())
    date_min = str((datetime.today() - pd.Timedelta(days=n_days)).date())

    # Get views.
    df_views = get_post_views_by_chart_id(
        chart_ids=chart_ids,
        date_min=date_min,
        date_max=date_max,
        include_parents_of_narrative_charts=include_parents_of_narrative_charts,
        include_references_of_all_charts_block=include_references_of_all_charts_block,
    )

    return df_views


def get_visualizations_using_data_by_producer(
    producers: Optional[List[str]] = None, excluded_steps: Optional[List[str]] = None
) -> pd.DataFrame:
    """Get all OWID visualizations (charts and collection views) using data from a given list of producers.

    Parameters
    ----------
    producers : list of str, optional
        Producer names to include. If None or empty, all producers are included.
    excluded_steps : list, optional
        Step patterns to exclude from the results. If empty, no filtering will be applied.
        Example: ['demography/.*/population', 'energy/.*/primary_energy_consumption', 'ggdc/.*/maddison_project_database', 'wb/.*/income_groups']

    TODO: Generalize to account for collection views, not only charts.
    """
    # Initialize empty lists if None is provided
    if excluded_steps is None:
        excluded_steps = []

    # Construct the base SQL query
    query = f"""WITH t_base AS (
	SELECT
		cd.chartId chart_id,
		JSON_UNQUOTE(JSON_EXTRACT(cc.full, '$.title')) chart_title,
		cc.slug chart_slug,
		CONCAT('{GRAPHERS_BASE_URL}', cc.slug) chart_url,
		JSON_EXTRACT(cc.full, '$.isPublished') is_published,
		cd.variableId variable_id,
		v.name variable_name,
		d.id dataset_id,
		d.name dataset_name,
		d.catalogPath dataset_uri,
		ov.originId origin_id,
		o.title origin_name,
        o.urlMain origin_url,
		o.producer producer
	FROM chart_dimensions cd
	LEFT JOIN charts c ON c.id = cd.chartId
	LEFT JOIN chart_configs cc ON cc.id = c.configId
	LEFT JOIN origins_variables ov ON ov.variableId = cd.variableId
	LEFT JOIN origins o ON o.id = ov.originId
	LEFT JOIN variables v ON cd.variableId = v.id
	LEFT JOIN datasets d ON d.id = v.datasetId
)
SELECT * FROM t_base
WHERE origin_id IS NOT NULL
AND is_published = true"""

    # Select producers, if specified.
    if producers and len(producers) > 0:
        producers_str = ", ".join(f"'{p}'" for p in [p.replace("'", "''") for p in producers])
        query += f"\nAND producer IN ({producers_str})"

    # Execute the query.
    df = OWID_ENV.read_sql(query)

    # Exclude certain steps using regex.
    # NOTE: This approach is safer and more accurate than excluding those steps directly in the query.
    if excluded_steps:
        exclude_steps_regex = "|".join(excluded_steps)
        excluded_rows = df["dataset_uri"].str.fullmatch(exclude_steps_regex)
        df = df.loc[~excluded_rows]

    # Handle special case of "Various sources".
    mask_various = df["producer"] == "Various sources"
    df.loc[mask_various, "producer"] = (
        df.loc[mask_various, "producer"] + " (" + df.loc[mask_various, "origin_url"] + ")"
    )

    return df
