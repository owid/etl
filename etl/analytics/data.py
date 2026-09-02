"""Functions to get analytics data, including specific answers."""

from datetime import datetime

import pandas as pd

from etl.analytics.config import (
    COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS,
    DATE_MAX,
    DATE_MIN,
    GA_SCHEMA,
    GRAPHERS_BASE_URL,
    OWID_BASE_URL,
    POST_LINK_TYPES_TO_URL,
    POST_TYPE_TO_URL,
    SEMANTIC_LAYER_SCHEMA,
)
from etl.analytics.datasette import read_datasette
from etl.analytics.metabase import read_semantic_layer
from etl.analytics.utils import _safe_concat, log
from etl.config import FORCE_DATASETTE, OWID_ENV


def read_analytics(sql: str, force_datasette: bool = FORCE_DATASETTE):
    """Retrieve data from the Metabase API using an arbitrary sql query. If Metabase credentials are not available, use Datasette as a fallback.

    Parameters
    ----------
    sql : str
        SQL query to execute.
    force_datasette : bool, optional
        If True, use Datasette instead of Metabase. This is a fallback if Metabase API credentials are not available.
    """
    if force_datasette:
        log.warning(
            "Missing Metabase credentials. Add them to your .env file to avoid this warning. For now, Datasette will be used."
        )
        return read_datasette(sql=sql)
    return read_semantic_layer(sql=sql)


def get_number_of_days(
    date_min: str,
    date_max: str,
    table_name: str,
    day_column_name: str = "day",
    published_at: pd.Series | None = None,
) -> pd.Series | int:
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
        Name of the DB table to query for the maximum informed date, qualified with its schema
        (BigQuery dataset), e.g. "prod_ga4.grapher_views_detailed". The tables involved no longer all
        live in the same dataset, so the caller says which one.
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
    date_max_informed = read_analytics(sql=query)["date_max"].item()
    date_end = min(pd.to_datetime(date_max_informed), pd.to_datetime(date_max))

    if isinstance(date_start, pd.Series) or isinstance(date_end, pd.Series):
        # Add a column with the number of days that the views are referring to.
        # Add 1 to include the end date in the count.
        n_days = (date_end - date_start).dt.days + 1  # ty: ignore[unresolved-attribute]
        # Set to 0 for cases where date_start > date_end.
        n_days = n_days.where(date_end >= date_start, 0)
    else:
        # If date_start and date_end are not series, simply calculate the number of days.
        # Add 1 to include the end date in the count.
        n_days = max(0, (date_end - date_start).days + 1)

    return n_days


def get_chart_views_per_day_by_chart_id(
    chart_ids: list[int] | None = None,
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

    # Join on the resolved `chart_id`, not on the URL as GA4 recorded it: that URL is truncated at 126
    # characters and may be a slug the chart has since been renamed away from, so matching it against
    # the chart's current URL loses views (owid/analytics#733).
    query = f"""
    SELECT
        c.chart_id,
        c.url,
        v.day,
        SUM(v.events) AS events
    FROM {SEMANTIC_LAYER_SCHEMA}.charts c
    JOIN {GA_SCHEMA}.grapher_views_detailed v ON v.chart_id = c.chart_id
    {where_sql}
    GROUP BY c.chart_id, c.url, v.day
    ORDER BY c.chart_id, v.day ASC;
    """
    df = read_analytics(sql=query)

    return df


def get_chart_views_by_chart_id(
    chart_ids: list[int] | None = None,
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

    # Join on the resolved `chart_id`, not on the URL as GA4 recorded it: that URL is truncated at 126
    # characters and may be a slug the chart has since been renamed away from, so matching it against
    # the chart's current URL loses views (owid/analytics#733).
    query = f"""
    SELECT
        c.chart_id,
        c.url,
        c.published_at,
        SUM(v.events) AS views
    FROM {SEMANTIC_LAYER_SCHEMA}.charts c
    JOIN {GA_SCHEMA}.grapher_views_detailed v ON v.chart_id = c.chart_id
    {where_sql}
    GROUP BY c.chart_id, c.url, c.published_at
    ORDER BY views DESC
    """
    df_views = read_analytics(sql=query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    df_views["n_days"] = get_number_of_days(
        published_at=df_views["published_at"],
        date_min=date_min,
        date_max=date_max,
        table_name=f"{GA_SCHEMA}.grapher_views_detailed",
        day_column_name="day",
    )

    # Add a column for the average number of daily views.
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views


def get_chart_views_last_n_days(
    chart_ids: list[int] | None = None,
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
    urls: list[str] | None = None,
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
    FROM {SEMANTIC_LAYER_SCHEMA}.views_detailed
    JOIN {SEMANTIC_LAYER_SCHEMA}.pages USING(url)
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
    df_views = read_analytics(sql=query)

    # To calculate the average daily views, we need to figure out the number of days for which we are counting views.
    df_views["n_days"] = get_number_of_days(
        date_min=date_min,
        date_max=date_max,
        table_name=f"{SEMANTIC_LAYER_SCHEMA}.views_detailed",
        day_column_name="day",
    )

    # Add a column for the average number of daily views.
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]

    # Fix infs (for charts that were published in the last day).
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    return df_views


def get_explorer_views_by_url(
    urls: list[str] | None = None,
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """
    Fetch number of views for explorer pages for a list of URLs from Metabase.

    Parameters
    ----------
    urls : list of str, optional
        List of explorer URLs to filter the results. If None, all explorer views are included.
    date_min : str, optional
        Minimum date to filter the results.
    date_max : str, optional
        Maximum date to filter the results.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the number of explorer views per URL, with columns:
        url, title, views, n_days, views_daily.
    """
    query = f"""
    SELECT
        url,
        SUM(views) AS views
    FROM {SEMANTIC_LAYER_SCHEMA}.views_detailed
    JOIN {SEMANTIC_LAYER_SCHEMA}.pages USING(url)
    WHERE day >= '{date_min}'
    AND day <= '{date_max}'
    AND type = 'explorer'
    AND url IS NOT NULL
    """
    if urls:
        url_list = ", ".join(f"'{url}'" for url in urls)
        query += f" AND url IN ({url_list})"
    query += """
    GROUP BY url
    ORDER BY views DESC
    """
    df_views = read_analytics(sql=query)

    df_views["n_days"] = get_number_of_days(
        date_min=date_min,
        date_max=date_max,
        table_name=f"{SEMANTIC_LAYER_SCHEMA}.views_detailed",
        day_column_name="day",
    )
    df_views["views_daily"] = df_views["views"] / df_views["n_days"]
    df_views.loc[df_views["views_daily"] == float("inf"), "views_daily"] = 0

    # Fetch explorer titles from the OWID DB by joining on slug derived from URL.
    explorer_base_url = POST_LINK_TYPES_TO_URL["explorer"]
    df_views["slug"] = df_views["url"].str.removeprefix(explorer_base_url).str.split("?").str[0]
    df_titles = OWID_ENV.read_sql(
        "SELECT slug, JSON_UNQUOTE(JSON_EXTRACT(config, '$.explorerTitle')) AS title FROM explorers"
    )
    df_views = df_views.merge(df_titles, on="slug", how="left").drop(columns=["slug"])

    # Reorder columns for convenience.
    cols = ["url", "title", "views", "n_days", "views_daily"]
    df_views = df_views[cols]

    return df_views


def get_views_by_url(
    urls: list[str],
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """Sum period views for an arbitrary list of URLs, reading the right table per URL prefix.

    One fetcher over two tables, keyed by URL prefix:
    * ``/grapher/…`` → ``grapher_views_detailed`` (``SUM(events)``). Both chart and mdim URLs live here.
    * ``/explorers/…`` → ``views_detailed`` + ``pages`` (``type='explorer'``, ``SUM(views)``).
      ``grapher_views_detailed`` has NO explorer rows, so explorer URLs must use this second table.

    Returns columns: url, views. Only URLs with at least one view in the range appear.
    """
    empty = pd.DataFrame(columns=["url", "views"])
    if not urls:
        return empty

    grapher_urls = [u for u in urls if "/grapher/" in u]
    explorer_urls = [u for u in urls if "/explorers/" in u]

    dfs = []
    if grapher_urls:
        url_list = ", ".join(f"'{u}'" for u in grapher_urls)
        dfs.append(
            read_analytics(
                sql=f"""
                SELECT grapher AS url, SUM(events) AS views
                FROM {GA_SCHEMA}.grapher_views_detailed
                WHERE day >= '{date_min}' AND day <= '{date_max}' AND grapher IN ({url_list})
                GROUP BY grapher
                """
            )
        )
    if explorer_urls:
        url_list = ", ".join(f"'{u}'" for u in explorer_urls)
        dfs.append(
            read_analytics(
                sql=f"""
                SELECT vd.url AS url, SUM(vd.views) AS views
                FROM {SEMANTIC_LAYER_SCHEMA}.views_detailed vd
                JOIN {SEMANTIC_LAYER_SCHEMA}.pages p USING(url)
                WHERE p.type = 'explorer' AND vd.day >= '{date_min}' AND vd.day <= '{date_max}'
                  AND vd.url IN ({url_list})
                GROUP BY vd.url
                """
            )
        )

    df = _safe_concat(dfs=dfs)
    return df if not df.empty else empty


def get_redirected_source_views(
    producer_view_config_ids: set[str],
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """Views absorbed by a producer's mdim VIEWS from charts/explorers redirected INTO them (Part A).

    View-level attribution: ``multi_dim_redirects`` maps each retired chart/explorer URL to the exact
    target mdim VIEW it now points to (``viewConfigId``). A source is credited only if that target view is
    one of the producer's views (``viewConfigId`` in ``producer_view_config_ids``, derived from
    ``mdim_explorers_x_indicators`` → indicator → producer). The source's pre-redirect views come from
    ``grapher_views_detailed`` (grapher sources) / ``views_detailed`` (explorer sources) via
    get_views_by_url - full render history, unaffected by the view-level analytics start date, because the
    source→view mapping is grapher-DB config, not analytics.

    This supersedes the earlier whole-surface attribution: crediting the specific view (not the whole
    mdim) removes the over-crediting of mixed-producer mdims. Rows with a null ``viewConfigId`` in
    ``multi_dim_redirects`` (a handful) are dropped - they can't be view-attributed.

    NOTE: this covers redirects recorded in ``multi_dim_redirects`` (chart→mdim and explorer→mdim, the
    only place with a ``viewConfigId``). The catch-all ``redirects_all`` log (chart→explorer,
    explorer→explorer, chart→mdim query-param) has no ``viewConfigId`` and is not view-attributable, so it
    is out of scope here; for the current providers those additions were negligible.

    Parameters
    ----------
    producer_view_config_ids : set of str
        The producer's mdim view ids (``view_config_id``). Only redirects into one of these are kept.

    Returns
    -------
    pd.DataFrame
        One row per absorbed source: source_url, target_slug, view_config_id, redirect_date, views.
    """
    cols = ["source_url", "target_slug", "view_config_id", "redirect_date", "views"]
    if not producer_view_config_ids:
        return pd.DataFrame(columns=cols)

    # multi_dim_redirects (grapher DB): retired source URL → target mdim view (viewConfigId) + slug.
    df_mdr = OWID_ENV.read_sql(
        """
        SELECT
            CONCAT('https://ourworldindata.org', r.source) AS source_url,
            r.viewConfigId AS view_config_id,
            m.slug AS target_slug,
            r.createdAt AS redirect_date
        FROM multi_dim_redirects r
        JOIN multi_dim_data_pages m ON m.id = r.multiDimId
        WHERE r.viewConfigId IS NOT NULL
        """
    )
    df_mdr = df_mdr[df_mdr["view_config_id"].isin(producer_view_config_ids)].copy()
    # One redirect per source URL (defensive against duplicate rows for the same source).
    df_mdr = df_mdr.drop_duplicates(subset=["source_url"]).reset_index(drop=True)
    if df_mdr.empty:
        return pd.DataFrame(columns=cols)

    # Pre-redirect views drawn by each source URL in the period (full render history via get_views_by_url).
    df_views = get_views_by_url(urls=sorted(set(df_mdr["source_url"])), date_min=date_min, date_max=date_max)
    df_mdr = df_mdr.merge(df_views, left_on="source_url", right_on="url", how="left").drop(columns=["url"])
    df_mdr["views"] = df_mdr["views"].fillna(0).astype(int)
    # createdAt is tz-naive from MySQL; normalise via utc=True so .dt works, then keep just the date.
    df_mdr["redirect_date"] = pd.to_datetime(df_mdr["redirect_date"], errors="coerce", utc=True).dt.date.astype(str)
    df_mdr.loc[df_mdr["redirect_date"] == "NaT", "redirect_date"] = ""

    return df_mdr[cols].sort_values("views", ascending=False).reset_index(drop=True)


def get_mdim_explorer_views_by_producer(
    producers: list[str],
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
) -> pd.DataFrame:
    """Get views of a producer's mdim VIEWS (broken out per view) and explorers.

    Mdims are reported **per view** (one row per producer view), not per whole mdim:

    - **Own views** come from ``chart_views_detailed`` (per ``view_config_id``). These are the config'd
      renders; renders with no ``view_config_id`` (~mount artifacts that double-count views already
      captured in the same session) are correctly excluded. This makes ``chart_views_detailed`` the
      accurate per-view AND total source; ``grapher_views_detailed`` (used previously) over-counts mdim
      views by an embedding-driven amount and is no longer used for mdim own-views.
    - **Redirected-in views** come from get_redirected_source_views (Part A): charts/explorers redirected
      into this specific view, credited via ``multi_dim_redirects.viewConfigId``.
    - Only views that use the producer's data are included, so there is no whole-surface over-crediting.
      ``uses_other_producers_data`` still flags a producer view that ALSO contains another producer's
      indicator.

    Explorers are reported **whole-surface** (one row per explorer), own views from ``views_detailed`` as
    before - explorer page views have no clean per-view equivalent, so they are not broken out and get no
    redirect recovery here.

    Coverage caveat: per-view own views only exist from the ``chart_views_detailed`` start date
    (2026-03-27). For periods before that a view's own views are undercounted; the current producer mdims
    are new enough that this is ~0. Redirected-in views are unaffected (full history).

    Parameters
    ----------
    producers : list of str
        Producer names to include.
    date_min : str, optional
        Minimum date to filter the results.
    date_max : str, optional
        Maximum date to filter the results.

    Returns
    -------
    pd.DataFrame
        Columns: slug, type, view_config_id (None for explorers), dimensions (None for explorers), title,
        url, own_views, redirected_views, views (own + redirected), n_days, views_daily,
        uses_other_producers_data, includes_redirect_views.
    """
    cols = [
        "slug",
        "type",
        "view_config_id",
        "dimensions",
        "title",
        "url",
        "own_views",
        "redirected_views",
        "views",
        "n_days",
        "views_daily",
        "uses_other_producers_data",
        "includes_redirect_views",
    ]

    def _empty_result() -> pd.DataFrame:
        # uses_other_producers_data / includes_redirect_views must be explicitly bool - see note in
        # get_producer_variable_ids callers; boolean-indexing an empty object-dtype column drops columns.
        return pd.DataFrame(columns=cols).astype({"uses_other_producers_data": bool, "includes_redirect_views": bool})

    variable_ids = set(get_producer_variable_ids(producers=producers))
    if not variable_ids:
        return _empty_result()

    # Per-view indicator attribution. NOTE: filtered client-side (see note in the whole-table pull) to
    # avoid inlining a prolific producer's ~80k indicator ids into the SQL.
    df_map = read_analytics(
        sql=f"SELECT DISTINCT slug, type, view_config_id, indicator_id "
        f"FROM {SEMANTIC_LAYER_SCHEMA}.mdim_explorers_x_indicators WHERE view_config_id IS NOT NULL"
    )
    df_map["is_producer"] = df_map["indicator_id"].isin(variable_ids)

    frames = []

    # ---- MDIMS: per view ----
    mdim_map = df_map[df_map["type"] == "multidim"]
    producer_views = mdim_map.loc[mdim_map["is_producer"], ["slug", "view_config_id"]].drop_duplicates()
    if not producer_views.empty:
        # Per-view "also uses another producer's data" flag.
        view_flags = (
            mdim_map.merge(producer_views, on=["slug", "view_config_id"])
            .groupby(["slug", "view_config_id"])["is_producer"]
            .agg(lambda is_producer: bool((~is_producer).any()))
            .rename("uses_other_producers_data")
            .reset_index()
        )
        producer_view_ids = set(producer_views["view_config_id"])
        mdim_slugs = sorted(producer_views["slug"].unique())
        slug_urls = ", ".join(f"'{GRAPHERS_BASE_URL}{s}'" for s in mdim_slugs)

        # Own views per view (window) + view metadata (chart_view_url, dimensions) from all-time so that
        # views absorbed via redirects but with ~no own renders in the window still get a label/URL.
        df_cvd = read_analytics(
            sql=f"""
            SELECT
                chart_url,
                view_config_id,
                ANY_VALUE(chart_view_url) AS chart_view_url,
                ANY_VALUE(dimensions) AS dimensions,
                SUM(IF(day >= '{date_min}' AND day <= '{date_max}', events, 0)) AS own_views
            FROM {SEMANTIC_LAYER_SCHEMA}.chart_views_detailed
            WHERE type = 'multidim' AND view_config_id IS NOT NULL AND chart_url IN ({slug_urls})
            GROUP BY chart_url, view_config_id
            """
        )
        df_cvd["slug"] = df_cvd["chart_url"].str.removeprefix(GRAPHERS_BASE_URL)

        # Redirected-in views per target view (Part A).
        df_red = get_redirected_source_views(
            producer_view_config_ids=producer_view_ids, date_min=date_min, date_max=date_max
        )
        red_by_view = (
            df_red.groupby(["target_slug", "view_config_id"])["views"]
            .sum()
            .rename("redirected_views")
            .reset_index()
            .rename(columns={"target_slug": "slug"})
        )

        # Master list = every producer view; left-join own views + metadata + redirected views + flags.
        mdims = producer_views.merge(
            df_cvd[["slug", "view_config_id", "chart_view_url", "dimensions", "own_views"]],
            on=["slug", "view_config_id"],
            how="left",
        )
        mdims = mdims.merge(red_by_view, on=["slug", "view_config_id"], how="left")
        mdims = mdims.merge(view_flags, on=["slug", "view_config_id"], how="left")
        mdims["own_views"] = mdims["own_views"].fillna(0).astype(int)
        mdims["redirected_views"] = mdims["redirected_views"].fillna(0).astype(int)
        mdims["uses_other_producers_data"] = mdims["uses_other_producers_data"].fillna(False)
        # Keep only views with some traffic in the period.
        mdims = mdims[(mdims["own_views"] + mdims["redirected_views"]) > 0].reset_index(drop=True)

        if not mdims.empty:
            mdims["type"] = "multidim"
            mdims["views"] = mdims["own_views"] + mdims["redirected_views"]
            mdims["includes_redirect_views"] = mdims["redirected_views"] > 0
            # URL: the specific view's URL if known, else the bare mdim URL.
            mdims["url"] = mdims["chart_view_url"].where(
                mdims["chart_view_url"].notna(), GRAPHERS_BASE_URL + mdims["slug"]
            )
            # Title = the actual grapher chart title for each view. Every mdim view resolves to its own
            # grapher config (chart_configs row named by view_config_id); its `$.title` is the chart title
            # a reader sees for that view (e.g. "Decadal average: Annual death rate from all natural
            # disasters"), which is more informative than the mdim title plus the raw dimension selection.
            vc_list = ", ".join(f"'{v}'" for v in mdims["view_config_id"].dropna().unique())
            df_view_titles = (
                OWID_ENV.read_sql(
                    f"SELECT id AS view_config_id, JSON_UNQUOTE(JSON_EXTRACT(config, '$.title')) AS title "
                    f"FROM chart_configs WHERE id IN ({vc_list})"
                )
                if vc_list
                else pd.DataFrame(columns=["view_config_id", "title"])
            )
            mdims = mdims.merge(df_view_titles, on="view_config_id", how="left")
            # Fall back to the mdim's own title, then its slug, for any view without a resolved title.
            mdim_list = ", ".join(f"'{s}'" for s in mdim_slugs)
            df_mdim_titles = OWID_ENV.read_sql(
                f"SELECT slug, JSON_UNQUOTE(JSON_EXTRACT(config, '$.title.title')) AS mdim_title "
                f"FROM multi_dim_data_pages WHERE slug IN ({mdim_list})"
            )
            mdims = mdims.merge(df_mdim_titles, on="slug", how="left")
            mdims["title"] = mdims["title"].fillna(mdims["mdim_title"]).fillna(mdims["slug"])
            mdims = mdims.drop(columns=["mdim_title"])
            n_days_mdim = get_number_of_days(
                date_min=date_min, date_max=date_max, table_name=f"{SEMANTIC_LAYER_SCHEMA}.chart_views_detailed"
            )
            mdims["n_days"] = n_days_mdim
            mdims["views_daily"] = mdims["views"] / mdims["n_days"]
            mdims.loc[mdims["views_daily"] == float("inf"), "views_daily"] = 0
            frames.append(mdims[cols])

    # ---- EXPLORERS: whole-surface (own page views; no per-view breakdown, no redirect recovery) ----
    explorer_map = df_map[df_map["type"] == "explorer"]
    explorer_relevant = explorer_map.loc[explorer_map["is_producer"], ["slug"]].drop_duplicates()
    if not explorer_relevant.empty:
        explorer_flags = (
            explorer_map.merge(explorer_relevant, on="slug")
            .groupby("slug")["is_producer"]
            .agg(lambda is_producer: bool((~is_producer).any()))
            .rename("uses_other_producers_data")
            .reset_index()
        )
        explorer_slugs = sorted(explorer_relevant["slug"].tolist())
        df_explorers = get_explorer_views_by_url(
            urls=[f"{POST_LINK_TYPES_TO_URL['explorer']}{slug}" for slug in explorer_slugs],
            date_min=date_min,
            date_max=date_max,
        )
        if not df_explorers.empty:
            df_explorers["slug"] = df_explorers["url"].str.removeprefix(POST_LINK_TYPES_TO_URL["explorer"])
            df_explorers = df_explorers.merge(explorer_flags, on="slug", how="left")
            df_explorers["type"] = "explorer"
            df_explorers["view_config_id"] = None
            df_explorers["dimensions"] = None
            df_explorers["own_views"] = df_explorers["views"].astype(int)
            df_explorers["redirected_views"] = 0
            df_explorers["includes_redirect_views"] = False
            df_explorers["uses_other_producers_data"] = df_explorers["uses_other_producers_data"].fillna(False)
            frames.append(df_explorers[cols])

    if not frames:
        return _empty_result()

    result = pd.concat(frames, ignore_index=True)
    return result.sort_values("views", ascending=False).reset_index(drop=True)


def _get_post_references_of_charts_and_redirected_charts(
    chart_ids: list[int] | None = None, component_types: list[str] | None = None
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


def _get_post_references_of_charts_via_narrative_charts(chart_ids: list[int] | None = None) -> pd.DataFrame:
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
    chart_ids: list[int] | None = None, only_topics_with_all_charts_block: bool = False
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


def get_post_references_of_charts(
    chart_ids: list[int] | None = None,
    component_types: list[str] | None = None,
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
    chart_ids: list[int] | None = None,
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


# Post types the producer report covers (articles, topic pages, data insights). Others (author/homepage/
# fragment/about) are excluded so recovered posts match the same universe as the live-chart post lookup.
_REPORTED_POST_TYPES = ("article", "topic-page", "linear-topic-page", "data-insight")


def _get_post_views_citing_slugs(
    slugs: list[str],
    component_types: list[str],
    date_min: str,
    date_max: str,
) -> pd.DataFrame:
    """Views of published posts that link any of ``slugs`` via one of ``component_types``.

    Shared tail of the post-recovery lookups: posts come from ``posts_gdocs_links`` (grapher DB), views
    from get_post_views_by_url. Returns the report-aligned columns: url, title, post_type,
    post_publication_date, views, n_days, views_daily.
    """
    cols = ["url", "title", "post_type", "post_publication_date", "views", "n_days", "views_daily"]
    empty = pd.DataFrame(columns=cols)
    if not slugs:
        return empty

    slug_list = ", ".join(f"'{s}'" for s in sorted(set(slugs)))
    component_types_str = ", ".join(f"'{c}'" for c in component_types)

    # Posts citing those slugs, restricted to the qualifying component types.
    df = OWID_ENV.read_sql(
        f"""
        SELECT DISTINCT
            pg.slug AS post_slug,
            pg.type AS post_type,
            JSON_UNQUOTE(JSON_EXTRACT(pg.content, '$.title')) AS title,
            pg.publishedAt AS post_publication_date
        FROM posts_gdocs pg
        JOIN posts_gdocs_links pgl ON pg.id = pgl.sourceId
        WHERE pg.published = 1
          AND pgl.componentType IN ({component_types_str})
          AND pgl.target IN ({slug_list})
        """
    )
    df = df[df["post_type"].isin(_REPORTED_POST_TYPES)].copy()
    if df.empty:
        return empty

    df["url"] = df["post_type"].map(POST_TYPE_TO_URL) + df["post_slug"]
    df = df.dropna(subset=["url"]).drop_duplicates(subset=["url"]).reset_index(drop=True)
    df["post_publication_date"] = pd.to_datetime(df["post_publication_date"]).dt.date.astype(str)

    # Attach post views.
    df_views = get_post_views_by_url(urls=sorted(set(df["url"])), date_min=date_min, date_max=date_max)
    df = df.merge(df_views, on="url", how="left").dropna(subset=["views"]).reset_index(drop=True)
    if df.empty:
        return empty
    df = df.astype({"views": int, "n_days": int})

    return df[cols]


def get_post_views_of_redirected_charts_by_producer(
    producers: list[str],
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
    component_types: list[str] | None = None,
) -> pd.DataFrame:
    """Views of posts that cite a chart/explorer slug now redirected INTO one of the producer's mdim views.

    Article-recovery counterpart of get_redirected_source_views (Part A): a post that links to a retired
    chart/explorer slug still "shows" the producer's data (the slug now redirects into a producer mdim
    view), but the live-chart post lookup (get_post_views_by_chart_id) misses it because there is no live
    producer chart_id to match on. This finds those posts via ``multi_dim_redirects`` (source slug →
    producer view) and returns their views.

    Only links whose ``componentType`` is in ``component_types`` (default
    COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS) are counted, matching the report's view-attribution rule -
    i.e. the same rule the live-chart lookup applies, so recovered and existing posts are comparable.

    Renamed chart slugs (redirect taxonomy category A) are already recovered by the live-chart lookup via
    ``chart_slug_redirects``; this adds the chart/explorer→mdim case (B/C/D). Redirects with a null
    ``viewConfigId`` can't be view-attributed and are skipped.

    Returns
    -------
    pd.DataFrame
        Columns: url, title, post_type, post_publication_date, views, n_days, views_daily - aligned with
        the report's processed posts dataframe so it can be concatenated directly.
    """
    cols = ["url", "title", "post_type", "post_publication_date", "views", "n_days", "views_daily"]
    empty = pd.DataFrame(columns=cols)

    if component_types is None:
        component_types = COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS

    variable_ids = set(get_producer_variable_ids(producers=producers))
    if not variable_ids:
        return empty

    # Producer mdim view ids (views that use the producer's data).
    df_map = read_analytics(
        sql=f"SELECT DISTINCT view_config_id, indicator_id FROM {SEMANTIC_LAYER_SCHEMA}.mdim_explorers_x_indicators "
        f"WHERE type = 'multidim' AND view_config_id IS NOT NULL"
    )
    producer_view_ids = set(df_map.loc[df_map["indicator_id"].isin(variable_ids), "view_config_id"])
    if not producer_view_ids:
        return empty

    # Retired source slugs that now redirect into one of those views.
    df_mdr = OWID_ENV.read_sql(
        "SELECT DISTINCT source, viewConfigId FROM multi_dim_redirects WHERE viewConfigId IS NOT NULL"
    )
    df_mdr = df_mdr[df_mdr["viewConfigId"].isin(producer_view_ids)]
    if df_mdr.empty:
        return empty

    def _slug(source: str) -> str:
        for marker in ("/grapher/", "/explorers/"):
            if marker in source:
                return source.split(marker)[-1].split("?")[0].strip("/")
        return source.strip("/")

    source_slugs = sorted({_slug(s) for s in df_mdr["source"]})

    return _get_post_views_citing_slugs(
        slugs=source_slugs, component_types=component_types, date_min=date_min, date_max=date_max
    )


def get_post_views_of_producer_collections(
    producers: list[str],
    date_min: str = DATE_MIN,
    date_max: str = DATE_MAX,
    component_types: list[str] | None = None,
) -> pd.DataFrame:
    """Views of posts that embed one of the producer's mdims or explorers (linked by the live slug).

    Post-attribution counterpart of get_mdim_explorer_views_by_producer: a post that embeds an mdim or
    explorer shows the producer's data, but the live-chart post lookup (get_post_views_by_chart_id)
    matches on chart ids (collections have none), and get_post_views_of_redirected_charts_by_producer
    only covers dead slugs. When charts are migrated into an mdim, posts are typically re-pointed to the
    live mdim slug, so both lookups miss them.

    Only links whose ``componentType`` is in ``component_types`` (default
    COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS) are counted - the same "post displays the chart" rule as the
    other post lookups. Like the whole-chart rule, a post embedding a multi-producer collection counts in
    full for each producer whose data is in it.

    Returns
    -------
    pd.DataFrame
        Columns: url, title, post_type, post_publication_date, views, n_days, views_daily - aligned with
        the report's processed posts dataframe so it can be concatenated directly.
    """
    cols = ["url", "title", "post_type", "post_publication_date", "views", "n_days", "views_daily"]
    empty = pd.DataFrame(columns=cols)

    if component_types is None:
        component_types = COMPONENT_TYPES_TO_LINK_GDOCS_WITH_VIEWS

    variable_ids = set(get_producer_variable_ids(producers=producers))
    if not variable_ids:
        return empty

    # Slugs of mdims/explorers with at least one view (mdims) or indicator (explorers) using the
    # producer's data.
    df_map = read_analytics(
        sql=f"SELECT DISTINCT slug, indicator_id FROM {SEMANTIC_LAYER_SCHEMA}.mdim_explorers_x_indicators"
    )
    collection_slugs = sorted(set(df_map.loc[df_map["indicator_id"].isin(variable_ids), "slug"]))

    return _get_post_views_citing_slugs(
        slugs=collection_slugs, component_types=component_types, date_min=date_min, date_max=date_max
    )


def get_post_views_last_n_days(
    chart_ids: list[int] | None = None,
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


def get_producer_variable_ids(producers: list[str]) -> list[int]:
    """Get all indicator (variable) IDs attributed to a given list of producers.

    Unlike get_visualizations_using_data_by_producer, this is not restricted to indicators used in charts, so it
    can also be used to find indicators used in other kinds of visualizations (e.g. mdim/explorer views).

    Parameters
    ----------
    producers : list of str
        Producer names to include.

    Returns
    -------
    list of int
        Distinct variable IDs attributed to the given producers.

    """
    producers_str = ", ".join(f"'{p}'" for p in [p.replace("'", "''") for p in producers])
    query = f"""
    SELECT DISTINCT ov.variableId AS variable_id
    FROM origins_variables ov
    JOIN origins o ON o.id = ov.originId
    WHERE o.producer IN ({producers_str})
    """
    df = OWID_ENV.read_sql(query)

    return sorted(df["variable_id"].tolist())


def get_visualizations_using_data_by_producer(
    producers: list[str] | None = None, excluded_steps: list[str] | None = None
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
		JSON_UNQUOTE(JSON_EXTRACT(cc.config, '$.title')) chart_title,
		cc.slug chart_slug,
		CONCAT('{GRAPHERS_BASE_URL}', cc.slug) chart_url,
		JSON_EXTRACT(cc.config, '$.isPublished') is_published,
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
        # Resolve producers to their indicator IDs first, and filter on those, instead of matching the producer
        # name directly in this query (avoids duplicating producer-name-matching logic in two places).
        variable_ids = get_producer_variable_ids(producers=producers)
        # Use an impossible id if none is found, so the query below still runs and returns an empty (but well-formed) dataframe.
        ids_str = ", ".join(str(vid) for vid in variable_ids) if variable_ids else "-1"
        query += f"\nAND variable_id IN ({ids_str})"

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
