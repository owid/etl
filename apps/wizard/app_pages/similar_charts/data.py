from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from apps.utils.google import read_gbq
from apps.wizard.utils.embeddings import Doc
from etl.config import memory
from etl.db import read_sql


@dataclass
class Chart(Doc):
    chart_id: int
    title: str
    subtitle: str
    note: str
    tags: list[str]
    slug: str
    created_at: Optional[datetime] = None
    views_7d: Optional[int] = None
    views_14d: Optional[int] = None
    views_365d: Optional[int] = None
    gpt_reason: Optional[str] = None
    coviews: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)


def get_raw_charts() -> pd.DataFrame:
    """Get all charts that exist in the database."""
    # TODO: allow archived charts to be returned. Maybe add argument to function

    # Get all data indicators from the database.
    query = """
    with tags as (
        select
            ct.chartId as chart_id,
            -- t.name as tag_name,
            -- t.slug as tag_slug,
            group_concat(t.name separator ';') as tags
        from chart_tags as ct
        join tags as t on ct.tagId = t.id
        group by 1
    )
    select
        c.id as chart_id,
        c.createdAt as created_at,
        cf.slug,
        cf.full->>'$.title' as title,
        cf.full->>'$.subtitle' as subtitle,
        cf.full->>'$.note' as note,
        t.tags,
        a.views_7d,
        a.views_14d,
        a.views_365d
    from charts as c
    join chart_configs as cf on c.configId = cf.id
    left join analytics_pageviews as a on cf.slug = SUBSTRING_INDEX(a.url, '/', -1) and a.url like '%%/grapher/%%'
    left join tags as t on c.id = t.chart_id
    -- TODO: remove in prod
    -- test it on charts with 'human' in the title
    -- where lower(cf.full->>'$.title') like '%%human%%'
    -- exclude drafts
    where cf.full->>'$.isPublished' != 'false'
    """
    df = read_sql(query)

    # charts must have unique id
    assert df["chart_id"].nunique() == df.shape[0]

    return df


@memory.cache
def get_coviews_sessions(after_date: str, min_sessions: int = 5) -> pd.Series:
    """
    Count of sessions in which a pair of URLs are both visited, aggregated daily

    note: this is a nondirectional network. url1 and url2 are string sorted and
    do not indicate anything about whether url1 was visited before/after url2 in
    the session.
    """
    query = f"""
        SELECT
            REGEXP_EXTRACT(url1, r'grapher/([^/]+)') AS slug1,
            REGEXP_EXTRACT(url2, r'grapher/([^/]+)') AS slug2,
            SUM(sessions_coviewed) AS total_sessions
        FROM prod_google_analytics4.coviews_by_day_page
        WHERE day >= '{after_date}'
            AND url1 LIKE 'https://ourworldindata.org/grapher%'
            AND url2 LIKE 'https://ourworldindata.org/grapher%'
        GROUP BY slug1, slug2
        HAVING total_sessions >= {min_sessions}
    """
    df = read_gbq(query, project_id="owid-analytics")

    # concat with reversed slug1 and slug2
    df = pd.concat([df, df.rename(columns={"slug1": "slug2", "slug2": "slug1"})])

    # set index for faster lookups
    return df.set_index(["slug1", "slug2"]).sort_index()["total_sessions"]
