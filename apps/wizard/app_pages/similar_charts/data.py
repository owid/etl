from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from apps.wizard.utils.embeddings import Doc
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
