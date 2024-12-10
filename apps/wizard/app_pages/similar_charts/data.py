from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd
import streamlit as st

from etl.db import read_sql


@dataclass
class Chart:
    chart_id: int
    title: str
    subtitle: str
    tags: str
    slug: str
    similarity: float


def get_raw_charts() -> pd.DataFrame:
    """Get all charts that exist in the database."""
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
        c.id as chartId,
        cf.slug,
        cf.full->>'$.title' as title,
        cf.full->>'$.subtitle' as subtitle,
        cf.full->>'$.note' as note,
        t.tags
    from charts as c
    join chart_configs as cf on c.configId = cf.id
    left join tags as t on c.id = t.chart_id
    -- test it on charts with 'human' in the title
    where lower(cf.full->>'$.title') like '%%human%%'
    """
    df = read_sql(query)

    return df


@st.cache_data(show_spinner=False, persist="disk", max_entries=1)
def get_charts() -> list[Chart]:
    with st.spinner("Loading charts..."):
        # Get charts from the database.
        df = get_raw_charts()

        charts = df.to_dict(orient="records")

    return [Chart(**c) for c in charts]  # type: ignore
