import json
import os
import re
from typing import Any, Dict, Tuple

import pandas as pd
import streamlit as st

from etl.db import read_sql


def get_raw_data_indicators() -> pd.DataFrame:
    """Get the content of data indicators that exist in the database."""
    # Get all data indicators from the database.
    # TODO: add catalogPath to the query
    query = """
    with t as (
        select
            v.id as variableId,
            v.name,
            COALESCE(v.description, v.descriptionShort) as description,
            v.catalogPath
        from variables as v
        join datasets as d on v.datasetId = d.id
        where d.isArchived = 0
    ), n_charts as (
        select
            variableId,
            count(distinct chartId) as n_charts
        from chart_dimensions as cd
        group by 1
    )
    select
        t.variableId,
        t.name,
        COALESCE(t.description, '') as description,
        t.catalogPath,
        COALESCE(n_charts.n_charts, 0) as n_charts
    from t
    -- TODO: uncomment me to include indicators without charts
    -- left join n_charts on t.variableId = n_charts.variableId
    join n_charts on t.variableId = n_charts.variableId
    """
    df = read_sql(query)

    return df


@st.cache_data(show_spinner=False)
def get_data_indicators() -> list[Dict[str, Any]]:
    with st.spinner("Loading data indicators..."):
        # Get the raw data indicators from the database.
        df = get_raw_data_indicators()

        indicators = df.to_dict(orient="records")

    return indicators
