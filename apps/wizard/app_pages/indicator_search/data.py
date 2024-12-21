from dataclasses import dataclass
from typing import Optional

import streamlit as st
from dataclasses_json import DataClassJsonMixin

from apps.wizard.utils.embeddings import Doc
from etl.db import read_sql


@dataclass
class Indicator(Doc, DataClassJsonMixin):
    variableId: int
    name: str
    description: str
    n_charts: int
    catalogPath: str
    dataset: Optional[str] = None

    def text(self) -> str:
        # Combine the name and description into a single string
        # NOTE: Using both name and description can be sometimes too long, making the embedding less accurate.
        #  One example is query "beer" for which the indicator
        #     "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol) - Beverage types: Beer"
        #  has lower similarity than indicator "Coffee".
        #  Append description only if the name is too short helps a bit. An alternative would be to shorten
        #  it with a summarizer before embedding.
        return self.name + " " + self.description


# TODO: The data is cached forever, we should sometimes refresh it.
@st.cache_data(show_spinner=True, persist="disk", max_entries=1)
def get_data_indicators() -> list[Indicator]:
    # Get the raw data indicators from the database.
    query = """
    with t as (
        select
            v.id as variableId,
            v.name,
            COALESCE(v.description, v.descriptionShort) as description,
            CASE
                WHEN v.catalogPath IS NULL THEN CONCAT('grapher/', COALESCE(d.namespace, 'NULL'), '/', COALESCE(d.version, 'NULL'), '/', 'NULL', '/NULL#', v.name)
                ELSE v.catalogPath
            END AS catalogPath
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
    left join n_charts on t.variableId = n_charts.variableId
    -- only indicators with charts
    -- join n_charts on t.variableId = n_charts.variableId
    """
    df = read_sql(query)

    assert df.catalogPath.notnull().all(), "Some indicators have null catalogPath"

    indicators = df.to_dict(orient="records")

    return [Indicator(**indicator) for indicator in indicators]  # type: ignore
