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


def _get_data_indicators_from_db() -> list[Indicator]:
    """Get indicators from database without caching."""
    query = """
    SELECT
        v.id as variableId,
        v.name,
        COALESCE(v.description, v.descriptionShort, '') as description,
        CASE
            WHEN v.catalogPath IS NULL THEN CONCAT('grapher/', COALESCE(d.namespace, 'NULL'), '/', COALESCE(d.version, 'NULL'), '/', 'NULL', '/NULL#', v.name)
            ELSE v.catalogPath
        END AS catalogPath,
        COALESCE(cd_counts.n_charts, 0) as n_charts
    FROM datasets d
    INNER JOIN variables v ON d.id = v.datasetId
    LEFT JOIN (
        SELECT
            variableId,
            COUNT(DISTINCT chartId) as n_charts
        FROM chart_dimensions
        GROUP BY variableId
    ) cd_counts ON v.id = cd_counts.variableId
    WHERE d.isArchived = 0
    """
    df = read_sql(query)
    indicators = df.to_dict(orient="records")
    return [Indicator(**indicator) for indicator in indicators]  # type: ignore


# TODO: The data is cached forever, we should sometimes refresh it.
@st.cache_data(show_spinner=True, persist="disk", max_entries=1)
def get_data_indicators() -> list[Indicator]:
    """Get indicators with Streamlit caching."""
    return _get_data_indicators_from_db()
