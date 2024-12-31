from collections import defaultdict
from typing import Any, Dict

import pandas as pd
import streamlit as st

from etl.config import OWID_ENV
from etl.db import read_sql
from etl.grapher.model import Dataset
from etl.indicator_upgrade.indicator_update import find_charts_from_variable_ids


@st.cache_data
def get_explorers(variable_ids):
    query = """select explorerSlug, variableId from explorer_variables where variableId in %s;"""
    param = (tuple(variable_ids),)

    df = read_sql(
        query,
        OWID_ENV.engine,
        params=param,
    )

    return df


@st.cache_data
def get_charts_views():
    """Get views of charts."""
    query = """SELECT SUBSTRING_INDEX(url, '/', -1) AS slug, views_7d, views_365d FROM analytics_pageviews WHERE url LIKE %s;"""
    param = ("%ourworldindata.org/grapher/%",)

    df = read_sql(
        query,
        OWID_ENV.engine,
        params=param,
    )
    return df.set_index("slug").to_dict(orient="index")


@st.cache_data
def get_explorers_views():
    """Get views of explorers."""
    query = """SELECT url, SUBSTRING_INDEX(url, '/', -1) AS slug, views_7d, views_365d FROM analytics_pageviews WHERE url LIKE %s;"""
    param = ("%ourworldindata.org/explorers/%",)

    df = read_sql(
        query,
        OWID_ENV.engine,
        params=param,
    )

    # Filter outlier
    df = df.loc[df["url"] != "https://ourworldindata.org/ourworldindata.org/explorers/co2"]

    return df.set_index("slug").to_dict(orient="index")


@st.cache_data
def get_datasets() -> Dict[int, Dict[str, Any]]:
    """Get list of datasets.

    NOTE: Only datasets with a catalogPath are considered (i.e. ETL-era).
    """
    df = Dataset.load_all_datasets()
    df = df.dropna(subset="catalogPath")

    # Set display name
    df["display_name"] = df["name"] + " --- " + df["catalogPath"] + " [" + df["version"] + "]"
    df = df.set_index("id", verify_integrity=True).sort_index(ascending=False)

    # Build dictionary
    dix = df.to_dict(orient="index")
    return dix  # type: ignore


@st.cache_data
def get_users():
    """Get users.

    This is to show actual names (and not user ids).
    """
    query = """SELECT * from users;"""

    df = read_sql(
        query,
        OWID_ENV.engine,
    )

    df = df.set_index("id").to_dict(orient="index")
    return df


def get_average_daily_views(views, chart_slug):
    num_views = views.get(chart_slug)

    if num_views is not None:
        num_views = num_views["views_365d"]
        return round(num_views / 365, 2)


def _get_title_chart(chart):
    if chart.publishedAt is None:
        return f"(DRAFT) {chart.config['title']}"
    return chart.config["title"]


def get_table_charts(indicator_charts, users, chart_views, indicator_id=None):
    if indicator_id is None:
        charts = indicator_charts.chart_id_to_chart.values()
    else:
        charts = indicator_charts.get_charts(indicator_id)
    df_charts = (
        pd.DataFrame(
            {
                # "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],  # type: ignore
                "Chart id": [chart.id for chart in charts],  # type: ignore
                "Chart": [_get_title_chart(chart) for chart in charts],  # type: ignore
                # "Views (last 7 days)": [CHART_VIEWS.get(chart.slug)["views_7d"] for chart in charts],  # type: ignore
                "views": [get_average_daily_views(chart_views, chart.slug) for chart in charts],  # type: ignore
                "Edit": [OWID_ENV.chart_admin_site(chart.id) for chart in charts],  # type: ignore
                "User": [users[chart.lastEditedByUserId]["fullName"] for chart in charts],  # type: ignore
                "Last edited": [chart.lastEditedAt for chart in charts],  # type: ignore
            }
        )
        .set_index("Chart id")
        .sort_values("views", ascending=False)
    )

    return df_charts


def get_table_explorers(indicator_explorers, explorer_views, indicator_id=None):
    if indicator_id is None:
        explorers = indicator_explorers.explorers
    else:
        explorers = indicator_explorers.get_explorers(indicator_id)

    if explorers is None:
        return None

    df = (
        pd.DataFrame(
            {
                # "thumbnail": [OWID_ENV.thumb_url(chart.slug) for chart in charts],  # type: ignore
                "Explorer slug": [e for e in explorers],  # type: ignore
                "views": [get_average_daily_views(explorer_views, slug) for slug in explorers],  # type: ignore
            }
        )
        .set_index("Explorer slug")
        .sort_values("views", ascending=False)
    )

    return df


def show_table_charts(df_charts):
    if not df_charts.empty:
        st.dataframe(
            df_charts,
            column_config={
                "Edit": st.column_config.LinkColumn(
                    label="Chart",
                    help="Link to chart edit page",
                    display_text=r"Edit",
                    width="small",
                ),
                "views": st.column_config.ProgressColumn(
                    label="Daily views (year-average)",
                    min_value=0,
                    max_value=1000,
                    format="%.2f",
                ),
            },
            use_container_width=True,
        )


def show_table_explorers(df):
    if (df is not None) and not df.empty:
        st.dataframe(
            df,
            use_container_width=True,
            column_config={
                "views": st.column_config.ProgressColumn(
                    label="Daily views (year-average)",
                    min_value=0,
                    max_value=1000,
                    format="%.2f",
                ),
            },
        )


#
# CLASSES
#
class IndicatorArray:
    def __init__(self, indicators, key):
        self.indicators = indicators
        self.key = key
        self.dimensions = None

    @property
    def is_mdim(self):
        return self.dimensions is not None


class IndicatorSingleDimension(IndicatorArray):
    """Wrapper around indicators that have a single dimension."""

    def __init__(self, indicator):
        """Object with a single indicator."""
        super().__init__([indicator], indicator.catalogPath)

    def get_dimension(self):
        return self.indicators[0]

    def get_default(self):
        raise self.get_dimension()


class IndicatorWithDimensions(IndicatorArray):
    """Wrapper around indicators that have multiple dimensions."""

    def __init__(self, indicators):
        """Object with all indicator-dimensions.

        indicators: List of Variable objects. They should all belong to the same indicator.
        """
        super().__init__(indicators, self.check_and_extract_key(indicators))
        self.df = self.create_df()
        self.df_dims = self.df.reset_index()[self.df.index.names].drop_duplicates()
        self.dimensions = self.get_dimensions()

    def get_dimensions_conditioned(self, dim_name, conditions=None):
        if (conditions is None) or (conditions == {}) or ((len(conditions) == 1) and (dim_name in conditions)):
            options = self.df_dims[dim_name].unique().tolist()
        else:
            # conditions = {
            #     "sex": "all",
            #     "age": "all",
            # }
            mask = True
            for col, value in conditions.items():
                if col == dim_name:
                    continue
                mask &= self.df_dims[col] == value
            options = self.df_dims.loc[mask, dim_name].unique().tolist()
        return options

    def get_default(self):
        dimensions = (d[0] for d in self.dimensions.values())
        return self.get_dimension(dimensions)

    def check_and_extract_key(self, indicators):
        short_name = None
        table = None
        for indicator in indicators:
            # Extract short_name
            short_name_ = indicator.dimensions["originalShortName"]
            assert isinstance(indicator.catalogPath, str), f"`catalogPath` is empty for variable {indicator.id}"
            # Extract table URI
            table_ = indicator.catalogPath.split("#")[0]

            if short_name is None:
                short_name = short_name_
            if table is None:
                table = table_

            # Checks
            assert short_name == short_name_, f"Short name mismatch: {short_name} != {short_name_}"
            assert table == table_, f"Table mismatch: {table} != {table_}"

        assert isinstance(table, str), "Table is empty"

        key = f"{table.replace('grapher/', '')}#{short_name}"
        return key

    def create_df(self):
        data = []
        for indicator in self.indicators:
            # Prepare data
            data_ = {
                "variable": indicator,
            }
            # Add dimensions, if any
            if "filters" in indicator.dimensions:
                for f in indicator.dimensions["filters"]:
                    data_[f["name"]] = f["value"]

            # Append to main list
            data.append(data_)

        # Build dataframe
        df = pd.DataFrame.from_records(data)
        columns_index = [col for col in df.columns if col != "variable"]
        df = df.set_index(columns_index)

        return df

    def get_dimensions(self):
        dimensions = {
            level: sorted(self.df.index.get_level_values(level).unique().tolist()) for level in self.df.index.names
        }
        return dimensions

    def get_dimension(self, dim_values):
        return self.df.loc[dim_values, "variable"]


class IndicatorsInCharts:
    """Mapping from indicators to charts."""

    def __init__(self, indicator_ids):
        # Get affected charts
        charts = find_charts_from_variable_ids(indicator_ids)
        # Map chart ID to chart object (1:1)
        self.chart_id_to_chart = {c.id: c for c in charts}
        # Map indicator ID to chart IDs (1:m)
        self.indicator_id_to_chart_ids = self._get_charts_by_indicator_id(charts)

    @classmethod
    def from_indicators(cls, indicators):
        indicator_ids = {v.id for v in indicators}
        return IndicatorsInCharts(indicator_ids)

    def _get_charts_by_indicator_id(self, charts):
        indicators_to_chart_ids = defaultdict(list)
        for c in charts:
            for dim in c.config["dimensions"]:
                indicators_to_chart_ids[dim["variableId"]].append(c.id)
        return indicators_to_chart_ids

    def get_charts(self, indicator_id):
        chart_ids = self.indicator_id_to_chart_ids[indicator_id]
        chart = [self.chart_id_to_chart[c] for c in chart_ids]
        return chart


class IndicatorsInExplorers:
    """Mapping from indicators to explorers."""

    def __init__(self, indicator_ids):
        # Get affected charts
        explorers = get_explorers(indicator_ids)
        # Map indicator ID to chart IDs (1:m)
        df = explorers.groupby("variableId")["explorerSlug"].unique().to_frame()
        self.indicator_id_to_explorer_slug = df["explorerSlug"].to_dict()
        self.indicator_id_to_explorer_slug = {k: list(v) for k, v in self.indicator_id_to_explorer_slug.items()}
        self.explorers = set(explorers["explorerSlug"].unique())

    @classmethod
    def from_indicators(cls, indicators):
        indicator_ids = {v.id for v in indicators}
        return IndicatorsInExplorers(indicator_ids)

    def get_explorers(self, indicator_id):
        explorer_slugs = self.indicator_id_to_explorer_slug.get(indicator_id)
        return explorer_slugs
