from typing import Dict, List, Any
import pandas as pd
from etl.db import get_connection
import json
from copy import deepcopy
from etl.chart_revision.variables import Variables
from etl.chart_revision.chart import Chart, ReviewedChart
from structlog import get_logger


log = get_logger()


class ChartReviewer:
    _charts: Dict[int, Chart] = None

    def __init__(self, mapping: Dict[int, int]):
        self.variables = Variables(mapping)

    @property
    def charts(self) -> Dict[int, "Chart"]:
        """Charts to be reviewed (as a dictionary).

        Returns:
            Dict[int, Chart]: Dictionary with chart ids as keys and Chart objects as values.
        """
        if not self._charts:
            self._charts = self.get_charts()
        return self._charts

    @property
    def charts_list(self) -> List["Chart"]:
        """Charts to be reviewed as a list.

        Returns:
            List[Chart]: List of Chart objects.
        """
        return list(self.charts.values())

    @property
    def charts_ids(self) -> List[int]:
        """IDs of the charts to be reviewed."""
        return list(self.charts.keys())

    @property
    def num_charts(self) -> int:
        """Number of charts to be reviewed."""
        return len(self.charts)

    def get_charts(self) -> Dict[int, "Chart"]:
        """Get the charts that have to be reviewed."""
        # Get raw chart data (dataframe)
        df_charts_dims = get_df_chart_dims(self.variables.ids_old)
        chart_ids = list(set(df_charts_dims["chartId"]))
        df_charts = get_df_charts(chart_ids)
        # Build dictionary with Chart objects
        charts = {}
        for row in df_charts.iterrows():
            chart_id = row[1]["id"]
            charts[chart_id] = Chart(
                id=chart_id,
                config=json.loads(row[1]["config"]),
                # dimensions=df_charts_dims.loc[df_charts_dims["chartId"] == chart_id].to_dict(orient="records"),
            )
        return charts

    def update_charts(self):
        """Review and update charts."""
        for chart in self.charts_list:
            self._charts[chart.id] = self.update_chart(chart)

    def update_chart(self, chart: Chart) -> ReviewedChart:
        """Review and update chart."""
        chart_reviewed = self._modify_chart_config_map(deepcopy(chart))
        chart_reviewed = self._modify_chart_config_time(chart_reviewed)
        chart_reviewed = self._check_fastt(chart)
        return chart_reviewed

    def _modify_chart_config_map(self, chart: Chart):
        """Update map parameters in chart."""
        # get old variable id
        old_var_id = chart.variable_id_map
        # update variable usage if it is needed
        if old_var_id is not None and old_var_id in self.variables.mapping:
            log.info(f"Chart {chart.id}: Updating map")
            # Set new id
            new_var_id = self.variables.mapping[old_var_id]
            chart.config["map"]["variableId"] = new_var_id

            # get new year range
            old_range = self.variables.get_year_range(old_var_id)
            new_range = self.variables.get_year_range(new_var_id)

            # Update
            chart.update_map_time(old_range, new_range)
        else:
            log.info(f"Chart {chart.id}: No need to update map")
        return chart

    def _modify_chart_config_time(self, chart: Chart) -> None:
        """modifies chart config maxTime and minTime,

        TODO: This function needs to be re-examined in detail. There are several things unclear to me:

            - What is the purpose of the time ranges?
            - Shouldn't charts have accesss to variable information? Like, which variable it contains, what are their min/max years, etc.
            - For the slider, shouldn't we just consider the time range of the x-axis variable?

        At least, we should aim at simplifying its logic. It is too complex and very hard to understand.
        """
        # get all current variables IDs used in chart
        old_variable_ids = chart.variable_ids
        # get equivalent updated variable IDs in chart
        new_variable_ids = [self.variables.mapping[_id] for _id in old_variable_ids if _id in self.variables.mapping]

        # get year range for old and new variables
        old_range = self.variables.get_year_range(old_variable_ids)
        new_range = self.variables.get_year_range(new_variable_ids)

        # Is the min year hard-coded in the chart's title or subtitle?
        # If so, no update is done on this
        if chart.is_min_year_hardcoded or chart.is_max_year_hardcoded:
            self.report_warning(
                f"Chart {chart.id} title or subtitle may contain a hard-coded "
                "year, so the minTime and maxTime fields will not be changed."
                f"\nTitle: {chart.config.get('title')}"
                f"\nSubtitle: {chart.config.get('subtitle')}"
                f"\nminTime: {chart.config.get('minTime')}; maxTime: {chart.config.get('maxTime')}"
            )
        else:
            # TODO: How could we access variable's year range from Chart object? Need to have "Variable" attributes?
            times_are_eq = chart.is_single_time(
                var_min_time=min(old_range),
                var_max_time=max(old_range),
            )
            if times_are_eq:
                # Check if used year is min year or max year
                use_min_year = chart.single_time_earliest(min(old_range), max(old_range))
                if use_min_year:
                    chart.config["minTime"] = min(new_range)
                    chart.config["maxTime"] = min(new_range)
                else:
                    chart.config["minTime"] = max(new_range)
                    chart.config["maxTime"] = max(new_range)
            else:
                # Check if minTime needs to be updated
                replace_min_time = (
                    "minTime" in chart.config and chart.config["minTime"] != "earliest" and pd.notnull(min(new_range))
                )
                if replace_min_time:
                    if pd.notnull(min(old_range)) and (min(new_range) > min(old_range)):
                        self.report_warning(
                            f"For chart {chart.id}, min year of new variable(s) > "
                            "min year of old variable(s). New variable(s): "
                            f"{new_variable_ids}"
                        )
                    chart.config["minTime"] = min(new_range)
                # Check if maxTime needs to be updated
                replace_max_time = (
                    "maxTime" in chart.config and chart.config["maxTime"] != "latest" and pd.notnull(max(new_range))
                )
                if replace_max_time:
                    if pd.notnull(max(old_range)) and (max(new_range) < max(old_range)):
                        self.report_warning(
                            f"For chart {chart.id}, max year of new variable(s) < "
                            "max year of old variable(s). New variable(s): "
                            f"{new_variable_ids}"
                        )
                    chart.config["maxTime"] = max(new_range)
        return chart

    def _check_fastt(self, chart: Chart):
        reports = chart.check_fastt()
        if reports:
            for report in reports:
                self.report_warning(report)


def get_df_charts(chart_ids: List[int]) -> pd.DataFrame:
    """Get dataframe with chart data."""
    # build query
    columns = [
        "id",
        "config",
        "createdAt",
        "updatedAt",
        "lastEditedAt",
        "publishedAt",
    ]
    query = f"""
        SELECT {','.join([f'`{col}`' for col in columns])}
        FROM charts
        WHERE id IN %(ids)s
    """
    # get data from db
    with get_connection() as db_conn:
        return pd.read_sql(query, db_conn, params={"ids": chart_ids})


def get_df_chart_dims(variable_ids: List[int]) -> pd.DataFrame:
    """Get dataframe with chart dimensions."""
    # build query
    columns = ["id", "chartId", "variableId", "property", "order"]
    query = f"""
        SELECT {','.join([f'`{col}`' for col in columns])}
        FROM chart_dimensions
        WHERE variableId IN %(variables)s
    """
    print(query)
    print(variable_ids)
    # get data from table
    with get_connection() as db_conn:
        return pd.read_sql(query, db_conn, params={"variables": variable_ids})
