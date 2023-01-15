"""TODO:

- ChartVariableUpdater should have a List[VariableMetadata] field (and not VariableUpdater as now).
    - Probably should have a list with with metadata of new variables, but this migh already be encoded in VariableUpdate?
    - HOWEVER, based on bobbie's code, seems like only year ranges for variables to be updated are actually needed!
"""
from copy import deepcopy
from typing import Any, Dict, List

import pandas as pd
import simplejson as json
from MySQLdb import IntegrityError
from structlog import get_logger

from etl.chart_revision.chart import Chart
from etl.chart_revision.variables import VariablesUpdate
from etl.config import GRAPHER_USER_ID
from etl.db import get_connection, open_db

log = get_logger()


def get_charts_to_update(variable_mapping: Dict[int, int]) -> List[int]:
    # variables update
    variables_update = VariablesUpdate(variable_mapping)
    # get details on dimensions and chart IDs affected by the variable update
    chart_dimensions = _get_chart_dimensions_from_db(list(variable_mapping.keys()))
    chart_ids = list(set(c["chartId"] for c in chart_dimensions))
    # get details on charts affected by the variable update
    charts_raw = _get_charts_from_db(chart_ids)
    # build list with chart objects
    charts = []
    for chart_raw in charts_raw:
        # create chart instance
        chart = ChartVariableUpdateRevision(
            id=chart_raw["id"],
            config=json.loads(chart_raw["config"]),
            variables_update=variables_update,
        )
        # add chart to list
        charts.append(chart)
    return charts


class ChartVariableUpdateRevision:
    # ID of the chart to be updated
    id: int
    # Chart as it actual is at the moment (without update)
    chart: "Chart"
    # Mapping of old variable IDs to new variable IDs
    variables_update: "VariablesUpdate"
    # Chart as it should be after the update
    chart_new: "Chart" = None
    # Revision
    _revision: Dict[str, Any] = None

    def __init__(self, id: str, config: Dict[str, Any], variables_update: "VariablesUpdate"):
        self.id = int(id)
        self.chart = Chart(id, config)
        self.variables_update = self._get_relevant_variable_updates(variables_update)

    def __getitem__(self, item):
        if self.revision:
            return self.revision[item]
        else:
            raise ValueError(
                "No revision available! This might be because the chart is not affected by the variable update."
            )

    def _get_relevant_variable_updates(self, variables_update: "VariablesUpdate") -> "VariablesUpdate":
        """Get relevant variable updates.

        Given the complete list of variable updates, obtain the relevant ones for this chart.
        """
        return variables_update.slice(variable_ids=self.variables_used_in_old_chart)

    @property
    def variables_used_in_old_chart(self):
        """Get list of variable IDs used in old chart (all dimensions)."""
        variables_used = set(d["variableId"] for d in self.chart.config["dimensions"])
        if "map" in self.chart.config and "variableId" in self.chart.config["map"]:
            variables_used |= {self.chart.config["map"]["variableId"]}
        return variables_used

    @property
    def revision(self) -> Dict[str, Any]:
        if self._revision is None:
            if self.config_has_changed:
                self.chart_new.increase_version()
                self._revision = {
                    "chartId": self.id,
                    "originalConfig": self.chart.config_as_str,
                    "suggestedConfig": self.chart_new.config_as_str,
                    "suggested_reason": "Reason TBD",
                    "chartSlug": self.chart.config.get("slug"),
                }
        return self._revision

    @property
    def config_has_changed(self) -> bool:
        assert isinstance(self.chart_new, Chart), "Chart has not been updated yet! Run"
        return self.chart.config_as_str != self.chart_new.config_as_str

    def run(self) -> "Chart":
        """Get new chart config and set it to `chart_new` attribute."""
        self.chart_new = self.get_new_chart(deepcopy(self.chart))

    def get_new_chart(self, chart: Chart) -> Chart:
        chart = deepcopy(self.chart)
        chart = self._update_config_map(chart)
        chart = self._update_config_time(chart)
        chart = self._update_chart_dimensions(chart)
        chart = self._update_available_entities(chart)
        chart.check_fastt()
        return chart

    def _update_config_map(self, chart: "Chart") -> "Chart":
        """Update map parameters in chart."""
        # get old variable id
        old_var_id = self.chart.variable_id_map
        # update variable usage if it is needed
        if old_var_id is not None and old_var_id in self.variables_update.mapping:
            log.info(f"Chart {self.id}: Updating map")
            # Set new id
            new_var_id = self.variables_update.map(old_var_id)
            chart.config["map"]["variableId"] = new_var_id

            # get new year range
            old_range = self.variables_update.get_year_range(old_var_id)
            new_range = self.variables_update.get_year_range(new_var_id)

            # Update
            chart.update_map_time(old_range, new_range)
        else:
            log.info(f"Chart {self.id}: No need to update map")

        return chart

    def _update_config_time(self, chart: "Chart") -> "Chart":
        """modifies chart config maxTime and minTime,

        TODO: This function needs to be re-examined in detail. There are several things unclear to me:

            - What is the purpose of the time ranges?
            - Shouldn't charts have accesss to variable information? Like, which variable it contains, what are their min/max years, etc.
            - For the slider, shouldn't we just consider the time range of the x-axis variable?

        At least, we should aim at simplifying its logic. It is too complex and very hard to understand.
        """
        # get *all* variable IDs used in chart (even those that will not be updated)
        old_variable_ids = self.variables_update.ids_old
        # get updated IDs of variables in chart to be updated
        new_variable_ids = self.variables_update.ids_new

        # get year range for old and new variables
        # TODO: get_year_range is not a VariableUpdate method! It is a method at Variable level.
        old_range = self.variables_update.get_year_range(old_variable_ids)
        new_range = self.variables_update.get_year_range(new_variable_ids)
        # Is the min year hard-coded in the chart's title or subtitle?
        # If so, no update is done on this
        if chart.is_min_year_hardcoded or chart.is_max_year_hardcoded:
            log.info(
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
                        log.info(
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
                        log.info(
                            f"For chart {chart.id}, max year of new variable(s) < "
                            "max year of old variable(s). New variable(s): "
                            f"{new_variable_ids}"
                        )
                    chart.config["maxTime"] = max(new_range)
        return chart

    def _update_chart_dimensions(self, chart: "Chart") -> "Chart":
        """modifies each chart dimension (in both chart_dimensions and chart config)."""
        for dimension in chart.config["dimensions"]:
            if dimension["variableId"] in self.variables_update.mapping:
                dimension["variableId"] = self.variables_update.mapping[dimension["variableId"]]
        return chart

    def _update_available_entities(self, chart: "Chart") -> "Chart":
        chart.config["data"]["availableEntities"] = ["Spain"]
        return chart


def _get_charts_from_db(chart_ids: List[int]) -> List[Dict[str, Any]]:
    """Get list with chart data."""
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
        df = pd.read_sql(query, db_conn, params={"ids": chart_ids})
    return df.to_dict(orient="records")


def _get_chart_dimensions_from_db(variable_ids: List[int]) -> List[Dict[str, Any]]:
    """Get dataframe with chart dimensions."""
    # build query
    columns = ["id", "chartId", "variableId", "property", "order"]
    query = f"""
        SELECT {','.join([f'`{col}`' for col in columns])}
        FROM chart_dimensions
        WHERE variableId IN %(variables)s
    """
    # get data from table
    with get_connection() as db_conn:
        df = pd.read_sql(query, db_conn, params={"variables": variable_ids})
    return df.to_dict(orient="records")


def submit_revisions_to_grapher(revisions: List[ChartVariableUpdateRevision]):
    """Submit chart revisions to Grapher."""
    n_before = 0
    try:
        with open_db() as db:
            n_before = db.fetch_one("SELECT COUNT(id) FROM suggested_chart_revisions")[0]

            res = db.fetch_many(
                """
                SELECT *
                FROM (
                    SELECT chartId, COUNT(chartId) as c
                    FROM suggested_chart_revisions
                    WHERE status IN ("pending", "flagged")
                    GROUP BY chartId
                    ORDER BY c DESC
                    ) as grouped
                WHERE grouped.c > 1
            """
            )
            if len(res):
                raise RuntimeError(
                    "Two or more suggested chart revisions with status IN "
                    "('pending', 'flagged') share an identical chart id. These "
                    "must be resolved before inserting more suggested "
                    f"chart revisions. Affected chart IDs: {[r[0] for r in res]}"
                )

            tuples = []
            for revision in revisions:
                t = (
                    revision["chartId"],
                    revision["suggestedConfig"],
                    revision["originalConfig"],
                    revision["suggested_reason"],
                    "pending",
                    GRAPHER_USER_ID,
                )
                tuples.append(t)

            chart_ids = [t[0] for t in tuples]
            assert len(chart_ids) == len(set(chart_ids)), "`suggested_chart_revisions` contains duplicate chart ids."

            query = """
                INSERT INTO suggested_chart_revisions
                    (chartId, suggestedConfig, originalConfig, suggestedReason, status, createdBy, createdAt, updatedAt)
                VALUES
                    (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """
            db.upsert_many(query, tuples)

            # checks if any of the affected chartIds now has multiple
            # pending suggested revisions. If so, then rejects the whole
            # insert and tell the user which suggested chart revisions need
            # to be approved/rejected.
            res = db.fetch_many(
                f"""
                SELECT id, scr.chartId, c, createdAt
                FROM (
                    SELECT chartId, COUNT(chartId) as c
                    FROM suggested_chart_revisions
                    WHERE status IN ("pending", "flagged") AND chartId IN ({", ".join([str(_id) for _id in chart_ids])})
                    GROUP BY chartId
                    ORDER BY c DESC
                ) as grouped
                LEFT JOIN (
                    SELECT *
                    FROM suggested_chart_revisions
                    WHERE status IN ("pending", "flagged")
                ) as scr ON grouped.chartId = scr.chartId
                WHERE grouped.c > 1
                ORDER BY createdAt ASC
            """
            )
            if len(res):
                df = pd.DataFrame(res, columns=["id", "chart_id", "count", "created_at"])
                df["drop"] = df.groupby("chart_id")["created_at"].transform(lambda gp: gp == gp.max())
                df = df[~df["drop"]]
                # problem_chart_ids = [r[0] for r in res]
                s = ""
                for nm, gp in df.groupby("chart_id"):
                    s += f"Chart ID: {nm}. Suggested chart revision IDs: {gp['id'].tolist()}\n"
                raise RuntimeError(
                    "For one or more of the suggested chart revisions that you are"
                    " trying to insert, a suggested chart revision already exists"
                    " for the same chartId with status IN ('pending', 'flagged')."
                    " You must approve/reject these suggested chart revisions"
                    " before new suggested revisions for the same charts can be"
                    f" created. Affected charts:\n{s}"
                )
    except IntegrityError as e:
        e = (
            "INSERT operation into `suggested_chart_revisions` cancelled. "
            "Failed to insert suggested chart revisions because one or "
            "more of the suggested revisions that you are trying to insert "
            "already exists with an equivalent chartId, originalVersion, "
            f"suggestedVersion, and isPendingOrFlagged. Error: {e}"
        )
        raise IntegrityError(e)
    except Exception as e:
        log.info(f"INSERT operation into `suggested_chart_revisions` cancelled. Error: {e}")
        raise e
    finally:
        with open_db() as db:
            n_after = db.fetch_one("SELECT COUNT(id) FROM suggested_chart_revisions")[0]

        log.info(f"{n_after - n_before} of {len(revisions)} suggested chart revisions inserted.")
