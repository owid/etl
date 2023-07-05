"""[This submodule will be deprecated, replaced by etl.chart_revision.cli]

Generate chart revisions in Grapher using MAPPING_FILE JSON file.

MAPPING_FILE is a JSON file with old_variable_id -> new_variable_id pairs. E.g. {2032: 147395, 2033: 147396, ...}.

Make sure that you are connected to the database. By default, it connects to Grapher based on the environment file found in the project's root directory `path/to/etl/.env`.
"""
# The original script was originally from the owid/importers repo: https://github.com/owid/importers/blob/master/standard_importer/chart_revision_suggester.py

import os
import re
import traceback
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple, Union, cast

import pandas as pd
import simplejson as json
import structlog
from MySQLdb import IntegrityError
from tqdm import tqdm

from etl.config import DEBUG, GRAPHER_USER_ID
from etl.db import open_db
from etl.grapher_helpers import IntRange

log = structlog.get_logger()
MSGTypes = Literal["error", "warning", "info", "success"]

# The maximum length of the suggested revision reason can't exceed the maximum length specified by the datatype "suggestedReason" in grapher.suggested_chart_revisions table.
SUGGESTED_REASON_MAX_LENGTH = 512


class ChartRevisionSuggester:
    """Implements methods for suggesting revisions to one or more
    charts, to be approved using the chart approval tool.

    This class is intended for use after a new dataset has been imported
    into the MySQL database and you wish to update the corresponding
    OWID charts to display the newly available data in place of the old data.

    Attributes:
        old_var_id2new_var_id: Dict[int, int]: Dictionary with mappings of old variable IDs to new variable IDs.
                                Example: {2032: 147395, 2033: 147396, ...}
        logs: List[Dict[str, str]]. List of messages to be reported to the user.
        revision_reason: str. Reason for the suggested chart revisions. If none is provided, an automated reason is generated (recommended).
        var_id2year_range: Dict[int, List[int]]: TODO
        df_charts: pd.DataFrame: TODO
        df_chart_dims: pd.DataFrame: TODO

    Usage:
        >>> from etl.chart_revision_suggester import ChartRevisionSuggester
        >>> mapping_filepath = "file/to/variable_mapping.json"
        >>> suggester = ChartRevisionSuggester.from_json(mapping_filepath)
        >>> suggester.suggest()
    """

    def __init__(self, variable_mapping: Dict[int, int], revision_reason: Optional[str] = None):
        """

        Parameters
        ----------
        variable_mapping : Dict[int, int]
            Dictionary with mappings of old variable IDs to new variable IDs. Example: {2032: 147395, 2033: 147396, ...}
        revision_reason : str, optional
            Reason for the suggested chart revisions. If none is provided, an automated reason is generated (recommended). by default None
        """
        self._sanity_check_mapping_dict(variable_mapping)
        self.old_var_id2new_var_id = variable_mapping
        self.logs: List[Dict[str, str]] = []
        self.revision_reason = revision_reason

        # Initiate some variables
        self.var_id2year_range = self._get_variable_year_ranges()
        self.df_charts, self.df_chart_dims, _ = self._get_charts_from_old_variables()

    def _sanity_check_mapping_dict(self, variable_mapping: Any) -> None:
        """Sanity check the mapping dictionary."""
        if not isinstance(variable_mapping, dict):
            raise TypeError(
                f"The variable mapping dictionary must be a dictionary. Found type '{type(variable_mapping)}'!"
            )
        for k, v in variable_mapping.items():
            if not isinstance(k, int):
                raise TypeError(f"The keys of the variable mapping dictionary must be integers. Found key '{k}'!")
            if not isinstance(v, int):
                raise TypeError(f"The values of the variable mapping dictionary must be integers. Found value '{v}'!")

    @classmethod
    def from_json(cls, filepath: str, revision_reason: Optional[str] = None) -> "ChartRevisionSuggester":
        """Load a ChartRevisionSuggester instance from a JSON file.

        Parameters
        ----------
        filepath : str
            Mapping file old_variable_id -> new_variable_id pairs. E.g. {2032: 147395, 2033: 147396, ...}.
        """
        try:
            with open(os.path.join(filepath), "r") as f:
                variable_mapping = {int(k): int(v) for k, v in json.load(f).items()}
        except FileNotFoundError:
            raise FileNotFoundError(f"Variable mapping file was not found at {filepath}.")
        return cls(variable_mapping=variable_mapping, revision_reason=revision_reason)

    @property
    def status(self) -> str:
        return "pending"

    def suggest(self, *args: Any, **kwargs: Any) -> None:
        """Prepare suggestions and submit them to Grapher for the Approval tool."""
        kwargs["suggested_chart_revisions"] = self.prepare_charts()
        self.insert(*args, **kwargs)

    def load_variable_replacements(self, filepath: str) -> Dict[int, int]:
        try:
            with open(os.path.join(filepath), "r") as f:
                data = {int(k): int(v) for k, v in json.load(f).items()}
        except FileNotFoundError:
            raise FileNotFoundError(f"Variable mapping file was not found at {filepath}.")
        return data

    def prepare_charts(self) -> List[dict[str, Any]]:
        suggested_chart_revisions = []
        for row in tqdm(self.df_charts.itertuples(), total=self.df_charts.shape[0]):
            revision = self.prepare_chart_single(row)
            if revision:
                suggested_chart_revisions.append(revision)
        return suggested_chart_revisions

    def prepare_chart_single(self, row) -> Union[Dict[str, Union[int, str]], None]:
        row = cast(Any, row)
        try:
            chart_id = row.id
            # retrieves chart dimensions to be updated.
            chart_dims = self.df_chart_dims.loc[self.df_chart_dims["chartId"] == chart_id].to_dict(orient="records")
            chart_dims_orig = deepcopy(chart_dims)
            chart_config = json.loads(row.config)

            # get list with new variables
            try:
                new_variables = [self.old_var_id2new_var_id[c["variableId"]] for c in chart_dims]
            except KeyError:
                raise KeyError("Problem found in self.old_var_id2new_var_id! some IDs are not in the mapping dict.")

            self._modify_chart_config_map(chart_config)
            self._modify_chart_config_time(chart_id, chart_config)
            self._check_chart_config_fastt(chart_id, chart_config)

            self._modify_chart_dimensions(chart_dims, chart_config)

            config_has_changed = json.dumps(chart_config, ignore_nan=True) != row.config
            dims_have_changed = any([dim != chart_dims_orig[i] for i, dim in enumerate(chart_dims)])
            assert config_has_changed == dims_have_changed, (
                f"Chart {chart_id}: Chart config and chart dimensions must "
                "have either BOTH changed or NEITHER changed, but only "
                "one has changed. Something went wrong."
            )
            if config_has_changed:
                # update version
                # if 'version' in chart_config and isinstance(chart_config['version'], int):
                chart_config["version"] += 1
                chart_config_str = json.dumps(chart_config, ignore_nan=True)
                suggested_chart_revision = {
                    "chartId": chart_id,
                    "originalConfig": row.config,
                    "suggestedConfig": chart_config_str,
                    "suggested_reason": self._get_chart_update_reason(new_variables),
                    "chartSlug": json.loads(row.config).get("slug"),
                }
                return suggested_chart_revision

        except Exception as e:
            self.report_error(f"Error encountered for chart {row.id}: {e}")
            if DEBUG:
                traceback.print_exc()

    def _get_chart_update_reason(self, variable_ids: List[int]) -> str:
        """Get the reason for the chart update.

        Accesses DB and finds out the name of the recently added dataset with the new variables."""
        try:
            with open_db() as db:
                if len(variable_ids) == 1:
                    results = db.fetch_many(
                        f"""
                        SELECT variables.name, datasets.name, datasets.version FROM datasets
                            JOIN variables ON datasets.id = variables.datasetId
                            WHERE variables.id IN ({variable_ids[0]})
                        """
                    )
                else:
                    results = db.fetch_many(
                        f"""
                        SELECT variables.name, datasets.name, datasets.version FROM datasets
                            JOIN variables ON datasets.id = variables.datasetId
                            WHERE variables.id IN {*variable_ids,}
                        """
                    )
        except Exception:
            self.report_error(
                "Problem found when accessing the DB trying to get details on the newly added variables"
                f" {variable_ids}. Therefore, no reason for suggested chart revision could be stablished!"
            )
            reason = "No reason could be found for this suggested chart revision! Please check with the devs/data team!"
        else:
            datasets = sorted(set(result[1] for result in results))
            # extended reason (might overflow `suggestedReason` datatype length limitation)
            # reason = [f"'{result[1]}' dataset update (variable '{result[0]}')" for result in results]
            if len(datasets) == 1:
                reason = f"Bulk update: {datasets[0]}"
            else:
                reason = "Bulk updates:" + "; ".join(datasets)

        # check length
        if len(reason) > SUGGESTED_REASON_MAX_LENGTH:
            reason = reason[:SUGGESTED_REASON_MAX_LENGTH]

        return reason

    def insert(self, suggested_chart_revisions: List[dict[str, Any]]) -> None:
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
                for rev in suggested_chart_revisions:
                    t = (
                        int(rev["chartId"]),
                        rev["suggestedConfig"],
                        rev["originalConfig"],
                        rev["suggested_reason"],
                        self.status,
                        GRAPHER_USER_ID,
                    )
                    tuples.append(t)

                chart_ids = [t[0] for t in tuples]
                assert len(chart_ids) == len(
                    set(chart_ids)
                ), "`suggested_chart_revisions` contains duplicate chart ids."

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
            self.report_error(f"INSERT operation into `suggested_chart_revisions` cancelled. Error: {e}")
            raise e
        finally:
            with open_db() as db:
                n_after = db.fetch_one("SELECT COUNT(id) FROM suggested_chart_revisions")[0]

            self.report_info(
                f"{n_after - n_before} of {len(suggested_chart_revisions)} suggested chart revisions inserted."
            )

    def _get_charts_from_old_variables(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """retrieves all charts, chart_dimensions, and chart_revisions rows
        for old variables.

        Returns:

            (df_charts,
             df_chart_dimensions,
             df_chart_revisions): Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame].
                df_charts: dataframe of charts rows.
                df_chart_dimensions: dataframe of chart_dimensions rows.
                df_chart_revisions: dataframe of chart_revisions rows.
        """
        with open_db() as db:
            # retrieves chart_dimensions
            variable_ids = list(self.old_var_id2new_var_id.keys())
            variable_ids_str = ",".join([str(_id) for _id in variable_ids])
            columns = ["id", "chartId", "variableId", "property", "order"]
            rows = db.fetch_many(
                f"""
                SELECT {','.join([f'`{col}`' for col in columns])}
                FROM chart_dimensions
                WHERE variableId IN ({variable_ids_str})
            """
            )
            df_chart_dimensions = pd.DataFrame(rows, columns=columns)

            # retrieves charts
            chart_ids_str = ",".join(
                [str(_id) for _id in df_chart_dimensions["chartId"].unique().tolist()]  # type: ignore
            )
            columns = [
                "id",
                "config",
                "createdAt",
                "updatedAt",
                "lastEditedAt",
                "publishedAt",
            ]
            rows = db.fetch_many(
                f"""
                SELECT {','.join(columns)}
                FROM charts
                WHERE id IN ({chart_ids_str})
            """
            )
            df_charts = pd.DataFrame(rows, columns=columns)

            # retrieves chart_revisions
            columns = ["id", "chartId", "userId", "config", "createdAt", "updatedAt"]
            rows = db.fetch_many(
                f"""
                SELECT {','.join(columns)}
                FROM chart_revisions
                WHERE chartId IN ({chart_ids_str})
            """
            )
            df_chart_revisions = pd.DataFrame(rows, columns=columns)
        return df_charts, df_chart_dimensions, df_chart_revisions

    def _get_variable_year_ranges(self) -> Dict[int, List[int]]:
        with open_db() as db:
            all_var_ids = list(self.old_var_id2new_var_id.keys()) + list(self.old_var_id2new_var_id.values())
            variable_ids_str = ",".join([str(_id) for _id in all_var_ids])
            raise NotImplementedError("data_values was deprecated")
            rows = db.fetch_many(
                f"""
                SELECT variableId, MIN(year) AS minYear, MAX(year) AS maxYear
                FROM data_values
                WHERE variableId IN ({variable_ids_str})
                GROUP BY variableId
            """
            )
            var_id2year_range = {}
            for variable_id, min_year, max_year in rows:
                var_id2year_range[variable_id] = [min_year, max_year]
        return var_id2year_range

    def _modify_chart_config_map(self, chart_config: dict[Any, Any]) -> None:
        """modifies chart config map."""
        old_var_id = None
        if "map" in chart_config:
            if "variableId" in chart_config["map"]:
                old_var_id = chart_config["map"]["variableId"]
            elif len(chart_config["dimensions"]) == 1:
                old_var_id = chart_config["dimensions"][0]["variableId"]

        if old_var_id is not None and old_var_id in self.old_var_id2new_var_id:
            new_var_id = self.old_var_id2new_var_id[old_var_id]
            chart_config["map"]["variableId"] = new_var_id
            old_range = self._vars_to_range([old_var_id])
            new_range = self._vars_to_range([new_var_id])

            # update targetYear
            if "targetYear" in chart_config["map"]:
                if pd.notnull(new_range.min) and chart_config["map"]["targetYear"] == old_range.min:
                    chart_config["map"]["targetYear"] = new_range.min
                elif pd.notnull(new_range.max):
                    chart_config["map"]["targetYear"] = new_range.max

            # update time
            if "time" in chart_config["map"]:
                if pd.notnull(new_range.min) and chart_config["map"]["time"] == old_range.min:
                    chart_config["map"]["time"] = new_range.min
                elif pd.notnull(new_range.max):
                    chart_config["map"]["time"] = new_range.max

    def _modify_chart_config_time(self, chart_id: int, chart_config: dict[Any, Any]) -> None:
        """modifies chart config maxTime and minTime"""
        old_variable_ids = set([dim["variableId"] for dim in chart_config["dimensions"]])
        if "map" in chart_config and "variableId" in chart_config["map"]:
            old_variable_ids.add(chart_config["map"]["variableId"])

        new_variable_ids = [
            self.old_var_id2new_var_id[_id] for _id in old_variable_ids if _id in self.old_var_id2new_var_id
        ]

        old_range = self._vars_to_range(old_variable_ids)
        new_range = self._vars_to_range(new_variable_ids)

        # Is the min year hard-coded in the chart's title or subtitle?
        min_year_hardcoded = self._is_min_year_hardcoded(chart_config)
        max_year_hardcoded = self._is_max_year_hardcoded(chart_config)
        if min_year_hardcoded or max_year_hardcoded:
            title = chart_config.get("title")
            subtitle = chart_config.get("subtitle")
            min_time = chart_config.get("minTime")
            max_time = chart_config.get("maxTime")
            self.report_warning(
                f"Chart {chart_id} title or subtitle may contain a hard-coded "
                "year, so the minTime and maxTime fields will not be changed."
                f"\nTitle: {title}"
                f"\nSubtitle: {subtitle}"
                f"\nminTime: {min_time}; maxTime: {max_time}"
            )
        else:
            times_are_eq = self._is_single_time(
                min_time=chart_config.get("minTime"),
                max_time=chart_config.get("maxTime"),
                min_time_old=old_range.min,
                max_time_old=old_range.max,
            )
            if times_are_eq:
                use_min_year = (
                    chart_config["minTime"] == "earliest"
                    or chart_config["maxTime"] == "earliest"
                    or (
                        pd.api.types.is_numeric_dtype(chart_config["minTime"])  # type: ignore
                        and pd.api.types.is_numeric_dtype(chart_config["maxTime"])  # type: ignore
                        and abs(chart_config["minTime"] - old_range.min) < abs(chart_config["maxTime"] - old_range.max)
                    )
                )
                if use_min_year:
                    chart_config["minTime"] = new_range.min
                    chart_config["maxTime"] = new_range.min
                else:
                    chart_config["minTime"] = new_range.max
                    chart_config["maxTime"] = new_range.max
            else:
                replace_min_time = (
                    "minTime" in chart_config and chart_config["minTime"] != "earliest" and pd.notnull(new_range.min)
                )
                if replace_min_time:
                    if pd.notnull(old_range.min) and (new_range.min > old_range.min):
                        self.report_warning(
                            f"For chart {chart_id}, min year of new variable(s) > "
                            "min year of old variable(s). New variable(s): "
                            f"{new_variable_ids}"
                        )
                    chart_config["minTime"] = new_range.min
                replace_max_time = (
                    "maxTime" in chart_config and chart_config["maxTime"] != "latest" and pd.notnull(new_range.max)
                )
                if replace_max_time:
                    if pd.notnull(old_range.max) and (new_range.max < old_range.max):
                        self.report_warning(
                            f"For chart {chart_id}, max year of new variable(s) < "
                            "max year of old variable(s). New variable(s): "
                            f"{new_variable_ids}"
                        )
                    chart_config["maxTime"] = new_range.max

    def _check_chart_config_fastt(self, chart_id: int, chart_config: dict[Any, Any]) -> None:
        """modifies chart config FASTT.

        update/check text fields: slug, note, title, subtitle, sourceDesc.
        """
        if "title" in chart_config and re.search(r"\b\d{4}\b", chart_config["title"]):
            self.report_warning(
                f"Chart {chart_id} title may have a hard-coded year in it that "
                f'will not be updated: "{chart_config["title"]}"'
            )
        if "subtitle" in chart_config and re.search(r"\b\d{4}\b", chart_config["subtitle"]):
            self.report_warning(
                f"Chart {chart_id} subtitle may have a hard-coded year in it "
                f'that will not be updated: "{chart_config["subtitle"]}"'
            )
        if "note" in chart_config and re.search(r"\b\d{4}\b", chart_config["note"]):
            self.report_warning(
                f"Chart {chart_id} note may have a hard-coded year in it that "
                f'will not be updated: "{chart_config["note"]}"'
            )
        if re.search(r"\b\d{4}\b", chart_config["slug"]):
            self.report_warning(
                f"Chart {chart_id} slug may have a hard-coded year in it that "
                f'will not be updated: "{chart_config["slug"]}"'
            )

    def _modify_chart_dimensions(self, chart_dimensions: List[dict[str, Any]], chart_config: dict[Any, Any]) -> None:
        """modifies each chart dimension (in both chart_dimensions and chart config)."""
        for dim in chart_dimensions:
            if dim["variableId"] in self.old_var_id2new_var_id:
                dim["variableId"] = self.old_var_id2new_var_id[dim["variableId"]]
                config_dim = chart_config["dimensions"][dim["order"]]
                config_dim["variableId"] = self.old_var_id2new_var_id[config_dim["variableId"]]

    def _vars_to_range(self, _ids: Iterable[int]) -> IntRange:
        years = []
        for _id in _ids:
            if _id in self.var_id2year_range:
                years += self.var_id2year_range[_id]
        if not years:
            raise ValueError(f"No year range was found because of variables {_ids}")
        return IntRange.from_values(years)

    def _is_min_year_hardcoded(self, chart_config: dict[Any, Any]) -> bool:
        min_year_hardcoded = (
            "minTime" in chart_config
            and "title" in chart_config
            and bool(re.search(rf"{chart_config['minTime']}", chart_config["title"]))
        ) or (
            "minTime" in chart_config
            and "subtitle" in chart_config
            and bool(re.search(rf"{chart_config['minTime']}", chart_config["subtitle"]))
        )
        return min_year_hardcoded

    def _is_max_year_hardcoded(self, chart_config: dict[Any, Any]) -> bool:
        # Is the min year hard-coded in the chart's title or subtitle?
        max_year_hardcoded = (
            "maxTime" in chart_config
            and "title" in chart_config
            and bool(re.search(rf"{chart_config['maxTime']}", chart_config["title"]))
        ) or (
            "maxTime" in chart_config
            and "subtitle" in chart_config
            and bool(re.search(rf"{chart_config['maxTime']}", chart_config["subtitle"]))
        )
        return max_year_hardcoded

    def _is_single_time(
        self,
        min_time: Optional[Union[int, Literal["earliest", "latest"]]],
        max_time: Optional[Union[int, Literal["earliest", "latest"]]],
        min_time_old: int,
        max_time_old: int,
    ) -> bool:
        times_are_eq = (
            min_time is not None
            and max_time is not None
            and (
                (min_time == max_time)
                or (min_time == "earliest" and max_time == min_time_old)
                or (min_time == min_time_old and max_time == "earliest")
                or (min_time == max_time_old and max_time == "latest")
                or (min_time == "latest" and max_time == max_time_old)
            )
        )
        return times_are_eq

    def report_error(self, msg: str):
        self.report_msg(msg, "error")

    def report_warning(self, msg: str):
        self.report_msg(msg, "warning")

    def report_info(self, msg: str):
        self.report_msg(msg, "info")

    def report_success(self, msg: str):
        self.report_msg(msg, "success")

    def report_msg(self, msg: str, type: MSGTypes):
        if type == "error":
            log.error(msg)
        elif type == "warning":
            log.warning(msg)
        elif type == "info":
            log.info(msg)
        elif type == "success":
            log.info(msg)
        else:
            raise ValueError(f"Invalid type: {type}")
        self.logs.append({"message": msg, "type": type})
