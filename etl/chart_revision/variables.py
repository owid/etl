"""Module dealing with variables."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.error import URLError

import pandas as pd

from backport.datasync.data_metadata import variable_data_df_from_s3
from etl.db import get_engine

# Set to True when running experiments locally and want to avoid downloading data from S3.
# Instead of getting the actual data, dummy data is generated.
# This still triggers an error on the grapher side though.
DEBUG_NO_S3 = False
# Threshold among which we consider a change in a datapoint to be significant.
# It is given in percentage terms, i.e. 100 * (datapoint_new - datapoint_old) / datapoint_old.
THRESHOLD_MAJOR_CHANGE = 5


@dataclass
class VariablesUpdate:
    """Contains the details on the variable updates."""

    mapping: Dict[int, int]
    # Variables metadata for each variable (by variable ID)
    metadata: List["VariableMetadata"]
    _metadata_dix: Optional[Dict[int, "VariableMetadata"]] = None
    # Update summary
    _update_summary: List[Tuple[Any, Any, Any]] = field(default_factory=list)

    def __init__(
        self,
        mapping: Dict[int, int],
        metadata: Optional[List["VariableMetadata"]] = None,
        update_summary: Optional[List[Tuple[Any, Any, Any]]] = None,
    ):
        self.mapping = mapping
        var_data = None
        if metadata:
            self.metadata = metadata
        else:
            if DEBUG_NO_S3:
                self.metadata = self._get_metadata_from_db()
            else:
                var_data = self._get_var_data_from_db()
                self.metadata = self._get_metadata_from_db(var_data)
        if update_summary:
            self._update_summary = update_summary
        else:
            if DEBUG_NO_S3:
                self._update_summary = self._build_variables_update_summary()
            else:
                if var_data is None:
                    var_data = self._get_var_data_from_db()
                self._update_summary = self._build_variables_update_summary(var_data)

    @property
    def ids_old(self) -> List[int]:
        return list(set(self.mapping.keys()))

    @property
    def ids_new(self) -> List[int]:
        return list(set(self.mapping.values()))

    @property
    def ids_all(self) -> List[int]:
        return list(set(self.ids_old) | set(self.ids_new))

    @property
    def metadata_dix(self) -> Dict[int, "VariableMetadata"]:
        if not self._metadata_dix:
            self._metadata_dix = {m.id: m for m in self.metadata}
        return self._metadata_dix

    def get_metadata(self, variable_id: int) -> "VariableMetadata":
        if variable_id not in self.metadata_dix:
            raise ValueError(f"Variable ID {variable_id} is not a variable to be updated!")
        return self.metadata_dix[variable_id]

    def map(self, old_id: int):
        if old_id not in self.mapping:
            raise ValueError(f"Variable ID {old_id} is not a variable to be updated!")
        return self.mapping[old_id]

    def _get_var_data_from_db(self, recursion_counter: int = 0) -> pd.DataFrame:
        if recursion_counter >= 2:
            raise URLError("Failed to download data from S3")
        try:
            df = variable_data_df_from_s3(get_engine(), variable_ids=self.ids_all, workers=10)
        except URLError:
            df = self._get_var_data_from_db(recursion_counter + 1)
        return df

    def _get_metadata_from_db(self, var_data: Optional[pd.DataFrame] = None) -> List["VariableMetadata"]:
        """Get metadata for all variables in the update."""
        # get variable names
        query = "SELECT id, name FROM variables v WHERE id IN %(ids)s"
        df_varnames = pd.read_sql(query, get_engine(), params={"ids": self.ids_all})

        if var_data is not None:
            # get min and max year for each variable
            df_var_years = (
                var_data.groupby("variableId")
                .year.agg(["min", "max"])
                .rename(columns={"min": "minYear", "max": "maxYear"})
            )
            df_var_years = df_var_years.merge(df_varnames, left_index=True, right_on="id")
            df_var_years = df_var_years.set_index("id")
        else:
            df_var_years = df_varnames.assign(minYear=0, maxYear=3000)
            df_var_years = df_var_years.set_index("id")

        # build list of variable metadata
        metadata_raw = df_var_years.to_dict("index")
        metadata = []
        for k, v in metadata_raw.items():
            metadata.append(
                VariableMetadata(
                    id=k,
                    min_year=v["minYear"],
                    max_year=v["maxYear"],
                    name=v["name"],
                )
            )
        return metadata

    def slice(self, variable_ids: List[int]) -> "VariablesUpdate":
        """Slice with only variable updates specified by `variable_ids` (currently used variables)."""
        mapping = {k: v for k, v in self.mapping.items() if k in variable_ids}
        all_ids = list(set(variable_ids) | set(mapping.values()))
        # Select relevant update summaries
        update_summary = [s for s in self._update_summary if s[0] in variable_ids]
        return VariablesUpdate(
            mapping=mapping,
            metadata=[m for m in self.metadata if m.id in all_ids],
            update_summary=update_summary,
        )

    def get_year_range(self, variable_id_or_ids: Union[int, List[int]]):
        """Get year range for variable(s).

        If variable_id_or_ids is a single variable ID, it obtains the min and max year for that variable. Otherwise,
        if variable_id_or_ids is a list of variable IDs, then it obtains the lowest minimum year and the highest maximum year
        of all variables in the list.
        """
        if isinstance(variable_id_or_ids, int):
            # print(1)
            if variable_id_or_ids not in self.metadata_dix:
                raise ValueError(f"Variable ID {variable_id_or_ids} is not a variable to be updated!")
            var_meta = self.get_metadata(variable_id_or_ids)
            return [var_meta.min_year, var_meta.max_year]
        elif isinstance(variable_id_or_ids, list):
            # print(2)
            if not all([v in self.metadata_dix for v in variable_id_or_ids]):
                raise ValueError(f"Some (or all) variable IDs in {variable_id_or_ids} are not variables to be updated!")
            year_min = 3000
            year_max = -1
            for var_id in variable_id_or_ids:
                var_meta = self.get_metadata(var_id)
                year_min = min(year_min, var_meta.min_year)
                year_max = max(year_max, var_meta.max_year)
            return [year_min, year_max]

    def _build_variables_update_summary(self, var_data: Optional[pd.DataFrame] = None) -> List[Tuple[Any, Any, Any]]:
        """Find out differences between old and new variable.

        For each variable update, it checks:
            - Changes in datapoints of the variable.
            - New datapoints added to the variable.
            - Datapoints removed from the variable.
        """
        update_summary = []
        # Iterate over all variable mapping tupples.
        for old_var, new_var in self.mapping.items():
            if var_data is None:
                update_summary.append((old_var, new_var, "No access to S3. Hence could not compare variable values."))
            else:
                # Datavalues of old variable
                df_old = var_data[var_data["variableId"] == old_var].reset_index(drop=True)
                # Datavalues of new variable
                df_new = var_data[var_data["variableId"] == new_var].reset_index(drop=True)
                # Build summary for this variable pdate
                summary = self._build_variable_update_summary(df_old, df_new)
                update_summary.append((old_var, new_var, summary))
        return update_summary

    def _build_variable_update_summary(self, df_old: pd.DataFrame, df_new: pd.DataFrame) -> str:
        """Generate a summary with major differences between `df_old` and `df_new`.

        Parameters
        ----------
        df_old : pd.DataFrame
            Dataframe with datapoints from old variable.
        df_new : pd.DataFrame
            Dataframe with datapoints from new variable.
        """
        # Pre-format dataframes
        columns_ignore = ["variableId"]
        columns_to_match = [col for col in df_old.columns if col not in columns_ignore]
        df_old = df_old[columns_to_match].sort_values(["entityId", "year"]).reset_index(drop=True)
        df_new = df_new[columns_to_match].sort_values(["entityId", "year"]).reset_index(drop=True)
        # Just keep relevant columns: entityId, year and value
        columns_relevant = ["entityId", "year", "value"]
        df_old = df_old[columns_relevant]
        df_new = df_new[columns_relevant]

        # Full equivalence check
        if df_old.equals(df_new):
            return "No change"

        entities_mapping = _get_entities_mapping()
        summary = ""
        # Check if there are any changes in datapoints
        summary_changes = _summary_datapoint_changes(df_old, df_new, entities_mapping)
        if summary_changes:
            summary += summary_changes + "<br>"
        # Check if there are new datapoints or datapoints removed
        summary_added_removed = _summary_datapoint_added_and_removed(df_old, df_new, entities_mapping)
        if summary_added_removed:
            summary += summary_added_removed
        return summary

    def bake_summary_of_changes(self) -> str:
        """Bake a summary of changes in the variables update.

        HTML-friendly"""
        if self._update_summary is None:
            raise ValueError("Update summary is not available. Please run `build_variables_update_summary` first!")
        # Iterate over all variable mapping tupples and build summary
        summary = ""
        # print(self._update_summary)
        for old_var, new_var, summary_var in self._update_summary:
            old_var_name = self.get_metadata(old_var).name
            new_var_name = self.get_metadata(new_var).name
            summary += f"<h4>{old_var_name} ({old_var}) → {new_var_name} ({new_var})</h4>{summary_var}<br>"
        return summary


def _summary_datapoint_changes(df_old: pd.DataFrame, df_new: pd.DataFrame, entities_mapping: Dict[int, str]) -> str:
    """Generate a summary with major differences between `df_old` and `df_new`.

    This includes: number of datapoints that changed and substantially changed, mean/max/min of relative difference.
    """
    # Estimate difference in new values
    df_merged_inner = df_old.merge(df_new, how="inner", on=["entityId", "year"], suffixes=("_old", "_new"))
    df_merged_inner["value_diff"] = df_merged_inner["value_old"].astype(float) - df_merged_inner["value_new"].astype(
        float
    )
    df_merged_inner["value_diff_rel"] = (
        100 * df_merged_inner["value_diff"] / df_merged_inner["value_old"].astype(float)
    ).round(4)
    df_merged_inner.loc[
        (df_merged_inner["value_old"] == "0") & (df_merged_inner["value_new"] == "0"), "value_diff_rel"
    ] = 0
    df_merged_inner["value_diff_rel_abs"] = df_merged_inner["value_diff_rel"].abs()

    # Datapoints with different values
    msk = df_merged_inner["value_diff_rel_abs"] != 0
    num_different = len(df_merged_inner[msk])
    # Distribution of datapoints with different values
    diff_mean = df_merged_inner.loc[msk, "value_diff_rel_abs"].mean()
    diff_max = df_merged_inner.loc[msk, "value_diff_rel_abs"].max()
    diff_min = df_merged_inner.loc[msk, "value_diff_rel_abs"].min()

    # Datapoints with substantially different values
    msk_major = df_merged_inner["value_diff_rel_abs"] >= THRESHOLD_MAJOR_CHANGE
    num_different_major = len(df_merged_inner[msk_major])
    # Build summary table with major differences
    df_values_have_changed = df_merged_inner.loc[msk]
    df_values_have_changed = prettify_datavalues_df(df_values_have_changed, entities_mapping)

    if num_different >= 0:
        return f"""<b>Number datapoints that changed:</b> {num_different}<br>
            Avg difference (abs): {diff_mean}%<br>
            Max difference (abs): {diff_max}%<br>
            Min difference (abs): {diff_min}%<br>
        <b>Number datapoints that substantially changed (≥{THRESHOLD_MAJOR_CHANGE}%):</b> {num_different_major}<br><br>
        """
        # Datapoints that changed:
        # {df_values_have_changed.to_html(index=False)}
        # """
    return ""


def _summary_datapoint_added_and_removed(
    df_old: pd.DataFrame, df_new: pd.DataFrame, entities_mapping: Dict[int, str]
) -> str:
    # Find out Missing/new datapoints
    df_merged_outer = df_old.merge(df_new, how="outer", on=["entityId", "year"], suffixes=("_old", "_new"))

    # number of datapoints added
    num_new_values = df_merged_outer["value_old"].isna().sum()
    # num_new_values_rel = (100*num_new_values/len(df_old)).round(2)  # equivalent percentage of old variable's datapoints that have been added
    # number of datapoints removed from the old variable
    num_missing_values = df_merged_outer["value_new"].isna().sum()
    # num_missing_values_rel = (100*num_missing_values/len(df_old)).round(2)  # percentage of old variable's datapoints that were removed

    # Build summary table with missing/new datapoints
    # df_new_values = prettify_datavalues_df(df_merged_outer[df_merged_outer["value_old"].isna()], entities_mapping)
    # df_lost_values = prettify_datavalues_df(df_merged_outer[df_merged_outer["value_new"].isna()], entities_mapping)

    if (num_new_values == 0) and (num_missing_values == 0):
        return ""

    return f"""<b>Number datapoints in old variable:</b> {len(df_old)}<br>
    <b>Number datapoints in new variable:</b> {len(df_new)} (= {len(df_old)} - {num_missing_values} + {num_new_values})
    """
    # New datapoints (present in new variable but not in old variable)):
    # {df_new_values.to_html(index=False)}

    # Missing datapoints (present in old variable but not in new variable)):
    # {df_lost_values.to_html(index=False)}
    # """


def prettify_datavalues_df(df: pd.DataFrame, entities_mapping: Dict[int, str]) -> pd.DataFrame:
    df["entity"] = df["entityId"].map(entities_mapping)
    if "value_diff_rel_abs" in df.columns:
        df = df.sort_values("value_diff_rel_abs")
        return df[["entity", "year", "value_diff_rel"]]
    df = df.sort_values(["entity", "year"])
    return df[["entity", "year"]]


def _get_entities_mapping() -> Dict[int, str]:
    query = "SELECT id, name FROM entities"
    # get data from db
    df = pd.read_sql(query, get_engine())
    # Dataframe to dictionary
    dix = df.set_index("id")["name"].to_dict()
    return dix


@dataclass
class VariableMetadata:
    """Wrapper around variable metadata."""

    id: int
    min_year: int
    max_year: int
    name: str
