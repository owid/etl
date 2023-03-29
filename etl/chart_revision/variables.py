from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from backport.datasync.data_metadata import variable_data_df_from_s3
from etl.db import get_engine


# Set to True when running experiments locally and want to avoid downloading data from S3.
# Instead of getting the actual data, dummy data is generated.
DEBUG_NO_S3 = False


@dataclass
class VariablesUpdate:
    """Contains the details on the variable updates."""

    mapping: Dict[int, int]
    # Variables metadata for each variable (by variable ID)
    metadata: List["VariableMetadata"]
    _metadata_dix: Optional[Dict[int, "VariableMetadata"]] = None

    def __init__(self, mapping: Dict[int, int], metadata: Optional[List["VariableMetadata"]] = None):
        self.mapping = mapping
        if metadata:
            self.metadata = metadata
        else:
            self.metadata = self._get_metadata_from_db()

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

    def _get_metadata_from_db(self) -> List["VariableMetadata"]:
        """Get metadata for all variables in the update."""
        if DEBUG_NO_S3:
            return [
                VariableMetadata(
                    id=i,
                    min_year=1900,
                    max_year=2020,
                )
                for i in self.ids_all
            ]
        # get data from S3
        df_var_years = variable_data_df_from_s3(get_engine(), variable_ids=self.ids_all, workers=10)

        # get min and max year for each variable
        df_var_years = (
            df_var_years.groupby("variableId")
            .year.agg(["min", "max"])
            .rename(columns={"min": "minYear", "max": "maxYear"})
        )

        # build list of variable metadata
        metadata_raw = df_var_years.to_dict("index")
        metadata = []
        for k, v in metadata_raw.items():
            metadata.append(
                VariableMetadata(
                    id=k,
                    min_year=v["minYear"],
                    max_year=v["maxYear"],
                )
            )
        return metadata

    def slice(self, variable_ids: List[int]) -> "VariablesUpdate":
        """Slice with only variable updates specified by `variable_ids` (currently used variables)."""
        mapping = {k: v for k, v in self.mapping.items() if k in variable_ids}
        all_ids = list(set(variable_ids) | set(mapping.values()))
        return VariablesUpdate(
            mapping=mapping,
            metadata=[m for m in self.metadata if m.id in all_ids],
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


@dataclass
class VariableMetadata:
    """Wrapper around variable metadata."""

    id: int
    min_year: int
    max_year: int
