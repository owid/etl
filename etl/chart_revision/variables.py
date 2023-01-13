from dataclasses import dataclass
from typing import List, Any, Dict, Union
from etl.db import get_connection
import pandas as pd


@dataclass
class Variables:
    # Variable ID mapping (old to new)
    mapping: Dict[int, int]
    # Metadata for each variable (by variable ID)
    _metadata: Dict[Any, Any] = None

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
    def metadata(self):
        if self._metadata is None:
            # build query
            query = """
                SELECT variableId, MIN(year) AS minYear, MAX(year) AS maxYear
                FROM data_values
                WHERE variableId IN %(variable_ids)s
                GROUP BY variableId
            """
            # get data
            with get_connection() as db_conn:
                df_var_years = pd.read_sql(query, db_conn, params={"variable_ids": self.ids_all})

            # build variables metadata
            self._metadata = df_var_years.set_index("variableId").to_dict("index")
        return self._metadata

    def get_year_range(self, variable_id_or_ids: Union[int, List[int]]):
        """Get year range for variable(s).

        If variable_id_or_ids is a single variable ID, it obtains the min and max year for that variable. Otherwise,
        if variable_id_or_ids is a list of variable IDs, then it obtains the lowest minimum year and the highest maximum year
        of all variables in the list.
        """
        if isinstance(variable_id_or_ids, int):
            return [self.metadata[variable_id_or_ids]["minYear"], self.metadata[variable_id_or_ids]["maxYear"]]
        elif isinstance(variable_id_or_ids, list):
            year_min = 3000
            year_max = -1
            for var_id in variable_id_or_ids:
                year_min = min(year_min, self.metadata[var_id]["minYear"])
                year_max = max(year_max, self.metadata[var_id]["maxYear"])
            return [year_min, year_max]
