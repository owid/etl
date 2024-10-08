import json
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from owid.repack import repack_frame

from etl.db_utils import get_connection

CURRENT_DIR = Path(__file__).parent
GRAPHER_VARIABLES_PATH = CURRENT_DIR / "grapher_variables.feather"


class VariableMatcher:
    """Matches a variable name to one or more variables in the grapher database,
    if any matching variables exist. Grapher variables should be first exported
    to a feather file by running this as a script.

    Matches are conducted using exact string matching (case sensitive), as well
    as name changes tracked in `wdi.variable_mapping.json` (case sensitive).

    Example usage::

        # first generate static file with
        # python etl/steps/data/garden/worldbank_wdi/2022-05-26/wdi/variable_matcher.py

        >>> vm = VariableMatcher()
        >>> matches = vm.find_grapher_variables('Gini index')

        >>> vm = VariableMatcher()
        >>> matches = vm.find_grapher_variables('Gini index')
        >>> print([(v['id'], v['name']) for v in matches])
        [(147787, 'Gini index (World Bank estimate)')]
    """

    def __init__(self, fname: str = "wdi", grapher_variables_path: Path = GRAPHER_VARIABLES_PATH) -> None:
        self.grapher_variables = pd.read_feather(grapher_variables_path)
        self.variable_mapping = self.load_variable_mapping(fname)

    @property
    def grapher_variables(self) -> pd.DataFrame:
        return self._grapher_variables

    @grapher_variables.setter
    def grapher_variables(self, value: pd.DataFrame) -> None:
        assert isinstance(value, pd.DataFrame)
        self._grapher_variables = value

    @property
    def variable_mapping(self) -> Dict[str, Any]:
        return self._variable_mapping

    @variable_mapping.setter
    def variable_mapping(self, value: Dict[str, Any]) -> None:
        assert isinstance(value, dict)
        self._variable_mapping = value

    @staticmethod
    def fetch_grapher_variables() -> pd.DataFrame:
        query = """
            WITH
            datasets AS (
                SELECT
                    id,
                    name,
                    createdAt,
                    updatedAt
                FROM datasets
                WHERE
                    namespace REGEXP "^worldbank_wdi"
                    OR name REGEXP "[Ww]orld [Dd]evelopment [Ii]ndicators"
            )
            SELECT
                id,
                name,
                description,
                unit,
                shortUnit,
                display,
                createdAt,
                updatedAt,
                datasetId,
                sourceId
            FROM variables
            WHERE datasetId IN (SELECT id FROM datasets)
            ORDER BY updatedAt DESC
        """
        df_vars = pd.read_sql(query, get_connection())

        # make it smaller
        df_vars = repack_frame(df_vars)

        return cast(pd.DataFrame, df_vars.reset_index(drop=True))

    def load_variable_mapping(self, fname: str) -> Dict[str, Any]:
        with open(Path(__file__).parent / f"{fname}.variable_mapping.json", "r") as f:
            mapping = json.load(f)
            assert isinstance(mapping, dict)
        return mapping

    def find_grapher_variables(self, name: str) -> Optional[List[Any]]:
        """returns grapher variables that match {name}, ordered by updatedAt
        (most recent -> least recent)."""
        names = [name]
        # retrieve alternate names of the variable
        for d in self.variable_mapping.values():
            mapping = d.get("change_in_description", {})
            if name in mapping.values():
                rev_mapping = {new: old for old, new in mapping.items()}
                assert len(mapping) == len(rev_mapping)
                names.append(rev_mapping[name])

        matches = (
            self.grapher_variables.query("name in @names")
            .sort_values("updatedAt", ascending=False)
            .to_dict(orient="records")
        )
        return matches


if __name__ == "__main__":
    # fetch grapher variables from the database and save them into a feather file
    # so that anyone can load them without having to connect to MySQL
    df = VariableMatcher.fetch_grapher_variables()
    df.to_feather(GRAPHER_VARIABLES_PATH)
