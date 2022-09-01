"""

Harmonize country names:

    $ harmonize data/meadow/worldbank_wdi/{version}/wdi/wdi.feather country etl/steps/data/garden/worldbank_wdi/{version}/wdi.country_mapping.json
"""

import re
import json
import zipfile
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import structlog
from owid.catalog import Dataset, Table, Source, VariableMeta
from owid.catalog.utils import underscore
from owid.walden import Catalog

from etl.db import get_connection
from etl.paths import DATA_DIR

COUNTRY_MAPPING_PATH = (Path(__file__).parent / "wdi.country_mapping.json").as_posix()

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    ds_meadow = Dataset((DATA_DIR / f"meadow/{namespace}/{version}/{fname}").as_posix())

    assert len(ds_meadow.table_names) == 1, "Expected meadow dataset to have only one table, but found > 1 table names."
    tb_meadow = ds_meadow[fname]
    df = pd.DataFrame(tb_meadow).reset_index()

    # harmonize entity names
    country_mapping = load_country_mapping()
    excluded_countries = load_excluded_countries()  # noqa: F841
    df = df.query("country not in @excluded_countries")
    assert df["country"].notnull().all()
    countries = df["country"].apply(lambda x: country_mapping.get(x, None))
    if countries.isnull().any():
        missing_countries = [x for x in df["country"].drop_duplicates() if x not in country_mapping]
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            "names; or (b) remove these country names from the raw table."
            f"Raw country names: {missing_countries}"
        )

    df["country"] = countries
    df.set_index(tb_meadow.metadata.primary_key, inplace=True)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = Table(df)
    tb_garden.metadata = tb_meadow.metadata
    tb_garden = add_variable_metadata(tb_garden)

    ds_garden.add(tb_garden)
    ds_garden.save()


def add_variable_metadata(table: Table) -> Table:
    var_codes = table.columns.tolist()  # type: ignore

    # retrieves raw data from walden
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    walden_ds = Catalog().find_one(namespace=namespace, short_name=fname, version=version)
    local_file = walden_ds.ensure_downloaded()
    zf = zipfile.ZipFile(local_file)
    df_vars = pd.read_csv(zf.open("WDISeries.csv"))
    df_vars.dropna(how="all", axis=1, inplace=True)
    df_vars.columns = df_vars.columns.map(underscore)
    df_vars.rename(columns={"series_code": "indicator_code"}, inplace=True)
    df_vars["indicator_code"] = df_vars["indicator_code"].apply(underscore)
    df_vars = df_vars.query("indicator_code in @var_codes").set_index("indicator_code", verify_integrity=True)

    df_vars["indicator_name"].str.replace(r"\s+", " ", regex=True)
    clean_source_mapping = load_clean_source_mapping()

    # construct metadata for each variable
    vm = VariableMatcher()
    for var_code in var_codes:
        var = df_vars.loc[var_code].to_dict()
        # retrieves unit + display metadata from the most recently updated
        # WDI grapher variable that matches this variable's name
        unit = ""
        short_unit = ""
        display = {}
        grapher_vars = vm.find_grapher_variables(var["indicator_name"])
        if grapher_vars:
            found_unit_metadata = False
            gvar = None
            while len(grapher_vars) and not found_unit_metadata:
                gvar = grapher_vars.pop(0)
                found_unit_metadata = bool(gvar["unit"] or gvar["shortUnit"])

            if found_unit_metadata and gvar:
                if pd.notnull(gvar["unit"]):
                    unit = gvar["unit"]
                if pd.notnull(gvar["shortUnit"]):
                    short_unit = gvar["shortUnit"]
                if pd.notnull(gvar["display"]):
                    display = json.loads(gvar["display"])

                year_regex = re.compile(r"\b([1-2]\d{3})\b")
                regex_res = year_regex.search(var["indicator_name"])
                if regex_res:
                    assert len(regex_res.groups()) == 1
                    year = regex_res.groups()[0]
                    unit = replace_years(unit, year)
                    short_unit = replace_years(short_unit, year)
                    for k in ["name", "unit", "shortUnit"]:
                        if pd.notnull(display.get(k)):
                            display[k] = replace_years(display[k], year)
        else:
            log.warning(
                f"Variable does not match an existing {fname} variable name in the grapher",
                variable_name=var["indicator_name"],
            )

        # retrieve clean source name, then construct source.
        source_raw_name = var["source"]
        clean_source = clean_source_mapping.get(source_raw_name)
        assert clean_source, f'`rawName` "{source_raw_name}" not found in wdi.sources.json'
        assert table[var_code].metadata.to_dict() == {}, (
            f"Expected metadata for variable {var_code} to be empty, but "
            f"metadata is: {table[var_code].metadata.to_dict()}."
        )
        source = Source(
            name=clean_source["name"],
            description=None,
            url=walden_ds.metadata["url"],
            source_data_url=walden_ds.metadata["source_data_url"],
            owid_data_url=walden_ds.metadata["owid_data_url"],
            date_accessed=walden_ds.metadata["date_accessed"],
            publication_date=walden_ds.metadata["publication_date"],
            publication_year=walden_ds.metadata["publication_year"],
            published_by=walden_ds.metadata["name"],
            publisher_source=clean_source["dataPublisherSource"],
        )

        table[var_code].metadata = VariableMeta(
            title=df_vars.loc[var_code, "indicator_name"],
            description=create_description(var),
            sources=[source],
            unit=unit,
            short_unit=short_unit,
            display=display,
            additional_info=None
            # licenses=[var['license_type']]
        )

    if not all([len(table[var_code].sources) == 1 for var_code in var_codes]):
        missing = [var_code for var_code in var_codes if len(table[var_code].sources) != 1]
        raise RuntimeError(
            "Expected each variable code to have one source, but the following variables "
            f"do not: {missing}. Are the source names for these variables "
            "missing from `wdi.sources.json`?"
        )

    return table


def load_country_mapping() -> Dict[str, str]:
    with open(COUNTRY_MAPPING_PATH, "r") as f:
        mapping = json.load(f)
        assert isinstance(mapping, dict)
    return mapping


def load_excluded_countries() -> List[str]:
    fname = Path(__file__).stem.split(".")[0]
    with open(Path(__file__).parent / f"{fname}.country_exclude.json", "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def load_clean_source_mapping() -> dict:
    with open(Path(__file__).parent / "wdi.sources.json", "r") as f:
        sources = json.load(f)
        source_mapping = {source["rawName"]: source for source in sources}
        assert len(sources) == len(source_mapping)
    return source_mapping


def create_description(var: dict) -> Optional[str]:
    desc = ""
    if pd.notnull(var["long_definition"]) and len(var["long_definition"].strip()) > 0:
        desc += var["long_definition"]
    elif pd.notnull(var["short_definition"]) and len(var["short_definition"].strip()) > 0:
        desc += var["short_definition"]

    if pd.notnull(var["limitations_and_exceptions"]) and len(var["limitations_and_exceptions"].strip()) > 0:
        desc += f'\n\nLimitations and exceptions: {var["limitations_and_exceptions"]}'

    if (
        pd.notnull(var["statistical_concept_and_methodology"])
        and len(var["statistical_concept_and_methodology"].strip()) > 0
    ):
        desc += f'\n\nStatistical concept and methodology: {var["statistical_concept_and_methodology"]}'

    # retrieves additional source info, if it exists.
    if pd.notnull(var["notes_from_original_source"]) and len(var["notes_from_original_source"].strip()) > 0:
        desc += f'\n\nNotes from original source: {var["notes_from_original_source"]}'

    desc = re.sub(r" *(\n+) *", r"\1", re.sub(r"[ \t]+", " ", desc)).strip()

    if len(desc) == 0:
        desc = None

    return desc


def replace_years(s: str, year: Union[int, str]) -> str:
    """replaces all years in string with {year}.

    Example:

        >>> replace_years("GDP (constant 2010 US$)", 2015)
        "GDP (constant 2015 US$)"
    """
    year_regex = re.compile(r"\b([1-2]\d{3})\b")
    s_new = year_regex.sub(str(year), s)
    return s_new


class VariableMatcher:
    """Matches a variable name to one or more variables in the grapher database,
    if any matching variables exist.

    Matches are conducted using exact string matching (case sensitive), as well
    as name changes tracked in `wdi.variable_mapping.json` (case sensitive).

    Example usage::

        >>> vm = VariableMatcher()
        >>> matches = vm.find_grapher_variables('Gini index')
        >>> print([(v['id'], v['name']) for v in matches])
        [(147787, 'Gini index (World Bank estimate)')]
    """

    def __init__(self):
        self.grapher_variables = self.fetch_grapher_variables()
        self.variable_mapping = self.load_variable_mapping()

    @property
    def grapher_variables(self) -> pd.DataFrame:
        return self._grapher_variables

    @grapher_variables.setter
    def grapher_variables(self, value: pd.DataFrame) -> None:
        assert isinstance(value, pd.DataFrame)
        self._grapher_variables = value

    @property
    def variable_mapping(self) -> dict:
        return self._variable_mapping

    @variable_mapping.setter
    def variable_mapping(self, value: dict) -> None:
        assert isinstance(value, dict)
        self._variable_mapping = value

    def fetch_grapher_variables(self) -> pd.DataFrame:
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
        return df_vars

    def load_variable_mapping(self) -> dict:
        fname = Path(__file__).stem
        with open(Path(__file__).parent / f"{fname}.variable_mapping.json", "r") as f:
            mapping = json.load(f)
        return mapping

    def find_grapher_variables(self, name: str) -> Optional[List[dict]]:
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
