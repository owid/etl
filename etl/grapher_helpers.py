import pandas as pd
from owid import catalog
from collections.abc import Iterable
import yaml
import slugify
import warnings
from pathlib import Path

from typing import Optional, Dict, Literal, cast, List, Any
from pydantic import BaseModel

from etl.paths import DATA_DIR


# TODO: remove if it turns out to be useless for real examples
class DatasetModel(BaseModel):
    source: str
    short_name: str
    namespace: str


# TODO: remove if this turns out to be useless for real examples
class DimensionModel(BaseModel):
    pass


class VariableModel(BaseModel):
    description: str
    unit: str
    short_unit: Optional[str]


class Annotation(BaseModel):
    dataset: Optional[DatasetModel]
    dimensions: Optional[Dict[str, Optional[DimensionModel]]]
    variables: Dict[str, VariableModel]

    @property
    def dimension_names(self) -> List[str]:
        if self.dimensions:
            return list(self.dimensions.keys())
        else:
            return []

    @property
    def variable_names(self) -> List[str]:
        return list(self.variables.keys())

    @classmethod
    def load_from_yaml(cls, path: Path) -> "Annotation":
        # Load variable descriptions and units from the annotations.yml file and
        # store them as column metadata
        with open(path) as istream:
            annotations = yaml.safe_load(istream)
        return cls.parse_obj(annotations)


def as_table(df: pd.DataFrame, table: catalog.Table) -> catalog.Table:
    """Convert dataframe into Table and add metadata from other table if available."""
    t = catalog.Table(df, metadata=table.metadata)
    for col in set(df.columns) & set(table.columns):
        t[col].metadata = table[col].metadata
    return t


def annotate_table_from_yaml(
    table: catalog.Table, path: Path, **kwargs: Any
) -> catalog.Table:
    """Load variable descriptions and units from the annotations.yml file and
    store them as column metadata."""
    annot = Annotation.load_from_yaml(path)
    return annotate_table(table, annot, **kwargs)


def annotate_table(
    table: catalog.Table,
    annot: Annotation,
    missing_col: Literal["raise", "ignore"] = "raise",
) -> catalog.Table:
    for column in annot.variable_names:
        v = annot.variables[column]
        if column not in table:
            if missing_col == "raise":
                raise Exception(f"Column {column} not in table")
            elif missing_col != "ignore":
                raise ValueError(f"Unknown missing_col value: {missing_col}")
        else:
            # overwrite metadata
            for k, v in dict(v).items():
                setattr(table[column].metadata, k, v)

    return table


def yield_wide_table(table: catalog.Table) -> Iterable[catalog.Table]:
    """We have 5 dimensions but graphers data model can only handle 2 (year and entityId). This means
    we have to iterate all combinations of the remaining 3 dimensions and create a new variable for
    every combination that cuts out only the data points for a specific combination of these 3 dimensions
    Grapher can only handle 2 dimensions (year and entityId)"""
    # Validation
    if "year" not in table.primary_key:
        raise Exception("Table is missing `year` primary key")
    if "entity_id" not in table.primary_key:
        raise Exception("Table is missing `entity_id` primary key")

    dim_names = [k for k in table.primary_key if k not in ("year", "entity_id")]

    if dim_names:
        grouped = table.groupby(dim_names, as_index=False)
    else:
        # a situation when there's only year and entity_id in index with no additional dimensions
        grouped = [([], table)]

    for dims, table_to_yield in grouped:
        # Now iterate over every column in the original dataset and export the
        # subset of data that we prepared above
        for column in table_to_yield.columns:

            # Add column and dimensions as short_name
            table_to_yield.metadata.short_name = slugify.slugify(
                "__".join([column] + list(dims)), separator="_"
            )

            # Safety check to see if the metadata is still intact
            assert (
                table_to_yield[column].metadata.unit is not None
            ), f"Unit for column {column} should not be None here!"

            print(f"Yielding table {table_to_yield.metadata.short_name}")

            yield table_to_yield.reset_index().set_index(["entity_id", "year"])[
                [column]
            ]


def yield_long_table(
    table: catalog.Table, annot: Optional[Annotation] = None
) -> Iterable[catalog.Table]:
    """Yield from long table with columns `variable`, `value` and optionally `unit`."""
    assert set(table.columns) <= {"variable", "value", "unit"}

    for var_name, t in table.groupby("variable"):
        t = t.rename(columns={"value": var_name})

        if "unit" in t.columns:
            # move variable to its own column and annotate it
            assert len(set(t["unit"])) == 1, "units must be the same for all rows"
            t[var_name].metadata.unit = t.unit.iloc[0]

        if annot:
            t = annotate_table(t, annot, missing_col="ignore")

        t = t.drop(["variable", "unit"], axis=1, errors="ignore")

        yield from yield_wide_table(cast(catalog.Table, t))


def country_to_entity_id(
    country: pd.Series, errors: Literal["raise", "ignore", "warn"] = "raise"
) -> pd.Series:
    """Convert country name to grapher entity_id."""
    reference_dataset = catalog.Dataset(DATA_DIR / "reference")
    countries_regions = reference_dataset["countries_regions"]
    country_map = countries_regions.set_index("name")["legacy_entity_id"]
    entity_id = country.map(country_map)

    if entity_id.isnull().any():
        msg = f"Some countries are not in the reference dataset: {set(country[entity_id.isnull()])}"
        if errors == "raise":
            raise ValueError(msg)
        elif errors == "warn":
            warnings.warn(msg)
        elif errors == "ignore":
            pass

        # Int64 allows NaN values
        return cast(pd.Series, entity_id.astype("Int64"))
    else:
        return cast(pd.Series, entity_id.astype(int))


def _unique(x: List[Any]) -> List[Any]:
    """Uniquify a list, preserving order."""
    return list(dict.fromkeys(x))


def join_sources(sources: List[catalog.meta.Source]) -> catalog.meta.Source:
    """Join multiple sources into one for the grapher."""
    meta = {}
    for key, sep in [
        ("name", ", "),
        ("description", "\n\n"),
        ("url", "; "),
        ("source_data_url", "; "),
        ("owid_data_url", "; "),
        ("published_by", ", "),
        ("publisher_source", ", "),
        ("date_accessed", "; "),
    ]:
        keys = _unique([getattr(s, key) for s in sources if getattr(s, key)])
        if keys:
            meta[key] = sep.join(keys)

    return catalog.meta.Source(**meta)
