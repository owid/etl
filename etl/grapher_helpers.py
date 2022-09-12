import copy
import warnings
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, cast

import numpy as np
import pandas as pd
import structlog
import yaml
from owid import catalog
from owid.catalog.utils import underscore
from pydantic import BaseModel

from etl.db import get_connection, get_engine
from etl.db_utils import DBUtils
from etl.paths import REFERENCE_DATASET

log = structlog.get_logger()


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


def annotate_table_from_yaml(table: catalog.Table, path: Path, **kwargs: Any) -> catalog.Table:
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


def _yield_wide_table(
    table: catalog.Table,
    na_action: Literal["drop", "raise"] = "raise",
    dim_titles: Optional[List[str]] = None,
) -> Iterable[catalog.Table]:
    """We have 5 dimensions but graphers data model can only handle 2 (year and entityId). This means
    we have to iterate all combinations of the remaining 3 dimensions and create a new variable for
    every combination that cuts out only the data points for a specific combination of these 3 dimensions
    Grapher can only handle 2 dimensions (year and entityId)

    :param na_action: grapher does not support missing values, you can either drop them using this argument
        or raise an exception
    :param dim_titles: Custom names to use for the dimensions, if not provided, the default names will be used.
        Dimension title will be used to create variable name, e.g. `Deaths - Age: 10-18` instead of `Deaths - age: 10-18`
    """
    table = copy.deepcopy(table)
    table.metadata = copy.deepcopy(table.metadata)

    # Validation
    if "year" not in table.primary_key:
        raise Exception("Table is missing `year` primary key")
    if "entity_id" not in table.primary_key:
        raise Exception("Table is missing `entity_id` primary key")
    if na_action == "raise":
        for col in table.columns:
            if table[col].isna().any():
                raise ValueError(f"Column `{col}` contains missing values")

    dim_names = [k for k in table.primary_key if k not in ("year", "entity_id")]
    if dim_titles:
        assert len(dim_names) == len(
            dim_titles
        ), "`dim_titles` must be the same length as your index without year and entity_id"
    else:
        dim_titles = dim_names

    if dim_names:
        grouped = table.groupby(dim_names, as_index=False, observed=True)
    else:
        # a situation when there's only year and entity_id in index with no additional dimensions
        grouped = [([], table)]

    for dims, table_to_yield in grouped:
        dims = [dims] if isinstance(dims, str) else dims

        # Now iterate over every column in the original dataset and export the
        # subset of data that we prepared above
        for column in table_to_yield.columns:
            # If all values are null, skip variable
            if table_to_yield[column].isnull().all():
                log.warning("yield_wide_table.null_variable", column=column, dims=dims)
                continue

            # Safety check to see if the metadata is still intact
            assert (
                table_to_yield[column].metadata.unit is not None
            ), f"Unit for column {column} should not be None here!"

            # Select only one column and dimensions for performance
            tab = table_to_yield[[column]].copy()
            tab.metadata = copy.deepcopy(tab.metadata)

            # Drop NA values
            tab = tab.dropna() if na_action == "drop" else tab

            # Create underscored name of a new column from the combination of column and dimensions
            short_name = _underscore_column_and_dimensions(column, dims, dim_names)

            # set new metadata with dimensions
            tab.metadata.short_name = short_name
            tab = tab.rename(columns={column: short_name})

            # add info about dimensions to metadata
            if dims:
                tab[short_name].metadata.additional_info = {
                    "dimensions": {
                        "originalShortName": column,
                        "originalName": tab[short_name].metadata.title,
                        "filters": [
                            {"name": dim_name, "value": dim_value} for dim_name, dim_value in zip(dim_names, dims)
                        ],
                    }
                }

            # Add dimensions to title (which will be used as variable name in grapher)
            title_with_dims: Optional[str]
            if tab[short_name].metadata.title:
                title_with_dims = _title_column_and_dimensions(tab[short_name].metadata.title, dims, dim_titles)
                tab[short_name].metadata.title = title_with_dims
            else:
                title_with_dims = None

            log.info(
                "yield_wide_table.create_variable",
                short_name=short_name,
                title=title_with_dims,
            )

            # Keep only entity_id and year in index
            yield tab.reset_index().set_index(["entity_id", "year"])[[short_name]]


def _title_column_and_dimensions(title: str, dims: List[str], dim_names: List[str]) -> str:
    """Create new title from column title and dimensions.
    For instance `Deaths`, ["age", "sex"], ["10-18", "male"] will be converted into
    Deaths - Age: 10-18 - Sex: male
    """
    dims = [f"{dim_name.capitalize()}: {dim}" for dim, dim_name in zip(dims, dim_names)]

    return " - ".join([title] + dims)


def _underscore_column_and_dimensions(column: str, dims: List[str], dim_names: List[str]) -> str:
    # add dimension names to dimensions
    dims = [f"{dim_name}_{dim}" for dim, dim_name in zip(dims, dim_names)]

    # underscore dimensions and append them using double underscores
    # NOTE: `column` has been already underscored in a table
    slug = "__".join([column] + [underscore(n) for n in dims])
    return cast(str, slug)


def _assert_long_table(table: catalog.Table) -> None:
    assert (
        table.metadata.dataset and table.metadata.dataset.sources
    ), "Table must have a dataset with sources in its metadata"

    assert set(table.columns) == {
        "variable",
        "meta",
        "value",
    }, "Table must have columns `variable`, `meta` and `value`"
    assert isinstance(table, catalog.Table), "Table must be instance of `catalog.Table`"
    assert (
        table["meta"].dropna().map(lambda x: isinstance(x, catalog.VariableMeta)).all()
    ), "Values in column `meta` must be either instances of `catalog.VariableMeta` or null"


def _yield_long_table(
    table: catalog.Table,
    annot: Optional[Annotation] = None,
    dim_titles: Optional[List[str]] = None,
) -> Iterable[catalog.Table]:
    """Takes as input a long table, which must have columns: ...
    - variable: short variable name (needs to be underscored)
    - value: variable value
    - meta: either VariableMeta object or null in every row

    Yields one or more wide tables from it, which have one column per variable.

    :param dim_titles: Custom names to use for the dimensions, if not provided, the default names will be used.
        Dimension title will be used to create variable name, e.g. `Deaths - Age: 10-18` instead of `Deaths - age: 10-18`
    """
    _assert_long_table(table)

    for var_name, t in table.groupby("variable"):
        t = t.rename(columns={"value": var_name})

        # extract metadata from column and make sure it is identical for all rows
        meta = t.pop("meta")
        t.pop("variable")
        assert set(meta.map(id)) == {
            id(meta.iloc[0])
        }, f"Variable `{var_name}` must have same metadata objects in column `meta` for all rows"
        t[var_name].metadata = meta.iloc[0]

        if annot:
            t = annotate_table(t, annot, missing_col="ignore")

        yield from _yield_wide_table(cast(catalog.Table, t), dim_titles=dim_titles)


def long_to_wide_tables(
    table: catalog.Table,
    annot: Optional[Annotation] = None,
) -> Iterable[catalog.Table]:
    """Yield wide tables from long table with the following columns:
    - variable: short variable name (needs to be underscored)
    - value: variable value
    - meta: either VariableMeta object or null in every row

    This function is similar to `yield_long_table`, but does not call `yield_wide_table` internally.
    """
    _assert_long_table(table)

    for var_name, t in table.groupby("variable"):
        t = t.rename(columns={"value": var_name})

        # extract metadata from column and make sure it is identical for all rows
        meta = t.pop("meta")
        t.pop("variable")
        assert set(meta.map(id)) == {
            id(meta.iloc[0])
        }, f"Variable `{var_name}` must have same metadata objects in column `meta` for all rows"
        t[var_name].metadata = meta.iloc[0]

        if annot:
            t = annotate_table(t, annot, missing_col="ignore")

        yield cast(catalog.Table, t)


def _get_entities_from_countries_regions(by: Literal["name", "code"] = "name") -> Dict[str, int]:
    reference_dataset = catalog.Dataset(REFERENCE_DATASET)
    countries_regions = reference_dataset["countries_regions"].reset_index()
    return cast(Dict[str, int], countries_regions.set_index(by)["legacy_entity_id"])


def _get_entities_from_db(countries: Set[str], by: Literal["name", "code"]) -> Dict[str, int]:
    q = f"select id as entity_id, {by} from entities where {by} in %(names)s"
    df = pd.read_sql(q, get_engine(), params={"names": list(countries)})
    return cast(Dict[str, int], df.set_index(by).entity_id.to_dict())


def _get_and_create_entities_in_db(countries: Set[str]) -> Dict[str, int]:
    cursor = get_connection().cursor()
    db = DBUtils(cursor)
    log.info("Creating entities in DB", countries=countries)
    return {name: db.get_or_create_entity(name) for name in countries}


def country_to_entity_id(
    country: pd.Series,
    fill_from_db: bool = True,
    create_entities: bool = False,
    errors: Literal["raise", "ignore", "warn"] = "raise",
    by: Literal["name", "code"] = "name",
) -> pd.Series:
    """Convert country name to grapher entity_id. Most of countries should be in countries_regions.csv,
    however some regions could be only in `entities` table in MySQL or doesn't exist at all.
    :param fill_from_db: if True, fill missing countries from `entities` table
    :param create_entities: if True, create missing countries in `entities` table
    :param errors: how to handle missing countries
    :param by: use `name` if you use country names, `code` if you use ISO codes
    """
    # get entities from countries_regions.csv
    entity_id = country.map(_get_entities_from_countries_regions(by=by))

    # fill entities from DB
    if entity_id.isnull().any() and fill_from_db:
        ix = entity_id.isnull()
        entity_id[ix] = country[ix].map(_get_entities_from_db(set(country[ix]), by=by))

    # create entities in DB
    if entity_id.isnull().any() and create_entities:
        assert fill_from_db, "fill_from_db must be True to create entities"
        assert by == "name", "create_entities works only with `by='name'`"
        ix = entity_id.isnull()
        entity_id[ix] = country[ix].map(_get_and_create_entities_in_db(set(country[ix])))

    if entity_id.isnull().any():
        msg = f"Some countries have not been mapped: {set(country[entity_id.isnull()])}"
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


def combine_metadata_sources(sources: List[catalog.Source]) -> catalog.Source:
    """Combine each of the attributes in the sources and assign them to the first source, since
    that is the only source that grapher will read.

    Parameters
    ----------
    sources : List[Source]
        List of sources to combine.

    Returns
    -------
    source : catalog.Source
        Combined source.

    """
    assert len(sources) >= 1, "Dataset needs to have at least one source in metadata."

    # Define the 'default_source', which will be the one where all sources' attributes are combined.
    default_source = copy.deepcopy(sources[0])
    # Attributes to combine from sources.
    attributes = [
        "name",
        "description",
        "url",
        "source_data_url",
        "owid_data_url",
        "date_accessed",
        "publication_date",
        "publication_year",
        "published_by",
        "publisher_source",
    ]
    # Combine sources' attributes into the first source (which is the only one that grapher will interpret).
    for attribute in attributes:
        # Gather non-empty values from each source for current attribute.
        values = _unique([getattr(source, attribute) for source in sources if getattr(source, attribute) is not None])
        if attribute == "description":
            # Descriptions are usually long, so it is better so put together descriptions from different sources in
            # separate lines.
            combined_value = "\n".join(values)
        elif attribute in ["date_accessed", "publication_date", "publication_year"]:
            # For dates simply take the one from the first source.
            # TODO: Instead of picking the first source, choose the most recent date.
            combined_value = values[0] if values else None
        elif attribute in ["url", "source_data_url", "owid_data_url"]:
            # Separate urls with " ; " (ensuring there is space between the urls and the semi-colons).
            combined_value = " ; ".join(values)
        else:
            # For any other attribute, values from different sources can be in the same line, separated by ;.
            combined_value = "; ".join(values)

        # Instead of leaving an empty string, make any empty field None.
        if combined_value == "":
            combined_value = None  # type: ignore

        setattr(default_source, attribute, combined_value)

    return default_source


def _adapt_dataset_metadata_for_grapher(
    metadata: catalog.DatasetMeta,
) -> catalog.DatasetMeta:
    """Adapt metadata of a garden dataset to be used in a grapher step. This function
    is not meant to be run explicitly, but by default in the grapher step.

    Parameters
    ----------
    metadata : catalog.DatasetMeta
        Dataset metadata.

    Returns
    -------
    metadata : catalog.DatasetMeta
        Adapted dataset metadata, ready to be inserted into grapher.

    """
    # Combine metadata sources into one.
    metadata.sources = [combine_metadata_sources(metadata.sources)]

    # Add the dataset description as if it was a source's description.
    if metadata.description is not None:
        if metadata.sources[0].description:
            metadata.sources[0].description = metadata.description + "\n" + metadata.sources[0].description
        else:
            metadata.sources[0].description = metadata.description

    # Empty dataset description (otherwise it will appear in `Internal notes` in the admin UI).
    metadata.description = ""

    return metadata


def _adapt_table_for_grapher(
    table: catalog.Table, country_col: str = "country", year_col: str = "year"
) -> catalog.Table:
    """Adapt table (from a garden dataset) to be used in a grapher step. This function
    is not meant to be run explicitly, but by default in the grapher step.

    Parameters
    ----------
    table : catalog.Table
        Table from garden dataset.
    country_col : str
        Name of country column in table.
    year_col : str
        Name of year column in table.

    Returns
    -------
    table : catalog.Table
        Adapted table, ready to be inserted into grapher.

    """
    table = deepcopy(table)
    if "entity_id" not in table.index.names and "year" not in table.index.names:
        # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
        table["entity_id"] = country_to_entity_id(table[country_col], create_entities=True)
        table = table.drop(columns=[country_col]).rename(columns={year_col: "year"})
        table = table.set_index(["entity_id", "year"])
    else:
        assert {"entity_id", "year"} <= set(table.index.names)

    # Ensure the default source of each column includes the description of the table (since that is the description that
    # will appear in grapher on the SOURCES tab).
    table = _ensure_source_per_variable(table)

    return cast(catalog.Table, table)


def _ensure_source_per_variable(table: catalog.Table) -> catalog.Table:
    assert table.metadata.dataset
    dataset_meta = table.metadata.dataset
    for column in table.columns:
        variable_meta: catalog.VariableMeta = table[column].metadata
        if len(variable_meta.sources) == 0:
            # Take the metadata sources from the dataset's metadata (after combining them into one).
            assert (
                len(dataset_meta.sources) > 0
            ), f"If column `{column}` has no sources, dataset must have at least one."
            source = combine_metadata_sources(dataset_meta.sources)

            # Add the dataset description as if it was a source's description.
            if dataset_meta.description is not None:
                if source.description:
                    source.description = dataset_meta.description + "\n" + source.description
                else:
                    source.description = dataset_meta.description
        else:
            sources: List[catalog.Source] = table[column].metadata.sources

            if len(sources) > 1:
                # Combine multiple sources into one.
                # NOTE: dataset description is not included in the combined source
                source = combine_metadata_sources(sources)
            else:
                source = sources[0]

            if source.description is None:
                # Use table description if sources don't have their own
                if table.metadata.description:
                    source.description = table.metadata.description
                # Or dataset description if sources don't have their own
                else:
                    source.description = dataset_meta.description

        table[column].metadata.sources = [source]

    return table


@dataclass
class IntRange:
    min: int
    _min: int = field(init=False, repr=False)
    max: int
    _max: int = field(init=False, repr=False)

    @property  # type: ignore
    def min(self) -> int:
        return self._min

    @min.setter
    def min(self, x: int) -> None:
        self._min = int(x)

    @property  # type: ignore
    def max(self) -> int:
        return self._max

    @max.setter
    def max(self, x: int) -> None:
        self._max = int(x)

    @staticmethod
    def from_values(xs: List[int]) -> "IntRange":
        return IntRange(min(xs), max(xs))

    def to_values(self) -> list[int]:
        return [self.min, self.max]


def contains_inf(s: pd.Series) -> bool:
    """Check if a series contains infinity."""
    return pd.api.types.is_numeric_dtype(s.dtype) and np.isinf(s).any()  # type: ignore
