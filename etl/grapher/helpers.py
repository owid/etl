import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, Union, cast

import numpy as np
import pandas as pd
import pymysql
import structlog
from jsonschema import validate
from owid import catalog
from owid.catalog import Table, jinja, warnings
from owid.catalog.utils import dynamic_yaml_load, dynamic_yaml_to_dict, underscore
from owid.catalog.yaml_metadata import merge_with_shared_meta
from sqlalchemy import exc, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.db import get_engine, read_sql
from etl.files import get_schema_from_url, yaml_dump
from etl.grapher.io import add_entity_code_and_name, trim_long_variable_name

log = structlog.get_logger()

# this might work too pd.api.types.is_integer_dtype(col)
INT_TYPES = tuple(
    {f"{n}{b}{p}" for n in ("int", "Int", "uint", "UInt") for b in ("8", "16", "32", "64") for p in ("", "[pyarrow]")}
)


def _yield_wide_table(
    table: catalog.Table,
    na_action: Literal["drop", "raise"] = "raise",
    warn_null_variables: bool = False,
    trim_long_short_name: bool = True,
) -> Iterable[catalog.Table]:
    """We have 5 dimensions but graphers data model can only handle 2 (year and entityId). This means
    we have to iterate all combinations of the remaining 3 dimensions and create a new variable for
    every combination that cuts out only the data points for a specific combination of these 3 dimensions
    Grapher can only handle 2 dimensions (year and entityId)

    :param na_action: grapher does not support missing values, you can either drop them using this argument
        or raise an exception
    :param trim_long_short_name: If true and there's a short name longer than 255 characters, we trim it to 240 characters
        and add a hash from its short name to the end to make it unique.
    """
    table = table.copy(deep=False)

    # Validation
    if "year" not in table.primary_key:
        raise Exception("Table is missing `year` primary key")
    if "entityId" not in table.primary_key:
        raise Exception("Table is missing `entityId` primary key")
    if na_action == "raise":
        for col in table.columns:
            if table[col].isna().any():
                raise ValueError(f"Column `{col}` contains missing values")
    cols_with_none_units = [col for col in table.columns if table[col].metadata.unit is None]
    if cols_with_none_units:
        raise Exception("Columns with missing units: " + ", ".join(cols_with_none_units))

    dim_names = [k for k in table.primary_key if k not in ("year", "entityId", "entityCode", "entityName")]

    # Keep only entity_id and year in index
    table = table.reset_index(level=dim_names)

    if dim_names:
        # `dropna=False` makes sure we don't drop NaN values from index
        grouped = table.groupby(
            dim_names if len(dim_names) > 1 else dim_names[0], as_index=False, observed=True, dropna=False
        )
    else:
        # a situation when there's only year and entity_id in index with no additional dimensions
        grouped = [([], table)]

    for dim_values, table_to_yield in grouped:
        if not isinstance(dim_values, tuple):
            dim_values = (dim_values,)

        # Exclude dimensions
        table_to_yield = table_to_yield[[c for c in table_to_yield.columns if c not in dim_names]]

        # Filter NaN values from dimensions and return dictionary
        dim_dict = _create_dim_dict(dim_names, dim_values)  # type: ignore

        # Now iterate over every column in the original dataset and export the
        # subset of data that we prepared above
        for column in table_to_yield.columns:
            # If all values are null, skip variable
            if table_to_yield[column].isnull().all():
                if warn_null_variables:
                    log.warning("yield_wide_table.null_variable", column=column, dim_dict=dim_dict)
                continue

            # Safety check to see if the metadata is still intact
            assert table_to_yield[column].metadata.unit is not None, (
                f"Unit for column {column} should not be None here!"
            )

            # Select only one column and dimensions for performance
            # Silence - DeprecationWarning: Passing a BlockManager to Table is deprecated and will raise
            # in a future version. Use public APIs instead.
            with warnings.ignore_warnings([DeprecationWarning]):
                mask = table_to_yield[column].notna() if na_action == "drop" else slice(None)
                # NOTE: this copy is important, otherwise we'd ruin metadata
                tab = table_to_yield.loc[mask, [column]].copy(deep=False)

            # Create underscored name of a new column from the combination of column and dimensions
            short_name = _underscore_column_and_dimensions(
                column,
                dim_dict,
                trim_long_short_name=trim_long_short_name,
            )

            # set new metadata with dimensions
            tab.metadata.short_name = short_name
            tab.rename(columns={column: short_name}, inplace=True)

            tab[short_name].metadata = _metadata_for_dimensions(tab[short_name].metadata, dim_dict, column)

            yield tab


def _metadata_for_dimensions(meta: catalog.VariableMeta, dim_dict: Dict[str, Any], column: str) -> catalog.VariableMeta:
    """Add dimensions to metadata and expand Jinja in metadata fields."""
    # Add info about dimensions to metadata
    if dim_dict:
        meta.dimensions = {dim_name: sanitize_numpy(dim_value) for dim_name, dim_value in dim_dict.items()}
        meta.original_short_name = column
        meta.original_title = meta.title

        # Soon to be deprecated
        meta.additional_info = {
            "dimensions": {
                "originalShortName": column,
                "originalName": meta.title,
                "filters": [
                    {"name": dim_name, "value": sanitize_numpy(dim_value)} for dim_name, dim_value in dim_dict.items()
                ],
            }
        }

    # If title doesn't contain Jinja, use default title with dimensions
    if meta.title and not jinja._uses_jinja(meta.title):
        meta.title = _title_column_and_dimensions(meta.title, dim_dict)

    # Render Jinja template with dimensions
    try:
        return meta.render(dim_dict)
    except Exception as e:
        # Reraise with more context
        raise ValueError(
            f"Error expanding Jinja in metadata for column '{column}' with dim values: {dim_dict}.\n\nVariable metadata:\n\n{yaml_dump(meta.to_dict())}"
        ) from e


def _create_dim_dict(dim_names: List[str], dim_values: List[Any]) -> Dict[str, Any]:
    # Filter NaN values from dimensions and return dictionary
    return {n: v for n, v in zip(dim_names, dim_values) if pd.notnull(v)}


def long_to_wide(long_tb: catalog.Table) -> catalog.Table:
    """Convert a long table to a wide table by unstacking dimensions. This function mimics the process that occurs
    when a long table is upserted to the database. With this function, you can explicitly perform this transformation
    in the grapher step and store a flattened dataset in the catalog."""

    dim_names = [k for k in long_tb.primary_key if k not in ("year", "country", "date")]

    # Unstack dimensions to a wide format
    wide_tb = cast(catalog.Table, long_tb.unstack(level=dim_names))  # type: ignore

    # Drop columns with all NaNs
    wide_tb = wide_tb.dropna(axis=1, how="all")

    # Get short names and metadata for all columns
    short_names = []
    metadatas = []
    for dims in wide_tb.columns:
        column = dims[0]

        # Filter NaN values from dimensions and return dictionary
        dim_dict = _create_dim_dict(dim_names, dims[1:])

        # Create a short name from dimension values
        short_name = _underscore_column_and_dimensions(column, dim_dict)

        if short_name in short_names:
            duplicate_short_name_ix = short_names.index(short_name)
            # raise ValueError(f"Duplicate short name: {short_name} for column: {column} and dimensions: {dim_dict}")
            duplicate_dim_dict = dict(zip(dim_names, wide_tb.columns[duplicate_short_name_ix][1:]))
            raise ValueError(
                f"Duplicate short name for column '{column}' with dim values:\n{duplicate_dim_dict}\n{dim_dict}"
            )

        short_names.append(short_name)

        # Create metadata for the column from dimensions
        metadatas.append(_metadata_for_dimensions(long_tb[dims[0]].metadata.copy(), dim_dict, column))

    # Set column names to new short names and use proper metadata
    wide_tb.columns = short_names
    for col, meta in zip(wide_tb.columns, metadatas):
        wide_tb[col].metadata = meta

    return wide_tb


def render_yaml_file(path: Union[str, Path], dim_dict: Dict[str, str]) -> Dict[str, Any]:
    """Load YAML file and render Jinja in all fields. Return a dictionary.

    Usage:
        # Create a playground.ipynb next to YAML file and run this in notebook
        from etl.grapher import helpers as gh

        m = gh.render_yaml_file("ghe.meta.yml", dim_dict={"sex": "male"})
        m['tables']['ghe']['variables']['death_count']
    """
    path_or_io = merge_with_shared_meta(Path(path))
    meta = dynamic_yaml_to_dict(dynamic_yaml_load(path_or_io))
    return jinja._expand_jinja(meta, dim_dict)


def _title_column_and_dimensions(title: str, dim_dict: Dict[str, Any]) -> str:
    """Create new title from column title and dimensions.
    For instance `Deaths`, ["age", "sex"], ["10-18", "male"] will be converted into
    Deaths - Age: 10-18 - Sex: male
    """
    dims = [f"{dim_name.replace('_', ' ').capitalize()}: {dim_value}" for dim_name, dim_value in dim_dict.items()]
    return " - ".join([title] + dims)


def _underscore_column_and_dimensions(column: str, dim_dict: Dict[str, Any], trim_long_short_name: bool = True) -> str:
    # add dimension names to dimensions
    dims = [f"{dim_name}_{dim_value}" for dim_name, dim_value in dim_dict.items()]

    # underscore dimensions and append them using double underscores
    # NOTE: `column` has been already underscored in a table
    slug = "__".join([column] + [underscore(n) for n in dims])
    short_name = cast(str, slug)

    if len(short_name) > 255:
        if trim_long_short_name:
            short_name = trim_long_variable_name(short_name)
            log.warning(
                "short_name_trimmed",
                short_name=short_name,
                column=column,
                dims=dim_dict,
            )
        else:
            raise AssertionError(f"short_name {short_name} is too long for MySQL variables.shortName column")

    return short_name


def _assert_long_table(table: catalog.Table) -> None:
    # NOTE: I'm not sure if we need this validation, looks like we don't
    # assert (
    #     table.metadata.dataset and table.metadata.dataset.sources
    # ), "Table must have a dataset with sources in its metadata"

    assert set(table.columns) == {
        "variable",
        "meta",
        "value",
    }, "Table must have columns `variable`, `meta` and `value`"
    assert isinstance(table, catalog.Table), "Table must be instance of `catalog.Table`"
    assert table["meta"].dropna().map(lambda x: isinstance(x, catalog.VariableMeta)).all(), (
        "Values in column `meta` must be either instances of `catalog.VariableMeta` or null"
    )


def long_to_wide_tables(
    table: catalog.Table,
    metadata_path: Optional[Path] = None,
) -> Iterable[catalog.Table]:
    """Yield wide tables from long table with the following columns:
    - variable: short variable name (needs to be underscored)
    - value: variable value
    - meta: either VariableMeta object or null in every row

    This function is similar to `yield_long_table`, but does not call `yield_wide_table` internally.
    """
    # validation
    _assert_long_table(table)
    for var_name in table.variable.unique():
        catalog.utils.validate_underscore(var_name, "Variable name")

    for var_name, t in table.groupby("variable"):
        t = t.rename(columns={"value": var_name})

        # extract metadata from column and make sure it is identical for all rows
        meta = t.pop("meta")
        t.pop("variable")
        assert set(meta.map(id)) == {id(meta.iloc[0])}, (
            f"Variable `{var_name}` must have same metadata objects in column `meta` for all rows"
        )
        t[var_name].metadata = meta.iloc[0]

        # name table as variable name
        t.metadata.short_name = var_name

        if metadata_path:
            t.update_metadata_from_yaml(metadata_path, var_name)

        yield cast(catalog.Table, t)


def _get_entities_from_db(
    countries: Set[str], by: Literal["name", "code"], engine: Engine | None = None
) -> Dict[str, int]:
    q = f"select id as entity_id, {by} from entities where {by} in %(names)s"
    df = read_sql(q, engine, params={"names": list(countries)})
    return cast(Dict[str, int], df.set_index(by).entity_id.to_dict())


def _get_and_create_entities_in_db(countries: Set[str], engine: Engine | None = None) -> Dict[str, int]:
    engine = engine or get_engine()
    with Session(engine) as session:
        log.info("Creating entities in DB", countries=countries)
        out = {}
        for name in countries:
            try:
                session.execute(
                    text(
                        """
                    INSERT INTO entities
                        (name, validated, createdAt, updatedAt)
                    VALUES
                        (:name, FALSE, NOW(), NOW())
                """
                    ),
                    {"name": name},
                )
                session.commit()
            except (pymysql.IntegrityError, exc.IntegrityError):
                # If another process inserted the same entity before us, we can
                # safely ignore the error and fetch the ID
                pass

            row = session.execute(
                text(
                    """
                SELECT id FROM entities
                WHERE name = :name
            """
                ),
                {"name": name},
            ).fetchone()
            assert row

            out[name] = row[0]

    return out


def country_to_entity_id(
    country: pd.Series,
    create_entities: bool = False,
    by: Literal["name", "code"] = "name",
    engine: Engine | None = None,
) -> pd.Series:
    """Convert country name to grapher entity_id. Most of countries should be in countries_regions.csv,
    however some regions could be only in `entities` table in MySQL or doesn't exist at all.

    This function should not be used from ETL steps, conversion to entity_id is done automatically
    when upserting to database.

    :param create_entities: if True, create missing countries in `entities` table
    :param errors: how to handle missing countries
    :param by: use `name` if you use country names, `code` if you use ISO codes
    """
    # fill entities from DB
    db_entities = _get_entities_from_db(set(country.unique()), by=by, engine=engine)
    entity_id = country.map(db_entities).astype(float)

    # create entities in DB
    if entity_id.isnull().any() and create_entities:
        assert by == "name", "create_entities works only with `by='name'`"
        ix = entity_id.isnull()
        # cast to float to fix issues with categories
        entity_id[ix] = (  # type: ignore[reportCallIssue]
            country[ix].map(_get_and_create_entities_in_db(set(country[ix].unique()), engine=engine)).astype(float)  # type: ignore[reportCallIssue]
        )

    assert not entity_id.isnull().any(), f"Some countries have not been mapped: {set(country[entity_id.isnull()])}"  # type: ignore[reportCallIssue]

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
    if metadata.sources:
        metadata.sources = [combine_metadata_sources(metadata.sources)]

        # Add the dataset description as if it was a source's description.
        if metadata.description is not None:
            if metadata.sources[0].description:
                # If descriptions are not subsets of each other (or equal), add them together
                if (
                    metadata.sources[0].description not in metadata.description
                    and metadata.description not in metadata.sources[0].description
                ):
                    metadata.sources[0].description = metadata.description + "\n" + metadata.sources[0].description
            else:
                metadata.sources[0].description = metadata.description

    # Empty dataset description (otherwise it will appear in `Internal notes` in the admin UI).
    metadata.description = ""

    return metadata


def _adapt_table_for_grapher(table: catalog.Table, engine: Engine) -> catalog.Table:
    """Adapt table (from a garden dataset) to be used in a grapher step. This function
    is not meant to be run explicitly, but by default in the grapher step.

    Parameters
    ----------
    table : catalog.Table
        Table from garden dataset.

    Returns
    -------
    table : catalog.Table
        Adapted table, ready to be inserted into grapher.

    """
    table = table.copy(deep=False)

    variable_titles = pd.Series([table[col].title for col in table.columns]).dropna()
    variable_titles_counts = variable_titles.value_counts()
    assert variable_titles_counts.empty or variable_titles_counts.max() == 1, (
        f"Variable titles are not unique:\n{variable_titles_counts[variable_titles_counts > 1].index}."
    )

    # Remember original dimensions
    dim_names = [n for n in table.index.names if n and n not in ("year", "date", "entity_id", "country")]

    # Reset index unless we have default index
    if table.index.names != [None]:
        table = table.reset_index()

    # If a table contains `date` instead of `year`, adapt it for grapher
    if "date" in table.columns:
        # NOTE: this can be relaxed if we ever need it
        assert "year" not in table.columns, "Table cannot have both `date` and `year` columns."
        table = adapt_table_with_dates_to_grapher(table)

    assert {"year", "country"} <= set(table.columns), "Table must have columns country and year."
    assert "entity_id" not in table.columns, "Table must not have column entity_id."

    # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
    table["entityId"] = country_to_entity_id(table["country"], create_entities=True, engine=engine)
    table = table.drop(columns=["country"])

    # Add entity code and name
    with Session(engine) as session:
        table = add_entity_code_and_name(session, table).copy_metadata(table)

    table = table.set_index(["entityId", "entityCode", "entityName", "year"] + dim_names)

    # Ensure the default source of each column includes the description of the table (since that is the description that
    # will appear in grapher on the SOURCES tab).
    table = _ensure_source_per_variable(table)

    return cast(catalog.Table, table)


def _ensure_source_per_variable(table: catalog.Table) -> catalog.Table:
    assert table.metadata.dataset
    dataset_meta = table.metadata.dataset
    for column in table.columns:
        variable_meta: catalog.VariableMeta = table[column].metadata

        # If neither variable and dataset has no sources, we're using origins instead.
        if len(variable_meta.sources) == 0 and len(dataset_meta.sources) == 0:
            assert len(variable_meta.origins) > 0, f"Variable `{column}` has no sources or origins."
            continue

        if len(variable_meta.sources) == 0:
            # Take the metadata sources from the dataset's metadata (after combining them into one).
            assert len(dataset_meta.sources) > 0, (
                f"If column `{column}` has no sources, dataset must have at least one."
            )
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
    min: int  # type: ignore
    _min: int = field(init=False, repr=False)
    max: int  # type: ignore
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


def sanitize_numpy(obj: Any) -> Any:
    """Sanitize numpy types so that we can insert them into MySQL."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


def add_columns_for_multiindicator_chart(
    table: catalog.Table,
    columns_in_chart: List[str],
    chart_slug: str,
    suffix_for_titles: Optional[str] = None,
    columns_to_fill_with_zeros: Optional[List[str]] = None,
) -> catalog.Table:
    """Add columns that will be used in a specific multi-indicator (e.g. a stacked area) chart handling issues with
    missing data.

    There are two common data issues that affect multi-indicator charts:
    1. Some indicators have missing data that should be zeros. This is a common bad practice done by data producers.
      For example, in the Statistical Review of World Energy, nuclear power for Australia before 2000 is missing, and
      Kazakhstan has no data on nuclear energy after 2000. In both cases, those missing values should be zero (since
      there was no nuclear power for those years in those countries).
    2. Different indicators have missing data for different years. This makes sense: Some indicators are better informed
      than others.
      For example, in the Electricity Mix dataset, Algeria in 2022 has data for renewables but not for fossil fuels.
      In stacked area charts showing renewable and fossil electricity, Algeria appears as 100% renewable in 2022,
      which is clearly wrong.

    This function can be used to create columns for all indicators used in a specific multi-indicator chart.
    It will:
    * Optionally, fill missing data for some of the new columns with zeros.
      In the example of issue 1, nuclear power could be filled with zeros.
    * Make nan rows in the new columns if any indicator used in the chart is nan.
      In the example of issue 2, Algeria's renewable electricity in 2022 would be nan.

    NOTES:
    * This function assumes a wide-format table.
    * In the grapher admin, ensure you select "Hide entities with missing data" in the corresponding chart.
    * This function creates new columns and does not affect already existing ones.

    Parameters
    ----------
    table : Table
        Original table.
    columns_in_chart : List[str]
        Column names in the relevant multi-indicator chart.
    chart_slug : str
        URL slug of the chart, which will be added to the name of the new columns.
        By convention, the suffix added to columns will be "_chart_" followed by the chart's slug in snake case format.
    suffix_for_titles: Optional[str]
        Suffix to be added to the new columns' titles, to avoid having multiple indicators with the same title.
    columns_to_fill_with_zeros : Optional[List[str]]
        Subset of columns_in_chart whose nans should be filled with zeros.
        Note: The original columns will not be affected, only the newly created ones.

    Returns
    -------
    table: Table
        Original table with new columns.

    """
    table = table.copy()

    def _rename_column(old_column_name: str, chart_slug: str) -> str:
        # Generate a name for a new column based on the old column name and the chart slug.
        return f"{old_column_name}_chart_{underscore(chart_slug)}"

    # Create new columns.
    new_columns = [_rename_column(old_column_name=column, chart_slug=chart_slug) for column in columns_in_chart]
    table[new_columns] = table[columns_in_chart].copy()

    # Optionally fill some of the new columns with zeros.
    if columns_to_fill_with_zeros is not None:
        # Sanity check.
        error = "columns_to_fill_with_zeros should be a subset of columns_in_chart."
        assert set(columns_to_fill_with_zeros) <= set(columns_in_chart), error
        # Fill nans with zeros (in new columns).
        new_columns_to_fill_with_zeros = [
            _rename_column(old_column_name=column, chart_slug=chart_slug) for column in columns_to_fill_with_zeros
        ]
        table[new_columns_to_fill_with_zeros] = table[new_columns_to_fill_with_zeros].fillna(0)

    # For each row, if any of the columns in the chart is nan, fill other columns in the same row with nan.
    table.loc[table[new_columns].isnull().any(axis=1), new_columns] = np.nan

    # Handle metadata.
    for column in new_columns:
        # If the indicator did not have any display name, use the original title.
        if ("name" not in table[column].display) or (table[column].metadata.display["name"] is None):
            table[column].metadata.display["name"] = table[column].metadata.title
        # To avoid having multiple indicators with the same title, add a suffix to the title.
        if suffix_for_titles is None:
            suffix_for_titles = f" (adapted for visualization of chart {chart_slug})"
        table[column].metadata.title += suffix_for_titles

    return table


def adapt_table_with_dates_to_grapher(
    tb: catalog.Table,
    columns: Optional[List[str]] = None,
    date_column: str = "date",
    country_column: str = "country",
    drop_date_column: bool = True,
) -> catalog.Table:
    """Adapt a table that has a date column to grapher requirements.

    This function adapts a table with, e.g. monthly data, so that it can be properly interpreted by grapher, and plotted
    with dates in the horizontal axis instead of years.

    Parameters
    ----------
    tb : Table
        Input table that has a date column.
    columns : Optional[List[str]], optional
        Columns that will be used in grapher.
    date_column : str, optional
        Name of column with dates, by default "date".
    country_column : str, optional
        Name of country column, by default "country".

    Returns
    -------
    Table
        Table adapted to grapher requirements.
    """
    tb = tb.copy()

    # Remove "year" column in the table, if any.
    if "year" in tb.columns:
        tb = tb.drop(columns=["year"], errors="raise")

    # Ensure date column is in datetime format.
    tb[date_column] = pd.to_datetime(tb[date_column].astype(object))

    # If no columns are specified, list all columns in the table (except the country and date columns).
    if columns is None:
        columns = [column for column in tb.columns if column not in [date_column, country_column]]

    for column in columns:
        # Find earliest date in the table.
        zero_day = tb[date_column].min()

        # Ensure display dictionary is present in the metadata of the relevant columns.
        if tb[column].metadata.display is None:
            tb[column].metadata.display = {}

        # Set the yearIsDay metadata field, so that grapher can read years as dates.
        tb[column].metadata.display["yearIsDay"] = True

        # Set zeroDay, which grapher will interpret as the earliest day from which to start counting dates.
        tb[column].metadata.display["zeroDay"] = zero_day.strftime("%Y-%m-%d")

        # Add a "year" column with number of days after zeroDay.
        tb["year"] = (tb["date"] - zero_day).dt.days

    if drop_date_column:
        # Drop the date column.
        tb = tb.drop(columns=[date_column])

    return tb


# TODO: Move to etl.grapher.helpers
def grapher_checks(ds: catalog.Dataset, warn_title_public: bool = True) -> None:
    """Check that the table is in the correct format for Grapher."""
    from etl.grapher import helpers as gh

    assert ds.metadata.title, "Dataset must have a title."

    for tab in ds:
        if {"year", "country"} <= set(tab.all_columns):
            if "year" in tab.columns:
                year = tab["year"]
            else:
                year = tab.index.get_level_values("year")
            assert year.dtype in gh.INT_TYPES, f"year must be of an integer type but was: {year.dtype}"
        elif {"date", "country"} <= set(tab.all_columns):
            pass
        else:
            raise AssertionError("Table must have columns country and year or date.")

        for col in tab:
            if col in ("year", "country"):
                continue
            catalog.utils.validate_underscore(col)
            assert tab[col].metadata.unit is not None, f"Column `{col}` must have a unit."
            assert tab[col].metadata.title is not None, f"Column `{col}` must have a title."
            assert tab[col].m.origins or tab[col].m.sources or ds.metadata.sources, (
                f"Column `{col}` must have either sources or origins"
            )

            _validate_description_key(tab[col].m.description_key, col)
            _validate_ordinal_variables(tab, col)
            _validate_grapher_config(tab, col)

            # Data Page title uses the following fallback
            # [title_public > grapher_config.title > display.name > title] - [attribution_short] - [title_variant]
            # the Table tab
            # [title_public > display.name > title] - [title_variant] - [attribution_short]
            # and chart heading
            # [grapher_config.title > title_public > display.name > title] - [grapher_config.subtitle > description_short]
            #
            # Warn if display.name (which is used for legend) exists and there's no title_public set. This
            # would override the indicator title in the Data Page.
            display_name = (tab[col].m.display or {}).get("name")
            title_public = getattr(tab[col].m.presentation, "title_public", None)
            if warn_title_public and display_name and not title_public:
                warnings.warn(
                    f"Column {col} uses display.name but no presentation.title_public. Ensure the latter is also defined, otherwise display.name will be used as the indicator's title.",
                    warnings.DisplayNameWarning,
                )


def _validate_grapher_config(tab: Table, col: str) -> None:
    """Validate grapher config against given schema or against the default schema."""
    grapher_config = getattr(tab[col].m.presentation, "grapher_config", None)
    if grapher_config:
        grapher_config.setdefault("$schema", DEFAULT_GRAPHER_SCHEMA)

        # Load schema and remove properties that are not relevant for the validation
        schema = get_schema_from_url(grapher_config["$schema"])
        # schema["required"] = [f for f in schema["required"] if f not in ("dimensions", "version", "title")]
        schema["required"] = []

        validate(grapher_config, schema)


def _validate_description_key(description_key: list[str], col: str) -> None:
    if description_key:
        assert not all(len(x) == 1 for x in description_key), (
            f"Column `{col}` uses string {description_key} as description_key, should be list of strings."
        )


def _validate_ordinal_variables(tab: Table, col: str) -> None:
    if tab[col].m.sort:
        # Exclude NaN values, these will be dropped before inserting to the database.
        vals = tab[col].dropna()

        extra_values = set(vals) - set(vals.m.sort)
        assert not extra_values, (
            f"Ordinal variable `{col}` has extra values that are not defined in field `sort`: {extra_values}"
        )
