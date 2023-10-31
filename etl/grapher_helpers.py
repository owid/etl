import copy
import warnings
from copy import deepcopy
from dataclasses import dataclass, field, is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Set, cast

import jinja2
import numpy as np
import pandas as pd
import structlog
from jinja2 import Environment
from owid import catalog
from owid.catalog.utils import underscore

from etl.db import get_connection, get_engine
from etl.db_utils import DBUtils

log = structlog.get_logger()

jinja_env = Environment(
    block_start_string="<%",
    block_end_string="%>",
    variable_start_string="<<",
    variable_end_string=">>",
    comment_start_string="<#",
    comment_end_string="#>",
    trim_blocks=True,
    lstrip_blocks=True,
)

# this might work too pd.api.types.is_integer_dtype(col)
INT_TYPES = tuple({f"{n}{b}" for n in ("int", "Int", "uint", "UInt") for b in ("8", "16", "32", "64")})


def as_table(df: pd.DataFrame, table: catalog.Table) -> catalog.Table:
    """Convert dataframe into Table and add metadata from other table if available."""
    t = catalog.Table(df, metadata=table.metadata)
    for col in set(df.columns) & set(table.columns):
        t[col].metadata = table[col].metadata
    return t


def expand_dimensions(tb: catalog.Table) -> catalog.Table:
    """Expands dataframe with extra dimensions beyond country and year into multiple tables.
    For instance DataFrame with index names [country, year, sex, a] would expand into a table
    with columns [country, year, a__sex_male, a__sex_female].

    This function is not very memory efficient as it returns a table that will be very sparse.
    """
    # rename country to entity_id for the sake of `_yield_wide_table`
    tb = tb.reset_index("country").rename(columns={"country": "entity_id"}).set_index("entity_id", append=True)
    tables = list(_yield_wide_table(tb, na_action="drop", warn_null_variables=False))

    # join all tables
    # NOTE: we could also return individual tables to reduce memory usage
    expanded_table = catalog.tables.concat(tables, axis=1)

    # rename entity_id back to country
    expanded_table = (
        expanded_table.reset_index("entity_id")
        .rename(columns={"entity_id": "country"})
        .set_index("country", append=True)
    )

    return expanded_table


def _yield_wide_table(
    table: catalog.Table,
    na_action: Literal["drop", "raise"] = "raise",
    dim_titles: Optional[List[str]] = None,
    warn_null_variables: bool = True,
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
    cols_with_none_units = [col for col in table.columns if table[col].metadata.unit is None]
    if cols_with_none_units:
        raise Exception("Columns with missing units: " + ", ".join(cols_with_none_units))

    dim_names = [k for k in table.primary_key if k not in ("year", "entity_id")]
    if dim_titles:
        assert len(dim_names) == len(
            dim_titles
        ), "`dim_titles` must be the same length as your index without year and entity_id"
    else:
        dim_titles = dim_names

    if dim_names:
        grouped = table.groupby(dim_names if len(dim_names) > 1 else dim_names[0], as_index=False, observed=True)
    else:
        # a situation when there's only year and entity_id in index with no additional dimensions
        grouped = [([], table)]

    for dim_values, table_to_yield in grouped:
        dim_values = [dim_values] if isinstance(dim_values, str) else dim_values

        # Now iterate over every column in the original dataset and export the
        # subset of data that we prepared above
        for column in table_to_yield.columns:
            # If all values are null, skip variable
            if table_to_yield[column].isnull().all():
                if warn_null_variables:
                    log.warning("yield_wide_table.null_variable", column=column, dims=dim_values)
                continue

            # Safety check to see if the metadata is still intact
            assert (
                table_to_yield[column].metadata.unit is not None
            ), f"Unit for column {column} should not be None here!"

            # Select only one column and dimensions for performance
            tab = table_to_yield[[column]].copy()

            # Drop NA values
            tab = tab.dropna() if na_action == "drop" else tab

            # Create underscored name of a new column from the combination of column and dimensions
            short_name = _underscore_column_and_dimensions(column, dim_values, dim_names)

            # set new metadata with dimensions
            tab.metadata.short_name = short_name
            tab = tab.rename(columns={column: short_name})

            # add info about dimensions to metadata
            if dim_values:
                tab[short_name].metadata.additional_info = {
                    "dimensions": {
                        "originalShortName": column,
                        "originalName": tab[short_name].metadata.title,
                        "filters": [
                            {"name": dim_name, "value": sanitize_numpy(dim_value)}
                            for dim_name, dim_value in zip(dim_names, dim_values)
                        ],
                    }
                }

            dim_dict = dict(zip(dim_names, dim_values))

            # Add dimensions to title (which will be used as variable name in grapher)
            if tab[short_name].metadata.title:
                # We use template as a title
                if _uses_jinja(tab[short_name].metadata.title):
                    title_with_dims = _expand_jinja_text(tab[short_name].metadata.title, dim_dict)
                # Otherwise use default
                else:
                    title_with_dims = _title_column_and_dimensions(
                        tab[short_name].metadata.title, dim_values, dim_titles
                    )

                tab[short_name].metadata.title = title_with_dims

            # traverse metadata and expand Jinja
            tab[short_name].metadata = _expand_jinja(tab[short_name].metadata, dim_dict)

            # Keep only entity_id and year in index
            yield tab.reset_index().set_index(["entity_id", "year"])[[short_name]]


def _uses_jinja(text: Optional[str]):
    if not text:
        return False
    return "<%" in text or "<<" in text


def _expand_jinja_text(text: str, dim_dict: Dict[str, str]) -> str:
    if not _uses_jinja(text) or not dim_dict:
        return text

    try:
        return jinja_env.from_string(text).render(dim_dict)
    except jinja2.exceptions.TemplateSyntaxError as e:
        new_message = f"{e.message}\n\nDimensions:\n{dim_dict}\n\nTemplate:\n{text}\n"
        raise e.__class__(new_message, e.lineno, e.name, e.filename) from e


def _expand_jinja(obj: Any, dim_dict: Dict[str, str]) -> Any:
    """Expand Jinja in all metadata fields."""
    if obj is None:
        return None
    elif isinstance(obj, str):
        return _expand_jinja_text(obj, dim_dict)
    elif is_dataclass(obj):
        for k, v in obj.__dict__.items():
            setattr(obj, k, _expand_jinja(v, dim_dict))
        return obj
    elif isinstance(obj, list):
        return [_expand_jinja(v, dim_dict) for v in obj]
    elif isinstance(obj, dict):
        return {k: _expand_jinja(v, dim_dict) for k, v in obj.items()}
    else:
        return obj


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
    assert (
        table["meta"].dropna().map(lambda x: isinstance(x, catalog.VariableMeta)).all()
    ), "Values in column `meta` must be either instances of `catalog.VariableMeta` or null"


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
        assert set(meta.map(id)) == {
            id(meta.iloc[0])
        }, f"Variable `{var_name}` must have same metadata objects in column `meta` for all rows"
        t[var_name].metadata = meta.iloc[0]

        # name table as variable name
        t.metadata.short_name = var_name

        if metadata_path:
            t.update_metadata_from_yaml(metadata_path, var_name)

        yield cast(catalog.Table, t)


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
    create_entities: bool = False,
    errors: Literal["raise", "ignore", "warn"] = "raise",
    by: Literal["name", "code"] = "name",
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
    db_entities = _get_entities_from_db(set(country), by=by)
    entity_id = country.map(db_entities).astype(float)

    # create entities in DB
    if entity_id.isnull().any() and create_entities:
        assert by == "name", "create_entities works only with `by='name'`"
        ix = entity_id.isnull()
        # cast to float to fix issues with categories
        entity_id[ix] = country[ix].map(_get_and_create_entities_in_db(set(country[ix]))).astype(float)

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

    variable_titles = pd.Series([table[col].title for col in table.columns]).dropna()
    variable_titles_counts = variable_titles.value_counts()
    assert (
        variable_titles_counts.empty or variable_titles_counts.max() == 1
    ), f"Variable titles are not unique ({variable_titles_counts[variable_titles_counts > 1].index})."

    # Remember original dimensions
    dim_names = [n for n in table.index.names if n and n not in ("year", "entity_id", country_col)]

    # Reset index unless we have default index
    if table.index.names != [None]:
        table = table.reset_index()

    assert {"year", country_col} <= set(table.columns), f"Table must have columns {country_col} and year."
    assert "entity_id" not in table.columns, "Table must not have column entity_id."

    # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
    table["entity_id"] = country_to_entity_id(table[country_col], create_entities=True)
    table = table.drop(columns=[country_col]).rename(columns={year_col: "year"})

    table = table.set_index(["entity_id", "year"] + dim_names)

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
