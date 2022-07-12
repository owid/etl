import requests
import numpy as np
import re
import itertools
import math
import pandas as pd
from collections import defaultdict

from structlog import get_logger
from pathlib import Path
from typing import Tuple, List, Any, Dict
from etl.steps.data.converters import convert_walden_metadata
from owid.walden import Catalog
from owid.catalog import Dataset, Table, DatasetMeta, TableMeta
from owid.catalog.utils import underscore

BASE_URL = "https://unstats.un.org/sdgapi"
log = get_logger()


def run(dest_dir: str, query: str = "") -> None:
    version = Path(__file__).parent.stem
    fname = Path(__file__).stem
    namespace = Path(__file__).parent.parent.stem
    ds_meadow = Dataset((DATA_DIR / f"meadow/{namespace}/{version}/{fname}").as_posix())

    assert (
        len(ds_meadow.table_names) == 1
    ), "Expected meadow dataset to have only one table, but found > 1 table names."
    tb_meadow = ds_meadow[fname]
    df = pd.DataFrame(tb_meadow).reset_index()

    full_df = create_dataframe(df)
    full_df = full_df[
        [
            "country",
            "year",
            "source",
            "value",
            "units_long",
            "short_unit",
        ]
    ]
    # verify_integrity checks for duplicates
    log.info("Size of dataframe", rows=full_df.shape[0], colums=full_df.shape[1])

    assert full_df["country"].notnull().all()
    assert full_df["value"].notnull().all()
    assert (
        not full_df.isnull().all(axis=1).any()
    ), "Unexpected state: One or more rows contains only NaN values."


def create_dataframe(original_df: pd.DataFrame) -> pd.DataFrame:
    # Removing the square brackets from the indicator column
    original_df = original_df.copy(deep=False)

    unit_description = attributes_description()

    dim_description = dimensions_description()

    original_df["Units_long"] = original_df["Units"].map(unit_description)
    original_df["short_unit"] = create_short_unit(original_df["Units_long"])

    init_dimensions = list(dim_description.keys())
    init_dimensions = list(set(init_dimensions).intersection(list(original_df.columns)))
    # init_dimensions.extend(["Country", "Year"])
    init_non_dimensions = list(
        [c for c in original_df.columns if c not in set(init_dimensions)]
    )

    all_series = original_df.groupby(["Indicator", "SeriesCode"])

    output_tables = []
    len_dimensions = []
    for group_name, df_group in all_series:
        log.info(
            "un_sdg.create_dataframe.group",
            indicator=group_name[0],
            series=group_name[1],
        )
        df_dim, dimensions = get_series_with_relevant_dimensions(
            df_group, init_dimensions, init_non_dimensions
        )
        len_dimensions.append(len(dimensions))
        if len(dimensions) == 0:
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(
                dim_dict=dim_description, data_dimensions=df_dim, dimensions=dimensions
            )
            table.set_index(["Country", "Year"])
            output_tables.append(table)

        else:
            # has additional dimensions
            tables = generate_tables_for_indicator_and_series(
                dim_dict=dim_description, data_dimensions=df_dim, dimensions=dimensions
            )

            tables.set_index(["Country", "Year"] + dimensions, verify_integrity=True)

            output_tables.append(tables)

        output_table = pd.concat(output_tables)
    return output_table


def generate_tables_for_indicator_and_series(
    dim_dict: dict,
    data_dimensions: pd.DataFrame,
    dimensions: List[str],
) -> pd.DataFrame:

    if len(dimensions) == 0:
        return data_dimensions
    else:
        for dim in dimensions:
            data_dimensions[dim] = data_dimensions[dim].map(dim_dict[dim])

    return data_dimensions


def get_series_with_relevant_dimensions(
    data_series: pd.DataFrame,
    init_dimensions: List[str],
    init_non_dimensions: List[str],
) -> Tuple[pd.DataFrame, List[str]]:
    """For a given indicator and series, return a tuple:
    - data filtered to that indicator and series
    - names of relevant dimensions
    - unique values for each relevant dimension
    """

    non_null_dimensions_columns = [
        col for col in init_dimensions if data_series.loc[:, col].notna().any()
    ]
    dimension_names = []

    for c in non_null_dimensions_columns:
        uniques = data_series[c].unique()
        if (
            len(uniques) > 1
        ):  # Means that columns where the value doesn't change aren't included e.g. Nature is typically consistent across a dimension whereas Age and Sex are less likely to be.
            dimension_names.append(c)
    return (
        data_series.loc[
            :,
            data_series.columns.intersection(
                init_non_dimensions + list(dimension_names)
            ),
        ],
        dimension_names,
    )





def create_short_unit(long_unit: pd.Series) -> np.ndarray:

    conditions = [
        (long_unit.str.contains("PERCENT"))
        | (long_unit.str.contains("Percentage") | (long_unit.str.contains("%"))),
        (long_unit.str.contains("KG")) | (long_unit.str.contains("Kilograms")),
        (long_unit.str.contains("USD")) | (long_unit.str.contains("usd")),
    ]

    choices = ["%", "kg", "$"]

    short_unit = np.select(conditions, choices, default=None)
    return short_unit


def manual_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some values for 15.2.1 is above 100% when this shouldn't be possible. This sets the max value to 100.
    Returns:
        pd.DataFrame with cleaned values for 15.2.1
    """
    df = df.copy(deep=False)

    df["Value"] = df["Value"].astype(float)
    df.loc[
        (df["Units_long"] == "Percentage")
        & (df["Value"] > 100)
        & (df["Indicator"] == "15.2.1"),
        "Value",
    ] = 100

    # Clean the IHR Capacity column, duplicate labelling of some attributes which doesn't work well with the grapher
    df["IHR Capacity"] = df["IHR Capacity"].replace(
        [
            "IHR02",
            "IHR03",
            "IHR06",
            "IHR07",
            "IHR08",
            "IHR09",
            "IHR10",
            "IHR11",
            "IHR12",
        ],
        [
            "SPAR02",
            "SPAR06",
            "SPAR10",
            "SPAR07",
            "SPAR05",
            "SPAR11",
            "SPAR03",
            "SPAR04",
            "SPAR12",
        ],
    )
    return df
