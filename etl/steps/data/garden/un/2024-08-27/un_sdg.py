"""Load a meadow dataset and create a garden dataset."""

import json
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("un_sdg.start")
    #
    # Load inputs.
    #
    # Load meadow dataset and relevant metadata conversions for units and dimensions
    ds_meadow = paths.load_dataset("un_sdg")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow.read("un_sdg", safe_types=False)

    # Create long and short units columns
    tb = create_units(tb_meadow)

    tb = manual_clean_data(tb)
    tb = remove_cities(tb)
    #
    # Process data.
    #
    log.info("un_sdg.harmonize_countries")
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Create a new table with the processed data.
    all_tables = create_tables(tb)

    # Creating OMMs
    all_tables = create_omms(all_tables)

    log.info("Saving data tables...")

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    for table in all_tables:
        log.info(
            "un_sdg.create_garden_table",
            indicator=table.index[0][4],
            series_code=table.index[0][5],
        )

        tb_garden = Table(table, short_name=paths.short_name)
        tb_garden.metadata = tb_meadow.metadata
        short_name = tb_garden.index[0][4] + "_" + tb_garden.index[0][5]
        tb_garden.metadata.short_name = underscore(short_name)
        ds_garden.add(tb_garden)

    ds_garden.save()

    log.info("un_sdg.end")


def create_units(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=False)
    unit_description = get_attributes_description()
    df["long_unit"] = df["units"].map(unit_description)
    assert df["long_unit"].isna().sum() == 0
    df["short_unit"] = create_short_unit(df["long_unit"])
    return df


def get_attributes_description() -> Dict:
    units_snapshot = paths.load_snapshot(short_name="un_sdg_unit.csv", namespace="un")
    df_units = pd.read_csv(units_snapshot.path)
    dict_units = df_units.set_index("AttCode").to_dict()["AttValue"]
    return dict_units


def get_dimension_description() -> dict[str, str]:
    dimensions_snapshot = paths.load_snapshot(short_name="un_sdg_dimension.json", namespace="un")
    with open(dimensions_snapshot.path) as json_file:
        dims: dict[str, str] = json.load(json_file)
    # underscore to match the df column names
    for key in dims.copy():
        dims[underscore(key)] = dims.pop(key)
    return dims


def create_short_unit(long_unit: pd.Series) -> np.ndarray[Any, np.dtype[Any]]:
    conditions = [
        (long_unit.str.contains("PERCENT")) | (long_unit.str.contains("Percentage") | (long_unit.str.contains("%"))),
        (long_unit.str.contains("KG")) | (long_unit.str.contains("Kilograms")),
        (long_unit.str.contains("USD")) | (long_unit.str.contains("usd")),
    ]

    choices = ["%", "kg", "$"]

    short_unit = np.select(conditions, choices, default="")
    return short_unit


def manual_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some values for 15.2.1 is above 100% when this shouldn't be possible. This sets the max value to 100.
    Returns:
        pd.DataFrame with cleaned values for 15.2.1
    """
    df = df.copy(deep=False)

    df["value"] = df["value"].astype(float)
    df.loc[
        (df["long_unit"] == "Percentage") & (df["value"] > 100) & (df["indicator"] == "15.2.1"),
        "value",
    ] = 100

    # Clean the IHR Capacity column, duplicate labelling of some attributes which doesn't work well with the grapher
    df["ihr_capacity"] = df["ihr_capacity"].replace(
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
    # Dropping average marine acidity as we don't have a way to visualise it
    df = df[~df["seriescode"].isin(["ER_OAW_MNACD"])]
    df = df.drop(["level_0", "index"], axis=1, errors="ignore")

    return df


def remove_cities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop out all city level estimates from the data as we can't visualise them
    """
    msk = df["seriescode"].isin(["ER_OAW_MNACD", "EN_REF_WASCOL", "SP_TRN_PUBL", "EN_URB_OPENSP", "EN_LND_CNSPOP"])
    city_df = df[msk]
    original_df = df[~msk]

    city_df = city_df[city_df["cities"].isin(["NOCITI", "_T"])]

    original_df = pd.concat([original_df, city_df])

    return original_df


def create_tables(original_df: pd.DataFrame) -> List[pd.DataFrame]:
    original_df = original_df.copy(deep=False)

    dim_description = get_dimension_description()
    init_dimensions = list(dim_description.keys())
    init_dimensions = list(set(init_dimensions).intersection(list(original_df.columns)))
    init_dimensions = sorted(init_dimensions)
    init_non_dimensions = list([c for c in original_df.columns if c not in set(init_dimensions)])
    init_non_dimensions = sorted(init_non_dimensions)
    all_series = original_df.groupby(["indicator", "seriescode"])

    output_tables = []
    len_dimensions = []
    for group_name, df_group in all_series:
        log.info(
            "un_sdg.create_dataframe.group",
            indicator=group_name[0],
            series=group_name[1],
        )
        df_dim, dimensions = get_series_with_relevant_dimensions(df_group, init_dimensions, init_non_dimensions)
        len_dimensions.append(len(dimensions))
        if len(dimensions) == 0:
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(
                dim_dict=dim_description, data_dimensions=df_dim, dimensions=dimensions
            )
            table_fil = table[
                [
                    "country",
                    "year",
                    "goal",
                    "target",
                    "indicator",
                    "seriescode",
                    "seriesdescription",
                    "value",
                    "long_unit",
                    "short_unit",
                    "source",
                ]
            ]
            table_fil = table_fil.dropna()
            table_fil.set_index(
                [
                    "country",
                    "year",
                    "goal",
                    "target",
                    "indicator",
                    "seriescode",
                ],
                inplace=True,
                verify_integrity=True,
            )

            output_tables.append(table_fil)

        else:
            # has additional dimensions
            tables = generate_tables_for_indicator_and_series(
                dim_dict=dim_description, data_dimensions=df_dim, dimensions=dimensions
            )
            tables_fil = tables[
                [
                    "country",
                    "year",
                    "goal",
                    "target",
                    "indicator",
                    "seriescode",
                    "seriesdescription",
                    "value",
                    "long_unit",
                    "short_unit",
                    "source",
                ]
                + dimensions
            ]
            tables_fil = tables_fil.dropna()
            tables_fil.set_index(
                [
                    "country",
                    "year",
                    "goal",
                    "target",
                    "indicator",
                    "seriescode",
                ]
                + dimensions,
                inplace=True,
                verify_integrity=True,
            )

            output_tables.append(tables_fil)
    return output_tables


def generate_tables_for_indicator_and_series(
    dim_dict: dict[Any, Any],
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

    non_null_dimensions_columns = [col for col in init_dimensions if data_series.loc[:, col].notna().any()]
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
            data_series.columns.intersection(init_non_dimensions + list(dimension_names)),
        ],
        dimension_names,
    )


def create_omms(all_tabs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    new_tabs = []
    for table in all_tabs:
        if table.index[0][5] in ("ER_BDY_ABT2NP", "SG_SCP_PROCN"):
            table = table.copy(deep=False)
            table = table.query('level_status != "No breakdown"')

            # exclude regions which contain more than one country and cannot be
            # converted to a level_status for a single country
            vc = table.groupby(["country", "year"]).value.sum().sort_values(ascending=False)
            regions = set(vc[vc > 1].index.get_level_values(0))
            table = table[~table.index.get_level_values("country").isin(regions)]

            table.reset_index(level=["level_status"], inplace=True)  # type: ignore
            table["value"] = table["level_status"]
            table.drop(columns=["level_status"], inplace=True)
        new_tabs.append(table)

    return new_tabs
