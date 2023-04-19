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
from etl.paths import DATA_DIR
from etl.snapshot import Snapshot

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("un_sdg.start")
    #
    # Load inputs.
    #
    # Load meadow dataset and relevant metadata conversions for units and dimensions
    ds_meadow = Dataset((DATA_DIR / f"meadow/{paths.namespace}/{paths.version}/{paths.short_name}").as_posix())

    # Read table from meadow dataset.
    tb_meadow = ds_meadow[paths.short_name]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    # Create long and short units columns
    df = create_units(df)

    df = manual_clean_data(df)
    #
    # Process data.
    #
    log.info("un_sdg.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Create a new table with the processed data.
    all_tables = create_tables(df)

    # Creating OMMs
    all_tables = create_omms(all_tables)
    # Combine Paris Principles variables
    all_tables = combine_paris_principles(all_tables)
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
    units: Snapshot = paths.load_dependency(short_name="un_sdg_unit.csv", namespace="un")
    df_units = pd.read_csv(units.path)
    dict_units = df_units.set_index("AttCode").to_dict()["AttValue"]
    return dict_units


def get_dimension_description() -> dict[str, str]:
    dimensions: Snapshot = paths.load_dependency(short_name="un_sdg_dimension.json", namespace="un")
    with open(dimensions.path) as json_file:
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


def combine_paris_principles(all_tabs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Combine the four variables regarding country's accreditation with the Paris Principles (having independent Human Rights Institutions)

    Currently there are four variables, one variable for each level of accreditation, shown by the value of 1. We should combine these and instead set the value as the series description.

    This is in a similar vain to the create_omms() function but in this case the variables each have their own variable code.


    """
    paris_principles_vars = ["SG_NHR_IMPLN", "SG_NHR_INTEXSTN", "SG_NHR_NOSTUSN", "SG_NHR_NOAPPLN"]
    paris_principles_tables = []
    for table in all_tabs:
        if table.index[0][5] in (paris_principles_vars):
            table = table.copy(deep=False)
            vc = table.groupby(["country", "year"]).value.sum().sort_values(ascending=False)
            regions = set(vc[vc > 1].index.get_level_values(0))
            table = table[~table.index.get_level_values("country").isin(regions)]
            table["value"] = table["seriesdescription"]
            table["long_unit"] = ""
            paris_principles_tables.append(table)
    paris_principles_table = pd.concat(paris_principles_tables)
    paris_principles_table = paris_principles_table.reset_index()
    # Adding new variable name
    paris_principles_table["seriescode"] = "SG_NHR_OWID"
    paris_principles_table["seriesdescription"] = "The level to which countries are compliant with the Paris Principles"
    paris_principles_table = paris_principles_table.set_index(
        ["country", "year", "goal", "target", "indicator", "seriescode"], verify_integrity=True
    )
    # Shortening the values to improve the source tab
    paris_principles_table.value = paris_principles_table.value.replace(
        {
            "Countries with National Human Rights Institutions in compliance with the Paris Principles, A status (1 = YES; 0 = NO)": "Compliant with Paris Principles",
            "Countries with National Human Rights Institutions not fully compliant with the Paris Principles, B status (1 = YES; 0 = NO)": "Observer Status",
            "Countries with no application for accreditation with the Paris Principles, D status  (1 = YES; 0 = NO)": "Not compliant with the Paris Principles",
            "Countries with National Human Rights Institutions and no status with the Paris Principles, C status (1 = YES; 0 = NO)": "No status with the Paris Principles",
        }
    )

    all_tabs.append(paris_principles_table)

    return all_tabs
