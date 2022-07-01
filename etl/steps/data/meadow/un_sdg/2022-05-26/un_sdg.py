import requests
import numpy as np
import re
import itertools
import math
import pandas as pd


from typing import Tuple

from owid.walden import Catalog
from owid.catalog import Dataset, Table, DatasetMeta, TableMeta
from owid.catalog.utils import underscore

BASE_URL = "https://unstats.un.org/sdgapi"


def run(dest_dir: str) -> None:
    # retrieves raw data from walden
    # version = Path(__file__).parent.stem
    # fname = Path(__file__).stem
    # namespace = Path(__file__).parent.parent.stem

    version = "2022-05-26"
    fname = "un_sdg"
    namespace = "un_sdg"
    walden_ds = Catalog().find_one(
        namespace=namespace, short_name=fname, version=version
    )
    local_file = walden_ds.ensure_downloaded()
    df = pd.read_csv(local_file, low_memory=False)
    df = load_and_clean(df)

    # drops rows with only NaN in the year column
    years = "Time_Detail"
    df.dropna(subset=years, how="all", inplace=True)
    df.shape
    full_df = create_dataframe(df)
    full_df.shape

    assert full_df["Country"].notnull().all()
    assert full_df["variable_name"].notnull().all()

    full_df[
        (full_df[["Country", "Time_Detail", "variable_name", "Value"]].duplicated())
        & (full_df["SeriesCode"] == "EN_ATM_CO2")
        & (full_df["Country"] == "Other Africa (IEA)")
    ]

    return full_df


def create_dataframe(original_df: pd.DataFrame) -> None:
    # Removing the square brackets from the indicator column
    new_columns = []
    for k in original_df.columns:
        new_columns.append(re.sub(r"[\[\]]", "", k))

    original_df.columns = new_columns

    unit_description = attributes_description()

    dim_description = dimensions_description()

    original_df["Units_long"] = original_df["Units"].apply(
        lambda x: unit_description[x]
    )
    original_df["short_unit"] = create_short_unit(original_df["Units_long"])

    original_df = manual_clean_data(original_df)

    init_dimensions = tuple(dim_description["id"].unique())
    init_non_dimensions = tuple(
        [c for c in original_df.columns if c not in set(init_dimensions)]
    )
    all_series = (
        original_df[["Indicator", "SeriesCode"]].drop_duplicates().reset_index()
    )

    all_series = original_df.groupby(["Indicator", "SeriesCode"])

    output_tables = []

    for group_name, df_group in all_series:
        _, dimensions, dimension_members = get_series_with_relevant_dimensions(
            df_group, init_dimensions, init_non_dimensions
        )

        if len(dimensions) == 0:
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(
                df_group, init_dimensions, init_non_dimensions, dim_description
            )
            table["variable_name"] = "%s - %s - %s" % (
                table["Indicator"].iloc[0],
                table["SeriesDescription"].iloc[0],
                table["SeriesCode"].iloc[0],
            )
            output_tables.append(table)
        else:
            # has additional dimensions
            table = generate_tables_for_indicator_and_series(
                df_group, init_dimensions, init_non_dimensions, dim_description
            )

            tables = []
            for tab, key in zip(table.values(), table.keys()):
                tab["variable_name"] = "%s - %s - %s - %s" % (
                    tab["Indicator"].iloc[0],
                    tab["SeriesDescription"].iloc[0],
                    tab["SeriesCode"].iloc[0],
                    " - ".join(map(str, key)),
                )
                tables.append(tab)

            tables = pd.concat(tables)

            output_tables.append(tables)

        output_table = pd.concat(output_tables)

    return output_table


def manual_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Some values for 15.2.1 is above 100% when this shouldn't be possible. This sets the max value to 100.
    Returns:
        pd.DataFrame with cleaned values for 15.2.1
    """
    df["Value"] = df["Value"].astype(float)
    df["Value"][
        (df["Units_long"] == "Percentage")
        & (df["Value"] > 100)
        & (df["Indicator"] == "15.2.1")
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


def get_goal_codes() -> list:
    # retrieves all goal codes
    url = f"{BASE_URL}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    return goal_codes


def attributes_description() -> dict:
    goal_codes = get_goal_codes()
    a = []
    for goal in goal_codes:
        url = f"{BASE_URL}/v1/sdg/Goal/{goal}/Attributes"
        res = requests.get(url)
        assert res.ok
        attr = res.json()
        for att in attr:
            for code in att["codes"]:
                a.append(
                    {
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    att_dict = pd.DataFrame(a).drop_duplicates().set_index("code").squeeze().to_dict()
    att_dict["PERCENT"] = "%"
    return att_dict


def dimensions_description() -> pd.DataFrame:
    goal_codes = get_goal_codes()
    d = []
    for goal in goal_codes:
        url = f"{BASE_URL}/v1/sdg/Goal/{goal}/Dimensions"
        res = requests.get(url)
        assert res.ok
        dims = res.json()
        for dim in dims:
            for code in dim["codes"]:
                d.append(
                    {
                        "id": dim["id"],
                        "code": code["code"],
                        "description": code["description"],
                    }
                )
    dim_dict = pd.DataFrame(d).drop_duplicates()
    # adding an nan code for each id - a problem for the Coverage dimension
    nan_data = {
        "id": dim_dict.id.unique(),
        "code": np.repeat(np.nan, len(dim_dict.id.unique()), axis=0),
        "description": np.repeat("", len(dim_dict.id.unique()), axis=0),
    }
    nan_df = pd.DataFrame(nan_data)
    dim_dict = pd.concat([dim_dict, nan_df])
    return dim_dict


def load_and_clean(original_df: pd.DataFrame) -> pd.DataFrame:
    # Load and clean the data
    print("Reading in original data...")
    # removing values that aren't numeric e.g. Null and N values
    original_df.dropna(subset=["Value"], inplace=True)
    original_df = original_df[
        pd.to_numeric(original_df["Value"], errors="coerce").notnull()
    ]
    original_df.rename(columns={"GeoAreaName": "Country"}, inplace=True)
    return original_df


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


def generate_tables_for_indicator_and_series(
    data_series: pd.DataFrame,
    init_dimensions: tuple,
    init_non_dimensions: tuple,
    dim_dict: dict,
) -> pd.DataFrame:
    tables_by_combination = {}
    data_dimensions, dimensions, dimension_values = get_series_with_relevant_dimensions(
        data_series, init_dimensions, init_non_dimensions
    )
    if len(dimensions) == 0:  # not the best solution.
        # no additional dimensions
        export = data_dimensions
        return export
    else:
        dim_desc = (
            dim_dict.set_index("id")
            .loc[dimensions]
            .set_index("code")
            .squeeze()
            .to_dict()
        )
        dim_desc["nan"] = ""
        i = 0
        # Mapping the dimension value codes to more meaningful descriptions
        for i in range(len(dimension_values)):
            df = pd.DataFrame({"value": dimension_values[i]})
            df["value"] = df["value"].astype(str)
            dimension_values[i] = [dim_desc[k] for k in df["value"].to_list()]
        # Mapping the descriptions into the dataframe
        for dim in dimensions:
            data_dimensions[dim] = data_dimensions[dim].astype(str)
            data_dimensions[dim] = [dim_desc[k] for k in data_dimensions[dim]]
        # Create each combination of dimension values, e.g. each age group & sex combination. Not all combinations will have associated data.
        for dimension_value_combination in itertools.product(*dimension_values):
            # build filter by reducing, start with a constant True boolean array
            filt = [True] * len(data_dimensions)
            for dim_idx, dim_value in enumerate(dimension_value_combination):
                dimension_name = dimensions[dim_idx]
                value_is_nan = type(dim_value) == float and math.isnan(dim_value)
                # Boolean identifying which rows contain the dimension combination
                filt = filt & (
                    data_dimensions[dimension_name].isnull()
                    if value_is_nan
                    else data_dimensions[dimension_name] == dim_value
                )
                # Pulling out the data for a given combination
                tables_by_combination[dimension_value_combination] = data_dimensions[
                    filt
                ].drop(dimensions, axis=1)
                # Removing tables for the combinations that don't exist
                tables_by_combination = {
                    k: v for (k, v) in tables_by_combination.items() if not v.empty
                }  # removing empty combinations
    return tables_by_combination


def get_series_with_relevant_dimensions(
    data_series: pd.DataFrame, init_dimensions: tuple, init_non_dimensions: tuple
) -> Tuple[pd.DataFrame, list, list]:
    """For a given indicator and series, return a tuple:
    - data filtered to that indicator and series
    - names of relevant dimensions
    - unique values for each relevant dimension
    """
    non_null_dimensions_columns = [
        col for col in init_dimensions if data_series.loc[:, col].notna().any()
    ]
    dimension_names = []
    dimension_unique_values = []

    for c in non_null_dimensions_columns:
        uniques = data_series[c].unique()
        if (
            len(uniques) > 1
        ):  # Means that columns where the value doesn't change aren't included e.g. Nature is typically consistent across a dimension whereas Age and Sex are less likely to be.
            dimension_names.append(c)
            dimension_unique_values.append(list(uniques))
    return (
        data_series[
            data_series.columns.intersection(
                list(init_non_dimensions) + list(dimension_names)
            )
        ],
        dimension_names,
        dimension_unique_values,
    )
