import zipfile
import requests
import numpy as np
import re

from pathlib import Path

import pandas as pd
from pandas.api.types import is_numeric_dtype  # type: ignore

from owid.walden import Catalog
from owid.catalog import Dataset, Table, DatasetMeta, TableMeta
from owid.catalog.utils import underscore


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

    assert df["geoareaname"].notnull().all()
    assert df["indicator"].notnull().all()

    return df


att_desc = attributes_description()

dim_desc = dimensions_description()


def attributes_description() -> dict:
    base_url = "https://unstats.un.org/sdgapi"
    # retrieves all goal codes
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    # retrieves all area codes
    a = []
    for goal in goal_codes:
        url = f"{base_url}/v1/sdg/Goal/{goal}/Attributes"
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
    return att_dict


def dimensions_description() -> pd.DataFrame:
    base_url = "https://unstats.un.org/sdgapi"
    # retrieves all goal codes
    url = f"{base_url}/v1/sdg/Goal/List"
    res = requests.get(url)
    assert res.ok
    goals = res.json()
    goal_codes = [int(goal["code"]) for goal in goals]
    # retrieves all area codes
    d = []
    for goal in goal_codes:
        url = f"{base_url}/v1/sdg/Goal/{goal}/Dimensions"
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

    original_df = clean_data(original_df)
    init_dimensions = tuple(dim_description.id.unique())
    init_non_dimensions = tuple(
        [c for c in original_df.columns if c not in set(init_dimensions)]
    )
    all_series = (
        original_df[["Indicator", "SeriesCode", "SeriesDescription", "Units_long"]]
        .drop_duplicates()
        .reset_index()
    )
    all_series["short_unit"] = create_short_unit(all_series.Units_long)
    print("Extracting variables from original data...")

    for i, row in tqdm(all_series.iterrows(), total=len(all_series)):
        data_filtered = pd.DataFrame(
            original_df[
                (original_df.Indicator == row["Indicator"])
                & (original_df.SeriesCode == row["SeriesCode"])
            ]
        )

        _, dimensions, dimension_members = get_series_with_relevant_dimensions(
            data_filtered, init_dimensions, init_non_dimensions
        )
        if len(dimensions) == 0:
            # no additional dimensions
            table = generate_tables_for_indicator_and_series(
                data_filtered, init_dimensions, init_non_dimensions, dim_description
            )
            variable = pd.DataFrame(
                data={
                    "dataset_id": [0],
                    "source_id": series2source_id[row["SeriesCode"]],
                    "id": variable_idx,
                    "name": "%s - %s - %s"
                    % (row["Indicator"], row["SeriesDescription"], row["SeriesCode"]),
                    "description": None,
                    "code": None,
                    "unit": row["Units_long"],
                    "short_unit": row["short_unit"],
                    "timespan": "%s - %s"
                    % (
                        int(np.min(data_filtered["TimePeriod"])),
                        int(np.max(data_filtered["TimePeriod"])),
                    ),
                    "coverage": None,
                    "display": None,
                    "original_metadata": None,
                }
            )
            variables = pd.concat([variables, variable], ignore_index=True)
            extract_datapoints(table).to_csv(
                os.path.join(OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx),
                index=False,
            )
            variable_idx += 1
        else:
            # has additional dimensions
            for member_combination, table in generate_tables_for_indicator_and_series(
                data_series=data_filtered,
                init_dimensions=init_dimensions,
                init_non_dimensions=init_non_dimensions,
                dim_dict=dim_description,
            ).items():
                variable = pd.DataFrame(
                    data={
                        "dataset_id": [0],
                        "source_id": series2source_id[row["SeriesCode"]],
                        "id": variable_idx,
                        "name": "%s - %s - %s - %s"
                        % (
                            row["Indicator"],
                            row["SeriesDescription"],
                            row["SeriesCode"],
                            " - ".join(map(str, member_combination)),
                        ),
                        "description": None,
                        "code": None,
                        "unit": row["Units_long"],
                        "short_unit": row["short_unit"],
                        "timespan": "%s - %s"
                        % (
                            int(np.min(data_filtered["TimePeriod"])),
                            int(np.max(data_filtered["TimePeriod"])),
                        ),
                        "coverage": None,
                        # "display": None,
                        "original_metadata": None,
                    }
                )
                variables = pd.concat([variables, variable], ignore_index=True)

                extract_datapoints(table).to_csv(
                    os.path.join(
                        OUTPATH, "datapoints", "datapoints_%d.csv" % variable_idx
                    ),
                    index=False,
                )
                variable_idx += 1
    variables = create_omms(variables)
    print("Saving variables csv...")
    variables.to_csv(os.path.join(OUTPATH, "variables.csv"), index=False)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
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
