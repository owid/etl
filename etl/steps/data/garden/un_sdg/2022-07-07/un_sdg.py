import json
import numpy as np
import pandas as pd

from structlog import get_logger
from pathlib import Path
from typing import Tuple, List, Any, Dict
from etl.steps.data.converters import convert_walden_metadata
from owid.walden import Catalog
from owid.catalog import Dataset, Table, DatasetMeta, TableMeta
from owid.catalog.utils import underscore

from owid.catalog import Dataset, Table
from etl.paths import DATA_DIR

COUNTRY_MAPPING_PATH = (
    Path(__file__).parent / "un_sdg.country_mapping.json"
).as_posix()

BASE_URL = "https://unstats.un.org/sdgapi"
VERSION = Path(__file__).parent.stem
FNAME = Path(__file__).stem
NAMESPACE = Path(__file__).parent.parent.stem

COUNTRY_MAPPING_PATH = (
    "etl/steps/data/garden/un_sdg/2022-07-07/un_sdg.country_mapping.json"
)

VERSION = "2022-07-07"
FNAME = "un_sdg"
NAMESPACE = "un_sdg"
log = get_logger()


def run(dest_dir: str, query: str = "") -> None:
    log.info("Reading in dataset from Meadow...")
    ds_meadow = Dataset((DATA_DIR / f"meadow/{NAMESPACE}/{VERSION}/{FNAME}").as_posix())

    assert (
        len(ds_meadow.table_names) == 1
    ), "Expected meadow dataset to have only one table, but found > 1 table names."
    tb_meadow = ds_meadow[FNAME]
    df = pd.DataFrame(tb_meadow)
    df = create_units(df)
    df = manual_clean_data(df)

    log.info("Harmonizing entity names...")
    country_mapping = load_country_mapping()
    excluded_countries = load_excluded_countries()
    df = df[~df.country.isin(excluded_countries)]
    assert df["country"].notnull().all()
    countries = df["country"].apply(lambda x: country_mapping.get(x, None))
    if countries.isnull().any():
        missing_countries = [
            x for x in df["country"].drop_duplicates() if x not in country_mapping
        ]
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {COUNTRY_MAPPING_PATH} to include these country "
            "names; or (b) remove these country names from the raw table."
            f"Raw country names: {missing_countries}"
        )

    assert df["country"].notnull().all()
    assert df["value"].notnull().all()
    assert (
        not df.isnull().all(axis=1).any()
    ), "Unexpected state: One or more rows contains only NaN values."

    log.info("Creating data tables...")
    all_tables = create_tables(df)

    log.info("Saving data tables...")
    for table in all_tables:
        log.info("un_sdg.create_garden_table", series_code=table.index[0][5])
        ds_garden = Dataset.create_empty(dest_dir)
        ds_garden.metadata = ds_meadow.metadata

        tb_garden = Table(table)
        tb_garden.metadata = tb_meadow.metadata
        ds_garden.add(tb_garden)

        ds_garden.save()


def create_tables(original_df: pd.DataFrame) -> List[pd.DataFrame]:
    # Removing the square brackets from the indicator column
    original_df = original_df.copy(deep=False)

    dim_description = get_dimension_description()

    init_dimensions = list(dim_description.keys())
    init_dimensions = list(set(init_dimensions).intersection(list(original_df.columns)))
    # init_dimensions.extend(["Country", "Year"])
    init_non_dimensions = list(
        [c for c in original_df.columns if c not in set(init_dimensions)]
    )

    all_series = original_df.groupby(["indicator", "seriescode"])

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
                    "units_long",
                    "short_unit",
                ]
            ]
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
                    "units_long",
                    "short_unit",
                ]
                + dimensions
            ]

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

        # output_table = pd.concat(output_tables)
    return output_tables


def create_units(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=False)
    unit_description = get_attributes_description()
    df["units_long"] = df["units"].map(unit_description)
    df["short_unit"] = create_short_unit(df["units_long"])
    return df


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


def create_short_unit(long_unit: pd.Series) -> np.ndarray[Any, np.dtype[Any]]:

    conditions = [
        (long_unit.str.contains("PERCENT"))
        | (long_unit.str.contains("Percentage") | (long_unit.str.contains("%"))),
        (long_unit.str.contains("KG")) | (long_unit.str.contains("Kilograms")),
        (long_unit.str.contains("USD")) | (long_unit.str.contains("usd")),
    ]

    choices = ["%", "kg", "$"]

    short_unit = np.select(conditions, choices, default="str")
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
        (df["units_long"] == "Percentage")
        & (df["value"] > 100)
        & (df["indicator"] == "15.2.1"),
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

    df = df.drop(["level_0", "index"], axis=1, errors="ignore")

    return df


def get_attributes_description() -> Any:
    walden_ds = Catalog().find_one(
        namespace=NAMESPACE, short_name="unit", version=VERSION
    )
    local_file = walden_ds.ensure_downloaded()
    with open(local_file) as json_file:
        units = json.load(json_file)
    return units


# not sure about this type return but it's what make check-typing demanded!
def get_dimension_description() -> Any:
    walden_ds = Catalog().find_one(
        namespace=NAMESPACE, short_name="dimension", version=VERSION
    )
    local_file = walden_ds.ensure_downloaded()
    with open(local_file) as json_file:
        dims = json.load(json_file)
    # underscore to match the df column names
    for key in dims.copy():
        dims[underscore(key)] = dims.pop(key)
    return dims


def load_country_mapping() -> Dict[str, str]:
    with open(COUNTRY_MAPPING_PATH, "r") as f:
        mapping = json.load(f)
        assert isinstance(mapping, dict)
    return mapping


def load_excluded_countries() -> List[str]:
    # with open(Path(__file__).parent / f"{FNAME}.country_exclude.json", "r") as f:
    with open(
        "etl/steps/data/garden/un_sdg/2022-07-07/un_sdg.country_exclude.json"
    ) as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data
