"""This script generates a dataset with the OMM on Life Expectancy.

To do this, we use different sources, depending on the year and metric:

> Life expectancy at birth:
    - UN WPP for data since 1950.
    - Zijdeman et al. (2015) for data prior to 1950.
    - Riley (2005) for data prior to 1950 for region aggregates.

> Life expectancy at age X:
    - UN WPP for data since 1950.
    - HMD for data prior to 1950.
"""
from typing import List

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)

# Dataset
GARDEN_WPP_DATASET = DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp"
GARDEN_HMD_DATASET = DATA_DIR / "garden" / "hmd" / "2022-11-04" / "life_tables"
GARDEN_ZIJDEMAN_DATASET = DATA_DIR / "garden" / "papers" / "2022-11-03" / "zijdeman_et_al_2015"
GARDEN_RILEY_DATASET = DATA_DIR / "garden" / "papers" / "2022-11-04" / "riley_2005"
# Other constants
COLUMNS_IDX = ["country", "year"]
AGES_EXTRA = ["15", "65", "80"]
YEAR_WPP_START = 1950
YEAR_WPP_END = 2022
# Versioning
VERSION = "2022-11-30"
# Cold start
# The first time this is executed, no metadata file is available. It is created on the fly, during execution time.
# Once this is done, we create the metadata YAML file using etl-metadata-export command.
# From then, we use the metadata in that YAML file, which might have some manual edits.
COLD_START = False


def run(dest_dir: str) -> None:
    log.info("life_expectancy.start")

    # read datasets from garden
    ds_wpp = Dataset(GARDEN_WPP_DATASET)
    ds_hmd = Dataset(GARDEN_HMD_DATASET)
    ds_zij = Dataset(GARDEN_ZIJDEMAN_DATASET)
    ds_ril = Dataset(GARDEN_RILEY_DATASET)
    all_ds = [ds_wpp, ds_hmd, ds_zij, ds_ril]

    # create table
    tb = make_table(ds_wpp, ds_hmd, ds_zij, ds_ril)
    # tb.update_metadata_from_yaml(N.metadata_path, "life_expectancy")

    # create dataset
    ds_garden = Dataset.create_empty(dest_dir)
    if COLD_START:
        ds_garden.metadata = make_metadata(all_ds)
    else:
        ds_garden.metadata.update_from_yaml(N.metadata_path)

    # add table to dataset
    ds_garden.add(tb)
    ds_garden.save()

    log.info("life_expectancy.end")


def make_table(ds_wpp: Dataset, ds_hmd: Dataset, ds_zij: Dataset, ds_ril: Dataset) -> Table:
    log.info("life_expectancy.make_table")
    # Build DataFrames
    df_wpp = load_wpp(ds_wpp)
    df_hmd = load_hmd(ds_hmd)
    df_zij = load_zijdeman(ds_zij)
    df_ril = load_riley(ds_ril)
    df = merge_dfs(df_wpp, df_hmd, df_zij, df_ril)

    # Build table
    log.info("life_expectancy.make_table.build_table")
    tb = Table(df)
    tb = underscore_table(tb)

    if COLD_START:
        tb.metadata = TableMeta(short_name="life_expectancy", title="Life Expectancy (various sources)")
    else:
        tb.update_metadata_from_yaml(N.metadata_path, "life_expectancy")
    return tb


def make_metadata(all_ds: List[Dataset]) -> DatasetMeta:
    """Create metadata for the dataset."""
    log.info("life_expectancy.make_metadata")
    # description
    description = "------\n\n"
    for ds in all_ds:
        description += f"{ds.metadata.title}:\n\n{ds.metadata.description}\n\n------\n\n"
    description = (
        "This dataset has been created using multiple sources. We use UN WPP for data since 1950. Prior to that, other"
        " sources are combined.\n\n" + description
    )

    # sources
    sources = [source for ds in all_ds for source in ds.sources]
    # licenses
    licenses_ = [license_ for ds in all_ds for license_ in ds.licenses]

    # metadata object
    metadata = DatasetMeta(
        namespace="demography",
        short_name="life_expectancy",
        title="Life Expectancy (various sources)",
        description=description,
        sources=sources,
        licenses=licenses_,
        version="2022-11-30",
    )
    return metadata


def load_wpp(ds: Dataset) -> pd.DataFrame:
    """Load data from WPP 2022 dataset.

    Output has the following columns: country, year, life_expectancy_0, life_expectancy_15, life_expectancy_65, life_expectancy_80.
    """
    log.info("life_expectancy.load_wpp")
    # Load table
    df = ds["un_wpp"]
    df = df.reset_index()
    # Filter relevant rows
    df = df.loc[
        (df["metric"] == "life_expectancy")
        & (df["age"].isin(["at birth"] + AGES_EXTRA))
        & (df["variant"].isin(["medium", "estimates"]))
        & (df["sex"] == "all")
        & (df["year"] <= YEAR_WPP_END)
    ]
    # Change age group from 'at birth' to '0'
    df = df.assign(age=df["age"].replace({"at birth": "0"}))
    # Pivot and set column names
    df = df.rename(columns={"location": "country"})
    df = df.pivot(index=COLUMNS_IDX, columns=["age"], values="value")
    df.columns = [f"life_expectancy_{col}" for col in df.columns]
    df = df.reset_index()
    return df


def load_hmd(ds: Dataset) -> pd.DataFrame:
    """Load data from HMD dataset.

    Output has the following columns: country, year, life_expectancy_15, life_expectancy_65, life_expectancy_80.
    """
    log.info("life_expectancy.load_hmd")
    df = ds["period_1x1"]
    df = df.reset_index()
    # Filter
    df = df.loc[
        (df["age"].isin(AGES_EXTRA)) & (df["year"].astype(int) < YEAR_WPP_START),
        COLUMNS_IDX + ["age", "life_expectancy"],
    ]
    # Pivot and set column names
    df = df.pivot(index=COLUMNS_IDX, columns=["age"], values="life_expectancy")
    df.columns = [f"life_expectancy_{col}" for col in df.columns]
    df = df.reset_index()
    # Correct values: expected years ahead => expected total years lived
    df = df.assign(
        life_expectancy_15=df["life_expectancy_15"] + 15,
        life_expectancy_65=df["life_expectancy_65"] + 65,
        life_expectancy_80=df["life_expectancy_80"] + 80,
    )
    return df


def load_zijdeman(ds: Dataset) -> pd.DataFrame:
    """Load data from Zijdeman et al. (2015) dataset.

    Output has the following columns: country, year, life_expectancy_0.
    """
    log.info("life_expectancy.load_zijdeman")
    df = ds["zijdeman_et_al_2015"].reset_index()
    # Filter
    df = df[df["year"] < YEAR_WPP_START]
    # Rename columns, drop columns
    columns_rename = {
        "country": "country",
        "year": "year",
        "life_expectancy": "life_expectancy_0",
    }
    df = df[columns_rename.keys()].rename(columns=columns_rename)
    return df


def load_riley(ds: Dataset) -> pd.DataFrame:
    """Load data from Zijdeman et al. (2015) dataset.

    Output has the following columns: country, year, life_expectancy_0.
    """
    log.info("life_expectancy.load_riley")
    df = ds["riley_2005"].reset_index()
    # Filter
    df = df[df["year"] < YEAR_WPP_START]
    # Rename columns, drop columns
    columns_rename = {
        "entity": "country",
        "year": "year",
        "life_expectancy": "life_expectancy_0",
    }
    df = df[columns_rename.keys()].rename(columns=columns_rename)
    return df


def merge_dfs(df_wpp: pd.DataFrame, df_hmd: pd.DataFrame, df_zij: pd.DataFrame, df_ril: pd.DataFrame) -> pd.DataFrame:
    """Merge all involved dataframes into a single one.

    - Life expectancy at birth is taken from UN WPP, Zijdeman et al. (2015) and Riley (2005) datasets.
    - Life expectancy at X is taken from UN WPP and HMD datasets.
    """
    log.info("life_expectancy.merge_dfs")
    # Merge with HMD
    df = pd.concat([df_wpp, df_hmd], ignore_index=True)
    # # Merge with Zijdeman et al. (2015)
    column_og = "life_expectancy_0"
    suffix = "_zij"
    column_extra = f"{column_og}{suffix}"
    df = df.merge(df_zij, how="outer", on=COLUMNS_IDX, suffixes=("", suffix))
    df = df.assign(life_expectancy_0=df[column_og].fillna(df[column_extra])).drop(columns=[column_extra])

    # # Merge with Riley (2005)
    assert not set(df.loc[df["year"] <= df_ril["year"].max(), "country"]).intersection(
        set(df_ril["country"])
    ), "There is some overlap between the dataset and Riley (2005) dataset"
    df = pd.concat([df, df_ril], ignore_index=True)

    # Dtypes, row sorting
    df = df.astype({"year": int})
    df = df.set_index(COLUMNS_IDX).sort_index()
    df = df.dropna(how="all", axis=0)

    # Rounding resolution
    rounding = 1e2
    df = ((df * rounding).round().fillna(-1).astype(int) / rounding).astype("float")
    df[df.life_expectancy_15 < 0] = pd.NA

    return df
