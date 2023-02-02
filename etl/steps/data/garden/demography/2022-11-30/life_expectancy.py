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
import yaml
from owid.catalog import Dataset, DatasetMeta, Table, TableMeta
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)

# short name of new dataset
SHORT_NAME = N.short_name
# dataset paths
GARDEN_WPP_DATASET = DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp"
GARDEN_HMD_DATASET = DATA_DIR / "garden" / "hmd" / "2022-11-04" / "life_tables"
GARDEN_ZIJDEMAN_DATASET = DATA_DIR / "garden" / "papers" / "2022-11-03" / "zijdeman_et_al_2015"
GARDEN_RILEY_DATASET = DATA_DIR / "garden" / "papers" / "2022-11-04" / "riley_2005"
# auxiliary datasets
GARDEN_POPULATION_WPP = DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp"
# index column names: this is used when setting indices in dataframes
COLUMNS_IDX = ["country", "year"]
# age groups considered besides at 0 (at birth)
AGES_EXTRA = ["15", "65", "80"]
# year when UN WPP data starts
YEAR_WPP_START = 1950
# year separating historical and projection data in UN WPP dataset.
YEAR_HIST_PROJ = 2021
# Versioning
VERSION = "2022-11-30"
# Cold start
# The first time this is executed, no metadata file is available. It is created on the fly, during execution time.
# Once this is done, we create the metadata YAML file using etl-metadata-export command.
# From then, we use the metadata in that YAML file, which might have some manual edits.
COLD_START = False

# Region mapping
# We will be using continent names without (Entity) suffix. This way charts show continuity between lines from different datasets (e.g. riley and UN)
REGION_MAPPING = {
    "Africa (Riley 2005)": "Africa",
    "Americas (Riley 2005)": "Americas",
    "Asia (Riley 2005)": "Asia",
    "Europe (Riley 2005)": "Europe",
    "Oceania (Riley 2005)": "Oceania",
    "Africa (UN)": "Africa",
    "Northern America (UN)": "Northern America",
    "Latin America and the Caribbean (UN)": "Latin America and the Caribbean",
    "Asia (UN)": "Asia",
    "Europe (UN)": "Europe",
    "Oceania (UN)": "Oceania",
}
# Path to historical events file
# this file contains a list of historical events that likely caused data anomalies in the dataset.
# note that proving that these anomalies are caused by those events would require some complicated causal inference.
PATH_HIST_EVENTS = N.directory / "life_expectancy.historical_events.yml"


def run(dest_dir: str) -> None:
    log.info("life_expectancy.start")

    # read datasets from garden
    ds_wpp = Dataset(GARDEN_WPP_DATASET)
    ds_hmd = Dataset(GARDEN_HMD_DATASET)
    ds_zij = Dataset(GARDEN_ZIJDEMAN_DATASET)
    ds_ril = Dataset(GARDEN_RILEY_DATASET)
    # group datasets into single list
    all_ds = [ds_wpp, ds_hmd, ds_zij, ds_ril]
    # load dataframes
    df_wpp = load_wpp(ds_wpp)
    df_hmd = load_hmd(ds_hmd)
    df_zij = load_zijdeman(ds_zij)
    df_ril = load_riley(ds_ril)

    # create tables (all-years, historical and projections)
    log.info("life_expectancy: create table with all-years data")
    tb = make_table(df_wpp, df_hmd, df_zij, df_ril)
    log.info("life_expectancy: create table with historical data")
    tb_historical = make_table(df_wpp, df_hmd, df_zij, df_ril, only_historical=True)
    log.info("life_expectancy: create table with projection data")
    tb_projection = make_table(df_wpp, df_hmd, df_zij, df_ril, only_projections=True)

    # create dataset
    ds_garden = Dataset.create_empty(dest_dir)
    if COLD_START:
        ds_garden.metadata = make_metadata(all_ds)
    else:
        ds_garden.metadata.update_from_yaml(N.metadata_path)

    # add tables to dataset
    log.info("life_expectancy: add tables to dataset")
    ds_garden.add(tb)
    ds_garden.save()
    ds_garden.add(tb_historical)
    ds_garden.save()
    ds_garden.add(tb_projection)
    ds_garden.save()

    # add historical events table
    ds_garden.add(make_hist_events_table())
    ds_garden.save()

    log.info("life_expectancy.end")


def make_table(
    df_wpp: pd.DataFrame,
    df_hmd: pd.DataFrame,
    df_zij: pd.DataFrame,
    df_ril: pd.DataFrame,
    only_historical: bool = False,
    only_projections: bool = False,
) -> Table:
    """Create table.

    Joins all different sources into a single dataframe.

    By default, it creates a table with all years. Use `only_historical` and `only_projections` to create tables with
    only historical data or only projections, respectively.
    """
    log.info("life_expectancy.make_table")
    df = merge_dfs(df_wpp, df_hmd, df_zij, df_ril)

    # Filter
    assert not (only_historical and only_projections), "Both only_historical and only_projections can't be True!"
    if only_historical:
        df = df[df.index.get_level_values("year") <= YEAR_HIST_PROJ]
    if only_projections:
        df = df[df.index.get_level_values("year") > YEAR_HIST_PROJ]

    # Build table
    log.info("life_expectancy.make_table.build_table")
    tb = Table(df)
    tb = underscore_table(tb)

    # metadata
    tb = add_metadata_to_table(tb, only_historical, only_projections)
    return tb


def add_metadata_to_table(tb: Table, only_historical: bool, only_projections: bool) -> Table:
    """Add metadata to table.

    This is done from scratch or by reading the YAML file. Note that only one table is actually defined.
    The other two (historical and projections) are equivalent with minor changes in title and variable titles/names.
    """

    def _get_metadata_cold_start(short_name):
        return TableMeta(short_name=short_name, title=f"Life Expectancy (various sources) - {short_name.capitalize()}")

    def _get_metadata(tb, short_name):
        tb.columns = [f"{col}_{short_name[:4]}" for col in tb.columns]
        tb.update_metadata_from_yaml(N.metadata_path, short_name)
        return tb

    if COLD_START:
        if only_projections:
            tb.metadata = _get_metadata_cold_start("projection")
        elif only_historical:
            tb.metadata = _get_metadata_cold_start("historical")
        else:
            tb.metadata = TableMeta(short_name=SHORT_NAME, title="Life Expectancy (various sources)")
    else:
        if only_projections:
            tb = _get_metadata(tb, "projection")
        elif only_historical:
            tb = _get_metadata(tb, "historical")
        else:
            tb.update_metadata_from_yaml(N.metadata_path, SHORT_NAME)
    return tb


def make_metadata(all_ds: List[Dataset]) -> DatasetMeta:
    """Create metadata for the dataset."""
    log.info("life_expectancy: creating metadata")
    # description
    description = "------\n\n"
    for ds in all_ds:
        description += f"{ds.metadata.title}:\n\n{ds.metadata.description}\n\n------\n\n"
    description = (
        "This dataset has been created using multiple sources. We use UN WPP for data since 1950 (estimates and medium"
        " variant). Prior to that, other sources are combined.\n\n" + description
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

    It loads medium variant for future projections.

    Output has the following columns: country, year, life_expectancy_0, life_expectancy_15, life_expectancy_65, life_expectancy_80.
    """
    log.info("life_expectancy: loading wpp data")
    # Load table
    df = ds["un_wpp"]
    df = df.reset_index()
    # Filter relevant rows
    df = df.loc[
        (df["metric"] == "life_expectancy")
        & (df["age"].isin(["at birth"] + AGES_EXTRA))
        & (df["variant"].isin(["medium", "estimates"]))
        & (df["sex"] == "all")
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
    log.info("life_expectancy: loading hmd data")
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
    log.info("life_expectancy: loading zijdeman 2015 data")
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
    log.info("life_expectancy: loading riley 2005 data")
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
    log.info("life_expectancy: merging dataframes")
    # Merge with HMD
    df = pd.concat([df_wpp, df_hmd], ignore_index=True)
    # # Merge with Zijdeman et al. (2015)
    column_og = "life_expectancy_0"
    suffix = "_zij"
    column_extra = f"{column_og}{suffix}"
    df = df.merge(df_zij, how="outer", on=COLUMNS_IDX, suffixes=("", suffix))
    df = df.assign(life_expectancy_0=df[column_og].fillna(df[column_extra])).drop(columns=[column_extra])

    # Merge with Riley (2005)
    assert not set(df.loc[df["year"] <= df_ril["year"].max(), "country"]).intersection(
        set(df_ril["country"])
    ), "There is some overlap between the dataset and Riley (2005) dataset"
    df = pd.concat([df, df_ril], ignore_index=True)

    # add region aggregates
    # df = add_region_aggregates(df)

    # Rename regions
    df["country"] = df["country"].replace(REGION_MAPPING)

    # add americas for >1950 using UN WPP data
    df = add_americas(df)

    # Dtypes, row sorting
    df = df.astype({"year": int})
    df = df.set_index(COLUMNS_IDX, verify_integrity=True).sort_index()
    df = df.dropna(how="all", axis=0)

    # Rounding resolution
    # We round to 2 decimals
    rounding = 1e2
    df = ((df * rounding).round().fillna(-1).astype(int) / rounding).astype("float")
    for col in df.columns:
        if col not in COLUMNS_IDX:
            df.loc[df[col] < 0, col] = pd.NA
    return df


def add_americas(frame: pd.DataFrame) -> pd.DataFrame:
    """Estimate value for the Americas using North America and LATAM/Caribbean."""
    # filter only member countries of the region
    region_members = ["Northern America", "Latin America and the Caribbean"]
    df = frame.loc[frame["country"].isin(region_members)].copy()
    # add population for LATAM and Northern America (from WPP, hence since 1950)
    assert df["year"].min() == YEAR_WPP_START
    df = add_population_americas_from_wpp(df)
    # sanity check: ensure there are NO missing values. This way, we can safely do the groupby
    assert (df.isna().sum() == 0).all()
    # estimate values for regions
    # y(country) = weight(country) * metric(country)
    df["life_expectancy_0"] *= df["population"]
    df["life_expectancy_15"] *= df["population"]
    df["life_expectancy_65"] *= df["population"]
    df["life_expectancy_80"] *= df["population"]
    # z(region) = sum{ y(country) } for country in region
    df = df.groupby("year", as_index=False).sum(numeric_only=True)
    # z(region) /  sum{ population(country) } for country in region
    df["life_expectancy_0"] /= df["population"]
    df["life_expectancy_15"] /= df["population"]
    df["life_expectancy_65"] /= df["population"]
    df["life_expectancy_80"] /= df["population"]

    # assign region name
    df = df.assign(country="Americas")
    # concatenate
    df = pd.concat([frame, df]).sort_values(["country", "year"], ignore_index=True).drop(columns="population")
    return df


def add_region_aggregates(frame: pd.DataFrame) -> pd.DataFrame:
    """Add life expectancy for continents.

    This function is currently not in use, but might be useful in the future.
    """
    log.info("life_expectancy: adding region aggregates")
    # new regions
    regions_new = [
        "Europe",
        "Oceania",
        "Asia",
        "Africa",
        "North America",
        "South America",
    ]
    # remove regions
    regions_ignore = [
        "Africa",
        # "Africa (UN)",
        "Asia",
        # "Asia (UN)",
        "Europe",
        # "Europe (UN)",
        # "Latin America and the Caribbean (UN)",
        "North America",
        # "Northern America (UN)",
        "Oceania",
        # "Oceania (UN)",
        "South America",
    ]
    frame = frame.loc[-frame["country"].isin(regions_ignore)]
    # add population
    df = geo.add_population_to_dataframe(frame.copy())

    # estimate values for regions
    # y(country) = weight(country) * metric(country)
    df["life_expectancy_0"] *= df["population"]
    df["life_expectancy_15"] *= df["population"]
    df["life_expectancy_65"] *= df["population"]
    df["life_expectancy_80"] *= df["population"]
    # z(region) = sum{ y(country) } for country in region
    for region in regions_new:
        df = geo.add_region_aggregates(df, region=region)
    df = df[df["country"].isin(regions_new)]
    # z(region) /  sum{ population(country) } for country in region
    df["life_expectancy_0"] /= df["population"]
    df["life_expectancy_15"] /= df["population"]
    df["life_expectancy_65"] /= df["population"]
    df["life_expectancy_80"] /= df["population"]

    # concatenate
    df = pd.concat([frame, df]).sort_values(["country", "year"], ignore_index=True).drop(columns="population")
    return df


def add_population_americas_from_wpp(df: pd.DataFrame):
    """Add population values for LATAM and Northern America.

    Data is sourced from UN WPP, hence only available since 1950.
    """
    pop = load_america_population_from_unwpp()
    df = df.merge(pop, on=["country", "year"])
    return df


def load_america_population_from_unwpp():
    """Load population data from UN WPP for Northern America and Latin America and the Caribbean.

    We use this dataset instead of the long-run because we want the entities as defined by the UN.
    """
    # load population from WPP
    locations = ["Latin America and the Caribbean (UN)", "Northern America (UN)"]
    ds = Dataset(GARDEN_POPULATION_WPP)
    df = ds["population"].reset_index()
    df = df.loc[
        (df["location"].isin(locations))
        & (df["metric"] == "population")
        & (df["sex"] == "all")
        & (df["age"] == "all")
        & (df["variant"].isin(["estimates", "medium"])),
        ["location", "year", "value"],
    ]
    assert len(set(df["location"])) == 2, f"Check that all of {locations} are in df"
    df["location"] = df["location"].replace(REGION_MAPPING)

    # rename columns
    df = df.rename(columns={"location": "country", "value": "population"})
    return df


def make_hist_events_table() -> Table:
    log.info("life_expectancy: making 'historical events' table")
    # Load historical events yaml file
    with open(PATH_HIST_EVENTS) as f:
        hist_events = yaml.safe_load(f)
    # store all yaml's content as a string in a cell in the table
    df = pd.DataFrame({"hist_events": [str(hist_events)]})
    tb = Table(df)
    # add metadata
    tb.metadata = TableMeta(
        short_name="_hist_events",
        description=(
            "this table contains a list of historical events that likely caused data anomalies for the life expectancy"
            " data in YAML format."
        ),
    )
    return tb
