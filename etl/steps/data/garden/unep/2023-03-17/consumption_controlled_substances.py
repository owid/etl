"""Load a meadow dataset and create a garden dataset."""

import json
from typing import List

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.geo import add_region_aggregates, list_countries_in_region
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("consumption_controlled_substances: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("consumption_controlled_substances")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["consumption_controlled_substances"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("consumption_controlled_substances: process data, creating table")
    tb_garden = df_to_table(df)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    # Update metadata
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("consumption_controlled_substances: end")


def df_to_table(df: pd.DataFrame) -> Table:
    # Dropna
    df = df.dropna(subset=["consumption"]).astype({"consumption": "float32"})
    # Check country mapping
    _check_country_mapping()
    # Harmonize countries
    log.info("consumption_controlled_substances: harmonizing countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Add EU28
    log.info("consumption_controlled_substances: add regions")
    df = add_regions(df)
    # Estimate total consumption of ozone-depleting substances (summation over all chemicals except HFCs)
    log.info("consumption_controlled_substances: estimating total")
    chemicals_ignore = [
        "Hydrofluorocarbons (HFCs)",
    ]
    df_depleting = df[~df["chemical"].isin(chemicals_ignore)]
    df_total = (
        df_depleting.groupby(["country", "year"], observed=True, as_index=False)[["consumption"]]
        .sum()
        .assign(chemical="All (Ozone-depleting)")
    )
    df = pd.concat([df, df_total], ignore_index=True).sort_values(["country", "year", "chemical"])
    # Add zero-filled column
    df = add_consumption_zerofilled(df)
    # Add consumption relative to 1986
    df = add_consumption_rel_1986(df)
    # Remove data for regions in last year
    df = remove_last_year_for_regions(df)
    # Set indices
    df = df.set_index(["country", "year", "chemical"])
    # Drop NaNs and set dtype
    df = df.astype({"consumption": "float32", "consumption_zf": "float32"})
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)
    return tb_garden


def add_regions(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["country", "year"]
    var_name = "chemical"
    value_name = "consumption"
    # Add data for the World
    df_world = df.groupby(["year", "chemical"], as_index=False)[[value_name]].sum().assign(country="World")
    df = pd.concat([df, df_world], ignore_index=True)
    # Pivot
    df_pivot = df.pivot(index=id_vars, columns=[var_name], values=value_name).reset_index()
    # Add continent data
    regions = ["Asia", "Africa", "North America", "South America", "Oceania"]
    for region in regions:
        df_pivot = add_region_aggregates(df_pivot, region=region)
    # Unpivot back
    df = df_pivot.melt(id_vars=id_vars, var_name=var_name, value_name=value_name).dropna(subset=[value_name])
    # Add EU28 data
    df = add_eu28(df)
    # Add Europe data
    df = add_europe(df)
    return df


def add_eu28(df: pd.DataFrame) -> pd.DataFrame:
    """Add EU28 data to the dataframe.

    This dataset provides data for European Union as a changing entity (i.e. member states vary over time). This
    function estimates EU 28, as a fixed entity, by summing up the data for all EU 28 members over time.

    EU 27 cannot be estimated because there is no UK data in the dataset prior to Brexit (2021).

    It removes the data for individual EU 28 member states.
    """
    # Get list of all EU28 members
    eu28_members = list_countries_in_region("European Union (27)") + ["United Kingdom", "European Union"]
    # Add EU28 data
    df = _add_region(df, eu28_members, "European Union (28)")
    return df


def add_europe(df: pd.DataFrame) -> pd.DataFrame:
    assert (
        "European Union (28)" in df.country.unique()
    ), "Check data! It looks like `European Union (28)` is not present."
    # EU states
    europe_members = list_countries_in_region("Europe") + ["European Union (28)"]
    assert (
        len(set(df.country).intersection(europe_members)) == 18
    ), "Check data! It might be that individual EU 28 member states are still present."
    # Add EU data
    df = _add_region(df, europe_members, "Europe", remove_members=False)
    return df


def _add_region(df: pd.DataFrame, members: List[str], region: str, remove_members: bool = True) -> pd.DataFrame:
    """Aggregate data for a region.

    This function is useful when adding regions that are not currently considered by etl.data_helpers.geo.add_region_aggregates.
    For instance "Europe Union (28)". Or when a region is built differently, e.g. Europe = EU 28 + ...
    """
    # Mask
    msk_region = df["country"].isin(members)
    df_region = df[msk_region].copy()
    df_region["country"] = region
    df_region = df_region.groupby(["country", "year", "chemical"], as_index=False)[["consumption"]].sum()
    if remove_members:
        df = pd.concat([df[~msk_region], df_region], ignore_index=True)
    else:
        df = pd.concat([df, df_region], ignore_index=True)
    return df


def _check_country_mapping():
    with open(paths.country_mapping_path, "r") as f:
        dix = json.load(f)
    assert len(dix.values()) == len(set(dix.values())), (
        "There are multiple countries with the same standardised name. Join step in Meadow might not be working"
        " properly."
    )


def add_consumption_zerofilled(df: pd.DataFrame) -> pd.DataFrame:
    id_vars = ["country", "year"]
    var_name = "chemical"
    value_name = "consumption"
    df = df.pivot(index=id_vars, columns=[var_name], values=value_name).reset_index()
    df = df.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)
    df["consumption_zf"] = df["consumption"].fillna(0)
    return df


def add_consumption_rel_1986(df: pd.DataFrame) -> pd.DataFrame:
    """Add column with ratio of consumption to 1986 consumption."""
    # Initial columns and new column names
    columns = list(df.columns)
    new_col = "consumption_rel_1986"
    # Get consumption in 1986, where it is not zero
    df_1986 = df[(df["year"] == 1986) & (df["consumption"] > 0)]
    # Merge and estimate ratio
    df = df.merge(df_1986, on=["country", "chemical"], suffixes=("", "_1986"), how="left")
    df[new_col] = (100 * df["consumption"] / df["consumption_1986"]).round(2)
    return df[columns + [new_col]]


def remove_last_year_for_regions(df: pd.DataFrame) -> pd.DataFrame:
    """Remove datapoint for latest available year in regions.

    Data for latest year for regions is usually an underestimate, because just a subset of countries have reported data."""
    REGIONS = [
        "Africa",
        "Asia",
        "Europe",
        "European Union (27)",
        "European Union (28)",
        "North America",
        "Oceania",
        "South America",
        "World",
    ]
    last_year = df["year"].max()
    df = df[~((df["year"] == last_year) & (df["country"].isin(REGIONS)))]
    return df
