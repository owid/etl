"""Load a meadow dataset and create a garden dataset."""

from itertools import product
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("guinea_worm.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency(short_name="guinea_worm", version="2023-06-29", channel="meadow"))
    ds_fasttrack = cast(
        Dataset, paths.load_dependency(short_name="guinea_worm", version="2023-06-28", channel="grapher")
    )
    # Read table from meadow dataset.
    tb = ds_meadow["guinea_worm"]
    tb_fasttrack = ds_fasttrack["guinea_worm"].reset_index().astype({"year": int})
    #
    # Process data.
    #
    log.info("guinea_worm.harmonize_countries")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = update_with_latest_status(tb)
    # Create time-series of certification
    tb_time_series = create_time_series(tb)
    tb_time_series = update_time_series_with_latest_information(tb_time_series)
    # Combine datasets
    tb = combine_datasets(tb, tb_time_series, tb_fasttrack)

    tb["year_certified"] = tb["year_certified"].astype("str")
    # tb = tb.dropna(axis=0, subset=["year_certified", "certification_status", "guinea_worm_reported_cases"], how="all")
    tb = add_missing_years(tb)
    # Fill na with 0
    tb["guinea_worm_reported_cases"] = tb["guinea_worm_reported_cases"].fillna(0)
    tb = tb.set_index(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("guinea_worm.end")


def add_missing_years(df: Table) -> Table:
    """
    Add full spectrum of year-country combinations to fast-track dataset so we have zeros where there is missing data
    """
    years = df["year"].drop_duplicates().to_list()
    countries = df["country"].drop_duplicates().to_list()
    comb_df = pd.DataFrame(list(product(countries, years)), columns=["country", "year"])

    df = Table(pd.merge(df, comb_df, on=["country", "year"], how="outer"), short_name=paths.short_name)

    return df


def update_with_latest_status(df: Table) -> Table:
    """
    Update with latest information as dataset only runs up to 2017
    Angola has had endemic status since 2020:  https://www.who.int/news/item/23-09-2020-eradicating-dracunculiasis-human-cases-and-animal-infections-decline-as-angola-becomes-endemic
    Kenya was certified guinea worm free in 2018: https://www.who.int/news/item/21-03-2018-dracunculiasis-eradication-south-sudan-claims-interruption-of-transmission-in-humans
    DRC was certified guinea worm free in 2022: https://www.who.int/news/item/15-12-2022-the-democratic-republic-of-the-congo-certified-free-of-dracunculiasis-transmission-by-who
    """
    df["year_certified"] = df["year_certified"].astype("object")
    df.loc[df["country"] == "Angola", "year_certified"] = "Endemic"
    df.loc[df["country"] == "Kenya", "year_certified"] = 2018
    df.loc[df["country"] == "Democratic Republic of Congo", "year_certified"] = 2022
    return df


def create_time_series(df: Table) -> Table:
    """
    Pivoting the table so that we can have a time-series of the guinea worm status and how it has changed over time
    """
    df_time = df.iloc[:, 0:24].drop(df.columns[[1]], axis=1)

    df_time.columns = df_time.columns.str.replace("_", "")
    years = df_time.drop("country", axis=1).columns.values
    df_piv = pd.melt(df_time, id_vars="country", value_vars=years)
    df_piv = df_piv.replace(
        {
            "value": {
                "Countries at precertification stage": "Pre-certification",
                "Previously endemic countries certified free of dracunculiasis": "Certified disease free",
                "Certified free of dracunculiasis": "Certified disease free",
                "Countries not known to have dracunculiasis but yet to be certified": "Pending surveillance",
                "Endemic for dracunculiasis": "Endemic",
            }
        }
    )
    df_piv = df_piv.rename(columns={"variable": "year", "value": "certification_status"})

    return df_piv


def update_time_series_with_latest_information(df: Table) -> Table:
    """
    For each country we replicate the status as it was in 2017 and then adjust the countries where this status has changed
    """
    df["year"] = df["year"].astype("int")
    years_to_add = [2018, 2019, 2020, 2021, 2022]

    year_to_copy = df[df["year"] == 2017].copy()

    for year in years_to_add:
        year_to_copy["year"] = year
        df = pd.concat([df, year_to_copy], ignore_index=True)

    assert any(df["year"].isin(years_to_add))
    df.loc[(df["country"] == "Angola") & (df["year"] >= 2020), "certification_status"] = "Endemic"
    df.loc[(df["country"] == "Kenya") & (df["year"] >= 2018), "certification_status"] = "Certified disease free"
    df.loc[
        (df["country"] == "Democratic Republic of Congo") & (df["year"] >= 2022), "certification_status"
    ] = "Certified disease free"

    return df


def combine_datasets(tb: Table, tb_time_series: Table, tb_fasttrack: Table) -> Table:
    tb["year"] = 2022
    tb = tb[["country", "year", "year_certified"]]

    tb_combined = pd.merge(tb, tb_time_series, on=["country", "year"], how="outer")
    tb_combined = pd.merge(tb_combined, tb_fasttrack, on=["country", "year"], how="outer")
    tb_combined = Table(tb_combined, short_name=paths.short_name)
    return tb_combined
