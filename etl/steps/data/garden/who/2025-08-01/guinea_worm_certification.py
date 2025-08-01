"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

LATEST_YEAR = 2022


def run(dest_dir: str) -> None:
    log.info("guinea_worm.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(
        Dataset, paths.load_dependency(short_name="guinea_worm_certification", version="2023-06-29", channel="meadow")
    )

    # Read table from meadow dataset.
    tb = ds_meadow["guinea_worm_certification"]

    #
    # Process data.
    #
    log.info("guinea_worm_certification.harmonize_countries")
    tb: Table = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = update_with_latest_status(tb)
    # Create time-series of certification
    tb_time_series = create_time_series(tb)
    tb_time_series = update_time_series_with_latest_information(tb_time_series)
    # Combine datasets
    tb = tb.drop(columns=[col for col in tb.columns if col not in ["country", "year", "year_certified"]])
    tb = add_year_certified(tb, tb_time_series)

    tb["year_certified"] = tb["year_certified"].astype("str")
    tb = tb.set_index(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("guinea_worm.end")


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


def create_time_series(df: Table):
    """
    Pivoting the table so that we can have a time-series of the guinea worm status and how it has changed over time
    """
    df_time = df.iloc[:, 0:24].drop(df.columns[[1]], axis=1)

    df_time.columns = df_time.columns.str.replace("_", "")
    years = df_time.drop("country", axis=1).columns.values
    df_piv = pr.melt(df_time, id_vars="country", value_vars=years)
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


def update_time_series_with_latest_information(tb: Table):
    """
    For each country we replicate the status as it was in 2017 and then adjust the countries where this status has changed
    """
    tb["year"] = tb["year"].astype("int")
    years_to_add = list(range(2018, LATEST_YEAR + 1))

    year_to_copy = tb[tb["year"] == 2017].copy()

    for year in years_to_add:
        year_to_copy["year"] = year
        tb = pr.concat([tb, year_to_copy], ignore_index=True)

    assert any(tb["year"].isin(years_to_add))
    tb.loc[(tb["country"] == "Angola") & (tb["year"] >= 2020), "certification_status"] = "Endemic"
    tb.loc[(tb["country"] == "Kenya") & (tb["year"] >= 2018), "certification_status"] = "Certified disease free"
    tb.loc[(tb["country"] == "Democratic Republic of Congo") & (tb["year"] >= 2022), "certification_status"] = (
        "Certified disease free"
    )

    return tb


def add_year_certified(tb: Table, tb_time_series: Table) -> Table:
    tb_time_series["year_certified"] = pd.NA
    for cntry in tb_time_series["country"].unique():
        year_certified = tb[tb["country"] == cntry]["year_certified"].max()
        if year_certified in ["Endemic", "Pre-certification", "Pending surveillance"]:
            # set all years to the certification status of that year
            tb_time_series.loc[tb_time_series["country"] == cntry, "year_certified"] = tb_time_series.loc[
                tb_time_series["country"] == cntry, "certification_status"
            ]
        else:
            year_certified = int(year_certified)
            # years after certification should have the year of certification
            tb_time_series.loc[
                (tb_time_series["country"] == cntry) & (tb_time_series["year"] >= year_certified),
                "year_certified",
            ] = year_certified
            # years before certification should have respective status of that year
            tb_time_series.loc[
                (tb_time_series["country"] == cntry) & (tb_time_series["year"] < year_certified),
                "year_certified",
            ] = tb_time_series.loc[
                (tb_time_series["country"] == cntry) & (tb_time_series["year"] < year_certified),
                "certification_status",
            ]

    return tb_time_series
